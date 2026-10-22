/*
 * test_transfer.c - unit tests for transfer.c over real loopback TCP
 * connections: one server thread runs serve_topic()/send_file_range() on a
 * hand-built conn_t, the main thread is the client (recv_topic()/recv_file()).
 *
 * Covers: complete transfers in RX_DIRECT and RX_PART mode (byte-identical
 * files, no .part left, exact data and wire byte counts on both sides), the
 * progress hook sequence (n = 0 right after every FILE_HDR), injected faults
 * (exact .part size, the FAULT row in the FDS_EVENTS CSV, prefix clamping and
 * an empty prefix), byte-offset resume including start_offset == file size,
 * every client-side error code, the server's ERROR 500 for an unreadable
 * file, a vanished peer, and the empty topic.
 *
 * Expected warnings on stderr: "cannot open .../b.bin" (unreadable file),
 * "invalid range"/"invalid resume point" (caller errors), and the client's
 * ".part has N bytes", "cannot resume ..." and "output path" lines.
 */
#define _GNU_SOURCE                      /* nftw() flags */
#include "unit.h"
#include "fds.h"
#include "log.h"
#include "net.h"
#include "protocol.h"
#include "server_core.h"
#include "topic_map.h"
#include "transfer.h"
#include "util.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

static char g_base[PATH_MAX];            /* mkdtemp() directory */
static char g_root[PATH_MAX];            /* topics root inside it */
static char g_events[PATH_MAX];          /* FDS_EVENTS CSV */
static topic_map_t g_map;
static const topic_t *g_t, *g_empty, *g_locked;
static int g_lfd = -1;
static uint16_t g_port;
static uint64_t g_conn_id;

/* ---- Setup helpers ------------------------------------------------------------ */

static void die(const char *what, const char *arg)
{
    fprintf(stderr, "test setup failed: %s %s: %s\n", what, arg, strerror(errno));
    exit(1);
}

static void write_file(const char *dir, const char *name, size_t size, int random)
{
    char path[PATH_MAX];
    uint8_t *buf = malloc(size > 0 ? size : 1);
    int fd;

    if (buf == NULL || path_join(path, sizeof(path), dir, name) != 0)
        die("alloc/path", name);
    if (random)
        random_bytes(buf, size);
    else
        for (size_t i = 0; i < size; i++)
            buf[i] = (uint8_t)(i * 7 + 3);
    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0 || (size > 0 && write(fd, buf, size) != (ssize_t)size) || close(fd) != 0)
        die("write", path);
    free(buf);
}

/* Read a whole file (malloc'd); *len = its size. NULL if it cannot be read. */
static uint8_t *read_file(const char *path, size_t *len)
{
    struct stat st;
    uint8_t *buf;
    int fd = open(path, O_RDONLY);

    if (fd < 0)
        return NULL;
    if (fstat(fd, &st) != 0 || (buf = malloc((size_t)st.st_size + 1)) == NULL) {
        close(fd);
        return NULL;
    }
    *len = (size_t)st.st_size;
    if (st.st_size > 0 && read(fd, buf, (size_t)st.st_size) != st.st_size) {
        free(buf);
        close(fd);
        return NULL;
    }
    close(fd);
    return buf;
}

static int64_t file_size_at(const char *dir, const char *name, const char *suffix)
{
    char path[PATH_MAX];
    struct stat st;

    if (path_join3(path, sizeof(path), dir, name, suffix) != 0 || stat(path, &st) != 0)
        return -1;
    return (int64_t)st.st_size;
}

/* 1 if dir/<name> is byte-identical to file i of topic t. */
static int same_as_source(const topic_t *t, uint32_t i, const char *dir)
{
    char a[PATH_MAX], b[PATH_MAX];
    size_t la = 0, lb = 0;
    uint8_t *pa, *pb;
    int same;

    if (path_join(a, sizeof(a), t->abs_path, t->files[i].name) != 0 ||
        path_join(b, sizeof(b), dir, t->files[i].name) != 0)
        return 0;
    pa = read_file(a, &la);
    pb = read_file(b, &lb);
    same = pa != NULL && pb != NULL && la == lb && la == t->files[i].size &&
           (la == 0 || memcmp(pa, pb, la) == 0);
    free(pa);
    free(pb);
    return same;
}

/* 1 if dir holds exactly the files of t, all identical, and nothing else. */
static int dir_is_complete_copy(const topic_t *t, const char *dir)
{
    DIR *d = opendir(dir);
    struct dirent *de;
    uint32_t entries = 0;
    int ok = d != NULL;

    while (ok && (de = readdir(d)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;
        entries++;
        if (topic_file_index(t, de->d_name) < 0) {
            fprintf(stderr, "unexpected entry in %s: %s\n", dir, de->d_name);
            ok = 0;
        }
    }
    if (d != NULL)
        closedir(d);
    for (uint32_t i = 0; ok && i < t->n_files; i++)
        if (!same_as_source(t, i, dir)) {
            fprintf(stderr, "%s/%s differs from the source\n", dir, t->files[i].name);
            ok = 0;
        }
    return ok && entries == t->n_files;
}

static void make_out_dir(char *out, size_t cap, const char *name)
{
    if (path_join(out, cap, g_base, name) != 0 || mkdir_p(out, 0755) != 0)
        die("mkdir", name);
}

static uint32_t index_of(const topic_t *t, const char *name)
{
    int64_t i = topic_file_index(t, name);

    if (i < 0) {
        fprintf(stderr, "file %s missing from topic %s\n", name, t->key);
        exit(1);
    }
    return (uint32_t)i;
}

static int rm_entry(const char *path, const struct stat *st, int flag, struct FTW *ftw)
{
    (void)st; (void)flag; (void)ftw;
    return remove(path);
}

/* ---- Wire byte arithmetic (design 3.3), computed independently of protocol.c ---- */

#define W_TOPIC_BEGIN (16u + 16u + 4u + 8u)
#define W_TOPIC_END   (16u + 4u + 8u)
#define W_FILE_END    (16u + 4u)
static uint64_t w_file_hdr(const char *name) { return 16u + 4u + 8u + 8u + 2u + strlen(name); }

/* Every byte the server sends for serve_topic(from, off, begin) without faults. */
static uint64_t expected_wire(const topic_t *t, uint32_t from, uint64_t off, int begin)
{
    uint64_t w = begin ? W_TOPIC_BEGIN : 0;

    for (uint32_t i = from; i < t->n_files; i++)
        w += w_file_hdr(t->files[i].name) + (t->files[i].size - (i == from ? off : 0)) + W_FILE_END;
    return w + W_TOPIC_END;
}

/* ---- Hooks ------------------------------------------------------------------------ */

#define MAX_REC 4096
typedef struct { uint32_t idx; uint64_t off, n; } prog_rec_t;
typedef struct { prog_rec_t r[MAX_REC]; size_t count; } recorder_t;

static void rec_hook(conn_t *c, uint32_t idx, uint64_t off, uint64_t n)
{
    recorder_t *r = c->user;

    if (r->count < MAX_REC)
        r->r[r->count] = (prog_rec_t){ idx, off, n };
    r->count++;
}

/* The hook sequence of an unfaulted serve from (from, off) must be exactly:
 * per file (i, start, 0), then one (i, new_off, chunk_len) per chunk. */
static int progress_ok(const recorder_t *r, const topic_t *t, uint32_t from, uint64_t off)
{
    size_t k = 0;

    for (uint32_t i = from; i < t->n_files; i++) {
        uint64_t pos = i == from ? off : 0, size = t->files[i].size;

        if (k >= r->count || r->r[k].idx != i || r->r[k].off != pos || r->r[k].n != 0)
            return 0;
        k++;
        while (pos < size) {
            uint64_t n = FDS_MIN((uint64_t)FDS_CHUNK_SIZE, size - pos);

            pos += n;
            if (k >= r->count || r->r[k].idx != i || r->r[k].off != pos || r->r[k].n != n)
                return 0;
            k++;
        }
    }
    return k == r->count;
}

/* Fault stub: faults at the k-th chunk drawn (1-based) with the given prefix. */
typedef struct { int k, draws; size_t prefix; } stub_fault_t;

static int stub_fault(void *st, size_t chunk_len, size_t *prefix)
{
    stub_fault_t *f = st;

    (void)chunk_len;
    if (++f->draws == f->k) {
        *prefix = f->prefix;             /* may break the contract on purpose: clamped */
        return 1;
    }
    return 0;
}

/* on_file_hdr probe: counts calls, checks nothing exists yet for a fresh
 * file, and can fail on the n-th call. */
typedef struct { int calls, early_files, fail_at; } hdr_probe_t;

static int probe_hdr(rx_ctx_t *rx, const m_file_hdr_t *h)
{
    hdr_probe_t *p = rx->user;

    p->calls++;
    if (h->start_offset == 0 && (file_size_at(rx->dir, h->name, ".part") >= 0 ||
                                 file_size_at(rx->dir, h->name, "") >= 0))
        p->early_files++;
    return p->calls == p->fail_at ? -1 : 0;
}

/* ---- Connection plumbing ------------------------------------------------------------ */

static int listen_random(uint16_t *port)
{
    for (int i = 0; i < 100; i++) {
        uint16_t r;
        int fd;

        random_bytes((uint8_t *)&r, sizeof(r));
        *port = (uint16_t)(20000 + r % 40001);
        fd = tcp_listen(*port);
        if (fd >= 0)
            return fd;
    }
    return -1;
}

/* A loopback pair; time-outs keep a broken test from hanging forever. */
static void make_pair(int *cli, int *srv)
{
    *cli = tcp_connect("127.0.0.1", g_port, 5);
    if (*cli < 0)
        die("connect", "127.0.0.1");
    *srv = accept(g_lfd, NULL, NULL);
    if (*srv < 0)
        die("accept", "");
    set_timeouts(*cli, 10, 10);
    set_timeouts(*srv, 10, 10);
}

typedef struct {
    conn_t       c;
    const topic_t *t;
    int          whole_topic;            /* 1: serve_topic, 0: send_file_range */
    uint32_t     idx;
    uint64_t     off;
    int          send_begin;
    uint8_t      sid[FDS_SESSION_ID_LEN];
    xfer_rc_t    rc;
    pthread_t    tid;
} job_t;

static void job_init(job_t *j, int fd, const topic_t *t)
{
    memset(j, 0, sizeof(*j));
    j->c.fd = fd;
    j->c.conn_id = ++g_conn_id;
    snprintf(j->c.peer, sizeof(j->c.peer), "test");
    j->c.kind = "get";
    j->c.status = "OK";
    snprintf(j->c.topic, sizeof(j->c.topic), "%s", t->key);
    j->t = t;
    j->whole_topic = 1;
    j->send_begin = 1;
    for (unsigned i = 0; i < FDS_SESSION_ID_LEN; i++)
        j->sid[i] = (uint8_t)(0xa0 + i);
}

/* Like run_conn(): serve, then close the socket unless it was aborted. */
static void *job_main(void *arg)
{
    job_t *j = arg;

    if (j->whole_topic)
        j->rc = serve_topic(&j->c, j->t, j->sid, j->idx, j->off, j->send_begin);
    else
        j->rc = send_file_range(&j->c, j->t, j->idx, j->off);
    if (j->c.fd >= 0) {
        close(j->c.fd);
        j->c.fd = -1;
    }
    return NULL;
}

static void job_start(job_t *j)
{
    if (pthread_create(&j->tid, NULL, job_main, j) != 0)
        die("pthread_create", "");
}

static void job_join(job_t *j)
{
    pthread_join(j->tid, NULL);
}

static void rx_init(rx_ctx_t *rx, int fd, const char *dir, rx_mode_t mode)
{
    memset(rx, 0, sizeof(*rx));
    rx->fd = fd;
    rx->dir = dir;
    rx->mode = mode;
}

/* Client side of a whole-topic serve that starts with TOPIC_BEGIN. */
static rx_rc_t client_topic(rx_ctx_t *rx, const topic_t *t, const uint8_t *sid, m_topic_end_t *end)
{
    uint8_t *p = NULL;
    size_t len = 0;
    m_topic_begin_t b;
    rx_rc_t rc = recv_expect(rx, MSG_TOPIC_BEGIN, &p, &len);

    if (rc != RX_OK)
        return rc;
    CHECK_EQ(dec_topic_begin(p, len, &b), 0);
    free(p);
    CHECK_EQ(b.file_count, t->n_files);
    CHECK_EQ(b.total_bytes, t->total_bytes);
    CHECK(memcmp(b.session_id, sid, FDS_SESSION_ID_LEN) == 0);
    return recv_topic(rx, end);
}

/* ---- Tests ---------------------------------------------------------------------------- */

static void test_full(rx_mode_t mode, const char *out_name)
{
    char out[PATH_MAX];
    job_t *j = malloc(sizeof(*j));
    recorder_t *rec = calloc(1, sizeof(*rec));
    hdr_probe_t probe = { 0, 0, 0 };
    rx_ctx_t rx;
    m_topic_end_t end;
    int cli, srv;

    make_out_dir(out, sizeof(out), out_name);
    make_pair(&cli, &srv);
    job_init(j, srv, g_t);
    j->c.progress_hook = rec_hook;
    j->c.user = rec;
    job_start(j);

    rx_init(&rx, cli, out, mode);
    rx.on_file_hdr = probe_hdr;
    rx.user = &probe;
    CHECK_EQ(client_topic(&rx, g_t, j->sid, &end), RX_OK);
    job_join(j);
    close(cli);

    CHECK_EQ(j->rc, XFER_OK);
    CHECK_STR(j->c.status, "OK");
    CHECK_EQ(j->c.data_bytes, g_t->total_bytes);
    CHECK_EQ(j->c.wire_bytes, expected_wire(g_t, 0, 0, 1));
    CHECK_EQ(j->c.chunks, g_t->total_chunks);
    CHECK_EQ(j->c.files_completed, g_t->n_files);
    CHECK_EQ(j->c.faulted, 0);

    CHECK_EQ(end.file_count, g_t->n_files);
    CHECK_EQ(end.total_bytes, g_t->total_bytes);
    CHECK_EQ(rx.data_bytes, g_t->total_bytes);
    CHECK_EQ(rx.wire_bytes, j->c.wire_bytes);      /* every byte sent was received */
    CHECK_EQ(rx.files_done, g_t->n_files);
    CHECK_EQ(rx.next_index, g_t->n_files);
    CHECK_EQ(rx.stored_bytes, g_t->total_bytes);
    CHECK_EQ(probe.calls, (int)g_t->n_files);
    CHECK_EQ(probe.early_files, 0);                /* hook runs before the file is created */
    CHECK(dir_is_complete_copy(g_t, out));         /* identical, and no .part left */

    CHECK_EQ(rec->count, g_t->n_files + g_t->total_chunks);
    CHECK(progress_ok(rec, g_t, 0, 0));
    free(rec);
    free(j);
}

/* Read the events CSV into a NUL-terminated string. */
static char *read_events(void)
{
    size_t len = 0;
    uint8_t *buf = read_file(g_events, &len);

    if (buf == NULL)
        return NULL;
    buf[len] = '\0';
    return (char *)buf;
}

static int count_substr(const char *s, const char *needle)
{
    int n = 0;

    for (const char *p = s; (p = strstr(p, needle)) != NULL; p++)
        n++;
    return n;
}

/* Fault in chunk 3 of big.bin (index 0) with a 1234-byte prefix, then a fault
 * in the 1-byte last chunk of f65537.bin (prefix clamped to 0), then resume
 * everything from the first checkpoint. */
static void test_fault_and_resume(void)
{
    char out[PATH_MAX], sid_hex[2 * FDS_SESSION_ID_LEN + 1], row[256];
    job_t *j = malloc(sizeof(*j));
    recorder_t *rec = calloc(1, sizeof(*rec));
    stub_fault_t f = { 3, 0, 1234 };
    uint32_t big = index_of(g_t, "big.bin"), f37 = index_of(g_t, "f65537.bin");
    uint64_t cut = 2 * (uint64_t)FDS_CHUNK_SIZE + 1234;
    rx_ctx_t rx;
    m_topic_end_t end;
    char *ev;
    int cli, srv;

    CHECK_EQ(big, 0);
    make_out_dir(out, sizeof(out), "out_fault");

    /* 1. fault with a prefix inside big.bin */
    make_pair(&cli, &srv);
    job_init(j, srv, g_t);
    j->c.fault_hook = stub_fault;
    j->c.fault_state = &f;
    j->c.progress_hook = rec_hook;
    j->c.user = rec;
    j->c.have_session = 1;
    memcpy(j->c.session_id, j->sid, FDS_SESSION_ID_LEN);
    job_start(j);
    rx_init(&rx, cli, out, RX_PART);
    CHECK_EQ(client_topic(&rx, g_t, j->sid, &end), RX_LOST);
    job_join(j);
    close(cli);

    CHECK_EQ(j->rc, XFER_FAULT);
    CHECK_EQ(j->c.faulted, 1);
    CHECK_STR(j->c.status, "FAULT");
    CHECK_EQ(j->c.fd, -1);                          /* conn_abort() closed it */
    CHECK_EQ(f.draws, 3);
    CHECK_EQ(j->c.data_bytes, cut);
    CHECK_EQ(j->c.chunks, 3);                       /* two full chunks + the partial one */
    CHECK_EQ(j->c.files_completed, 0);
    CHECK_EQ(j->c.wire_bytes, W_TOPIC_BEGIN + w_file_hdr("big.bin") + cut);
    CHECK_EQ(rx.data_bytes, cut);
    CHECK_EQ(rx.wire_bytes, j->c.wire_bytes);
    CHECK_EQ(file_size_at(out, "big.bin", ".part"), (int64_t)cut);
    CHECK_EQ(file_size_at(out, "big.bin", ""), -1);   /* never renamed */
    CHECK_EQ(rec->count, 4);
    CHECK(rec->r[0].idx == big && rec->r[0].off == 0 && rec->r[0].n == 0);
    CHECK(rec->r[3].idx == big && rec->r[3].off == cut && rec->r[3].n == 1234);

    ev = read_events();
    CHECK(ev != NULL);
    hex_encode(j->sid, FDS_SESSION_ID_LEN, sid_hex);
    /* ...,FAULT,conn_id,session_id,topic,file_index,offset,chunk_index,conn_data_bytes,detail */
    snprintf(row, sizeof(row), ",FAULT,%llu,%s,\"t\",0,131072,2,%llu,\"prefix=1234/65536\"\n",
             (unsigned long long)j->c.conn_id, sid_hex, (unsigned long long)cut);
    CHECK(ev != NULL && strstr(ev, row) != NULL);
    if (ev != NULL && strstr(ev, row) == NULL)
        fprintf(stderr, "expected event row fragment %s in:\n%s", row, ev);
    CHECK(ev != NULL && strncmp(ev, "run_id,case,ts_ns,event,", 24) == 0);   /* header */
    CHECK(ev != NULL && count_substr(ev, ",FAULT,") == 1);
    free(ev);

    /* 2. fault in a 1-byte chunk: the prefix (70000) is clamped to 0, the
     *    hook still fires with n = 0 */
    rec->count = 0;
    f = (stub_fault_t){ 2, 0, 70000 };
    make_pair(&cli, &srv);
    job_init(j, srv, g_t);
    j->idx = f37;
    j->c.fault_hook = stub_fault;
    j->c.fault_state = &f;
    j->c.progress_hook = rec_hook;
    j->c.user = rec;
    job_start(j);
    rx_init(&rx, cli, out, RX_PART);
    rx.next_index = f37;
    CHECK_EQ(client_topic(&rx, g_t, j->sid, &end), RX_LOST);
    job_join(j);
    close(cli);
    CHECK_EQ(j->rc, XFER_FAULT);
    CHECK_EQ(j->c.data_bytes, FDS_CHUNK_SIZE);
    CHECK_EQ(j->c.chunks, 1);                       /* an empty prefix is not a chunk */
    CHECK_EQ(file_size_at(out, "f65537.bin", ".part"), FDS_CHUNK_SIZE);
    CHECK_EQ(rec->count, 3);
    CHECK(rec->r[2].idx == f37 && rec->r[2].off == FDS_CHUNK_SIZE && rec->r[2].n == 0);
    ev = read_events();
    CHECK(ev != NULL && strstr(ev, "\"prefix=0/1\"") != NULL);
    CHECK(ev != NULL && count_substr(ev, ",FAULT,") == 2);
    free(ev);

    /* 3. resume from the first checkpoint: (big.bin, .part size), no TOPIC_BEGIN */
    rec->count = 0;
    make_pair(&cli, &srv);
    job_init(j, srv, g_t);
    j->idx = big;
    j->off = (uint64_t)file_size_at(out, "big.bin", ".part");
    j->send_begin = 0;
    j->c.progress_hook = rec_hook;
    j->c.user = rec;
    job_start(j);
    rx_init(&rx, cli, out, RX_PART);
    rx.next_index = big;
    CHECK_EQ(recv_topic(&rx, &end), RX_OK);
    job_join(j);
    close(cli);
    CHECK_EQ(j->rc, XFER_OK);
    CHECK_EQ(j->c.data_bytes, g_t->total_bytes - cut);
    CHECK_EQ(j->c.wire_bytes, expected_wire(g_t, big, cut, 0));
    CHECK_EQ(rx.wire_bytes, j->c.wire_bytes);
    CHECK_EQ(rx.data_bytes, g_t->total_bytes - cut);
    CHECK(progress_ok(rec, g_t, big, cut));
    CHECK(dir_is_complete_copy(g_t, out));          /* f65537.bin.part was redone from 0 */
    free(rec);
    free(j);
}

/* Server sends one file from `off`; the client handles FILE_HDR + recv_file. */
static rx_rc_t one_file(const char *out, uint32_t idx, uint64_t off, rx_mode_t mode, job_t *j,
                        rx_ctx_t *rx)
{
    uint8_t *p = NULL;
    size_t len = 0;
    m_file_hdr_t h;
    rx_rc_t rc;
    int cli, srv;

    make_pair(&cli, &srv);
    job_init(j, srv, g_t);
    j->whole_topic = 0;
    j->idx = idx;
    j->off = off;
    job_start(j);
    rx_init(rx, cli, out, mode);
    rc = recv_expect(rx, MSG_FILE_HDR, &p, &len);
    if (rc == RX_OK) {
        CHECK_EQ(dec_file_hdr(p, len, &h), 0);
        CHECK_EQ(h.file_index, idx);
        CHECK_EQ(h.start_offset, off);
        CHECK_EQ(h.file_size, g_t->files[idx].size);
        rc = recv_file(rx, &h);
    }
    free(p);
    close(cli);                         /* before the join: the server may still be sending */
    job_join(j);
    return rc;
}

static void test_resume_edges(void)
{
    char out[PATH_MAX], a[PATH_MAX], b[PATH_MAX];
    job_t *j = malloc(sizeof(*j));
    uint32_t f37 = index_of(g_t, "f65537.bin"), sp = index_of(g_t, "name with spaces.dat");
    uint64_t s37 = g_t->files[f37].size, ssp = g_t->files[sp].size;
    rx_ctx_t rx;
    int fd;

    /* A complete copy to work on. */
    make_out_dir(out, sizeof(out), "out_edges");
    for (uint32_t i = 0; i < g_t->n_files; i++) {
        CHECK_EQ(one_file(out, i, 0, RX_PART, j, &rx), RX_OK);
        CHECK_EQ(j->rc, XFER_OK);
    }
    CHECK(dir_is_complete_copy(g_t, out));

    /* start_offset == size, final file complete, no .part: only FILE_END. */
    CHECK_EQ(one_file(out, f37, s37, RX_PART, j, &rx), RX_OK);
    CHECK_EQ(j->rc, XFER_OK);
    CHECK_EQ(j->c.data_bytes, 0);
    CHECK_EQ(j->c.chunks, 0);
    CHECK_EQ(j->c.files_completed, 1);
    CHECK_EQ(rx.data_bytes, 0);
    CHECK_EQ(rx.files_done, 1);
    CHECK_EQ(rx.wire_bytes, w_file_hdr("f65537.bin") + W_FILE_END);
    CHECK(dir_is_complete_copy(g_t, out));

    /* start_offset == size with a complete .part (cut just before FILE_END). */
    path_join3(a, sizeof(a), out, "name with spaces.dat", "");
    path_join3(b, sizeof(b), out, "name with spaces.dat", ".part");
    CHECK_EQ(rename(a, b), 0);
    CHECK_EQ(one_file(out, sp, ssp, RX_PART, j, &rx), RX_OK);
    CHECK(dir_is_complete_copy(g_t, out));

    /* resume in the middle of a .part that is longer than the offset: cut back */
    CHECK_EQ(rename(a, b), 0);
    CHECK_EQ(one_file(out, sp, 500, RX_PART, j, &rx), RX_OK);
    CHECK_EQ(rx.data_bytes, ssp - 500);
    CHECK(dir_is_complete_copy(g_t, out));

    /* .part shorter than the offset: the checkpoint cannot be trusted */
    fd = open(b, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0 && write(fd, "0123456789", 10) == 10);
    close(fd);
    CHECK_EQ(one_file(out, sp, 500, RX_PART, j, &rx), RX_IOERR);
    CHECK_EQ(file_size_at(out, "name with spaces.dat", ".part"), 10);   /* left alone */
    CHECK_EQ(unlink(b), 0);

    /* no .part and the final file is complete, but offset < size: inconsistent */
    CHECK_EQ(one_file(out, sp, 500, RX_PART, j, &rx), RX_IOERR);
    CHECK(dir_is_complete_copy(g_t, out));

    /* RX_DIRECT never resumes */
    CHECK_EQ(one_file(out, sp, 500, RX_DIRECT, j, &rx), RX_PROTO);
    free(j);
}

static void test_unreadable(void)
{
    char out[PATH_MAX], path[PATH_MAX];
    job_t *j = malloc(sizeof(*j));
    uint32_t bi = index_of(g_locked, "b.bin");
    rx_ctx_t rx;
    m_topic_end_t end;
    int cli, srv;

    if (geteuid() == 0) {
        printf("test_unreadable skipped: root can read a mode-000 file\n");
        free(j);
        return;
    }
    path_join(path, sizeof(path), g_locked->abs_path, "b.bin");
    CHECK_EQ(chmod(path, 0), 0);
    make_out_dir(out, sizeof(out), "out_locked");
    make_pair(&cli, &srv);
    job_init(j, srv, g_locked);
    job_start(j);
    rx_init(&rx, cli, out, RX_PART);
    CHECK_EQ(client_topic(&rx, g_locked, j->sid, &end), RX_SERVER_ERR);
    job_join(j);
    close(cli);
    CHECK_EQ(rx.err.code, FDS_ERR_IO);
    CHECK(strstr(rx.err.msg, "b.bin") != NULL);
    CHECK_EQ(rx.files_done, bi);                    /* a.bin arrived before the error */
    CHECK_EQ(j->rc, XFER_IOERR);
    CHECK_STR(j->c.status, "ERR_500");
    CHECK_EQ(rx.wire_bytes, j->c.wire_bytes);       /* the ERROR is counted on both sides */
    CHECK_EQ(chmod(path, 0644), 0);
    free(j);
}

/* Write raw messages from the "server" end, then run a client call. */
static void send_raw(int fd, const uint8_t *buf, size_t n)
{
    CHECK_EQ(sendall(fd, buf, n, 0), 0);
}

static void test_client_errors(void)
{
    char out[PATH_MAX];
    uint8_t buf[512], *p;
    size_t n, len;
    m_file_hdr_t h;
    m_topic_end_t end;
    hdr_probe_t probe = { 0, 0, 1 };
    rx_ctx_t rx;
    int cli, srv;

    make_out_dir(out, sizeof(out), "out_errors");

    /* out-of-order FILE_HDR index */
    make_pair(&cli, &srv);
    memset(&h, 0, sizeof(h));
    h.file_index = 1;
    h.file_size = 3;
    snprintf(h.name, sizeof(h.name), "x.bin");
    n = enc_file_hdr(buf, sizeof(buf), &h);
    send_raw(srv, buf, n);              /* no data bytes: the stream stays in sync below */
    rx_init(&rx, cli, out, RX_PART);
    CHECK_EQ(recv_topic(&rx, &end), RX_PROTO);
    CHECK_EQ(file_size_at(out, "x.bin", ".part"), -1);   /* rejected before any file */

    /* RESUME_REJECT, wrong type, ERROR, bad header, EOF via recv_expect */
    n = enc_resume_reject(buf, sizeof(buf), FDS_REJ_AHEAD_OF_SENT);
    send_raw(srv, buf, n);
    rx_init(&rx, cli, out, RX_PART);
    p = (uint8_t *)&rx;                 /* must be reset to NULL */
    CHECK_EQ(recv_expect(&rx, MSG_RESUME_OK, &p, &len), RX_REJECT);
    CHECK_EQ(rx.reject_reason, FDS_REJ_AHEAD_OF_SENT);
    CHECK(p == NULL);
    n = enc_file_end(buf, sizeof(buf), 0);
    send_raw(srv, buf, n);
    CHECK_EQ(recv_expect(&rx, MSG_RESUME_OK, &p, &len), RX_PROTO);
    n = enc_error(buf, sizeof(buf), FDS_ERR_NOT_FOUND, "no such topic");
    send_raw(srv, buf, n);
    CHECK_EQ(recv_expect(&rx, MSG_TOPIC_BEGIN, &p, &len), RX_SERVER_ERR);
    CHECK_EQ(rx.err.code, FDS_ERR_NOT_FOUND);
    CHECK_STR(rx.err.msg, "no such topic");
    {
        m_resume_ok_t ok = { 7, 99 };
        m_resume_ok_t got;

        n = enc_resume_ok(buf, sizeof(buf), &ok);
        send_raw(srv, buf, n);
        CHECK_EQ(recv_expect(&rx, MSG_RESUME_OK, &p, &len), RX_OK);
        CHECK(p != NULL && dec_resume_ok(p, len, &got) == 0 && got.file_index == 7 &&
              got.offset == 99);
        free(p);
    }
    memset(buf, 0xee, FDS_HDR_LEN);
    send_raw(srv, buf, FDS_HDR_LEN);
    CHECK_EQ(recv_expect(&rx, MSG_TOPIC_BEGIN, &p, &len), RX_PROTO);
    close(srv);
    close(cli);

    /* EOF between messages and an error message in recv_topic */
    make_pair(&cli, &srv);
    n = enc_error(buf, sizeof(buf), FDS_ERR_BUSY, "server busy");
    send_raw(srv, buf, n);
    rx_init(&rx, cli, out, RX_PART);
    CHECK_EQ(recv_topic(&rx, &end), RX_SERVER_ERR);
    CHECK_EQ(rx.err.code, FDS_ERR_BUSY);
    close(srv);
    CHECK_EQ(recv_expect(&rx, MSG_TOPIC_BEGIN, &p, &len), RX_LOST);
    CHECK_EQ(recv_topic(&rx, &end), RX_LOST);
    close(cli);

    /* FILE_END with the wrong index */
    make_pair(&cli, &srv);
    h.file_index = 0;
    n = enc_file_hdr(buf, sizeof(buf), &h);
    send_raw(srv, buf, n);
    send_raw(srv, (const uint8_t *)"abc", 3);
    n = enc_file_end(buf, sizeof(buf), 1);
    send_raw(srv, buf, n);
    rx_init(&rx, cli, out, RX_PART);
    CHECK_EQ(recv_topic(&rx, &end), RX_PROTO);
    CHECK_EQ(file_size_at(out, "x.bin", ""), -1);         /* not renamed */
    CHECK_EQ(file_size_at(out, "x.bin", ".part"), 3);
    close(srv);
    close(cli);

    /* cut in the middle of the data, RX_DIRECT: partial file stays, RX_LOST */
    make_pair(&cli, &srv);
    h.file_size = 100;
    snprintf(h.name, sizeof(h.name), "cut.bin");
    n = enc_file_hdr(buf, sizeof(buf), &h);
    send_raw(srv, buf, n);
    memset(buf, 'z', 40);
    send_raw(srv, buf, 40);
    close(srv);
    rx_init(&rx, cli, out, RX_DIRECT);
    CHECK_EQ(recv_topic(&rx, &end), RX_LOST);
    CHECK_EQ(file_size_at(out, "cut.bin", ""), 40);
    CHECK_EQ(rx.data_bytes, 40);
    CHECK_EQ(rx.wire_bytes, n + 40);
    CHECK_EQ(rx.files_done, 0);
    close(cli);

    /* on_file_hdr refusing: RX_IOERR before any file is created */
    make_pair(&cli, &srv);
    snprintf(h.name, sizeof(h.name), "refused.bin");
    n = enc_file_hdr(buf, sizeof(buf), &h);
    send_raw(srv, buf, n);
    rx_init(&rx, cli, out, RX_PART);
    rx.on_file_hdr = probe_hdr;
    rx.user = &probe;
    CHECK_EQ(recv_topic(&rx, &end), RX_IOERR);
    CHECK_EQ(probe.calls, 1);
    CHECK_EQ(file_size_at(out, "refused.bin", ".part"), -1);
    close(srv);
    close(cli);

    /* recv_file() is public: it re-checks what dec_file_hdr() would check */
    rx_init(&rx, -1, out, RX_PART);
    h.start_offset = 101;               /* > file_size */
    CHECK_EQ(recv_file(&rx, &h), RX_PROTO);
    h.start_offset = 0;
    snprintf(h.name, sizeof(h.name), "../evil");
    CHECK_EQ(recv_file(&rx, &h), RX_PROTO);
}

static void test_server_errors(void)
{
    char out[PATH_MAX];
    job_t *j = malloc(sizeof(*j));
    rx_ctx_t rx;
    m_topic_end_t end;
    int cli, srv, small = 4096;

    make_out_dir(out, sizeof(out), "out_srv");

    /* from_idx == n_files: only TOPIC_END */
    make_pair(&cli, &srv);
    job_init(j, srv, g_t);
    j->idx = g_t->n_files;
    j->send_begin = 0;
    job_start(j);
    rx_init(&rx, cli, out, RX_PART);
    rx.next_index = g_t->n_files;
    CHECK_EQ(recv_topic(&rx, &end), RX_OK);
    job_join(j);
    close(cli);
    CHECK_EQ(j->rc, XFER_OK);
    CHECK_EQ(j->c.wire_bytes, W_TOPIC_END);
    CHECK_EQ(end.file_count, g_t->n_files);

    /* from_idx > n_files and off > size: caller errors -> ERROR 500 */
    make_pair(&cli, &srv);
    job_init(j, srv, g_t);
    j->idx = g_t->n_files + 1;
    job_start(j);
    rx_init(&rx, cli, out, RX_PART);
    CHECK_EQ(recv_topic(&rx, &end), RX_SERVER_ERR);
    CHECK_EQ(rx.err.code, FDS_ERR_IO);
    job_join(j);
    close(cli);
    CHECK_EQ(j->rc, XFER_IOERR);
    CHECK_STR(j->c.status, "ERR_500");

    make_pair(&cli, &srv);
    job_init(j, srv, g_t);
    j->whole_topic = 0;
    j->idx = 0;
    j->off = g_t->files[0].size + 1;
    job_start(j);
    rx_init(&rx, cli, out, RX_PART);
    CHECK_EQ(recv_topic(&rx, &end), RX_SERVER_ERR);
    job_join(j);
    close(cli);
    CHECK_EQ(j->rc, XFER_IOERR);

    /* the peer vanishes: XFER_PEER / PEER_LOST, and the process survives
     * (main() ignores SIGPIPE as server_run() does: sendfile() has no
     * MSG_NOSIGNAL) */
    make_pair(&cli, &srv);
    setsockopt(srv, SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
    close(cli);
    job_init(j, srv, g_t);
    job_start(j);
    job_join(j);
    CHECK_EQ(j->rc, XFER_PEER);
    CHECK_STR(j->c.status, "PEER_LOST");
    CHECK(j->c.data_bytes < g_t->total_bytes);
    free(j);
}

static void test_empty_topic(void)
{
    char out[PATH_MAX];
    job_t *j = malloc(sizeof(*j));
    rx_ctx_t rx;
    m_topic_end_t end;
    int cli, srv;

    make_out_dir(out, sizeof(out), "out_empty");
    make_pair(&cli, &srv);
    job_init(j, srv, g_empty);
    memset(j->sid, 0, sizeof(j->sid));
    job_start(j);
    rx_init(&rx, cli, out, RX_DIRECT);
    CHECK_EQ(client_topic(&rx, g_empty, j->sid, &end), RX_OK);
    job_join(j);
    close(cli);
    CHECK_EQ(j->rc, XFER_OK);
    CHECK_EQ(end.file_count, 0);
    CHECK_EQ(end.total_bytes, 0);
    CHECK_EQ(j->c.wire_bytes, W_TOPIC_BEGIN + W_TOPIC_END);
    CHECK_EQ(rx.wire_bytes, W_TOPIC_BEGIN + W_TOPIC_END);
    CHECK_EQ(j->c.data_bytes, 0);
    CHECK(dir_is_complete_copy(g_empty, out));
    free(j);
}

int main(void)
{
    const char *tmp = env_str("TMPDIR");
    log_ctx_t lc = { "server", "test", "multi", 0.5, 0, 0 };
    char dir[PATH_MAX];

    if (path_join(g_base, sizeof(g_base), tmp != NULL ? tmp : "/tmp", "fds_xfer_XXXXXX") != 0 ||
        mkdtemp(g_base) == NULL)
        die("mkdtemp", g_base);
    if (path_join(g_root, sizeof(g_root), g_base, "root") != 0 ||
        path_join(g_events, sizeof(g_events), g_base, "events.csv") != 0)
        die("path", g_base);

    /* topic "t": every interesting size around the 64 KB chunk boundary */
    path_join(dir, sizeof(dir), g_root, "t");
    if (mkdir_p(dir, 0755) != 0)
        die("mkdir", dir);
    write_file(dir, "empty.bin", 0, 0);
    write_file(dir, "one.bin", 1, 0);
    write_file(dir, "f65535.bin", 65535, 1);
    write_file(dir, "f65536.bin", 65536, 1);
    write_file(dir, "f65537.bin", 65537, 1);
    write_file(dir, "big.bin", 1 << 20, 1);
    write_file(dir, "name with spaces.dat", 1000, 0);
    path_join(dir, sizeof(dir), g_root, "empty");
    if (mkdir_p(dir, 0755) != 0)
        die("mkdir", dir);
    path_join(dir, sizeof(dir), g_root, "locked");
    if (mkdir_p(dir, 0755) != 0)
        die("mkdir", dir);
    write_file(dir, "a.bin", 100, 0);
    write_file(dir, "b.bin", 1000, 0);

    /* Same as server_run(): sendfile() raises SIGPIPE on a reset peer. */
    signal(SIGPIPE, SIG_IGN);
    setenv("FDS_EVENTS", g_events, 1);
    setenv("FDS_QUIET", "1", 1);
    log_init(&lc);
    if (topic_map_load(&g_map, g_root) != 0)
        die("topic_map_load", g_root);
    g_t = topic_find(&g_map, "t");
    g_empty = topic_find(&g_map, "empty");
    g_locked = topic_find(&g_map, "locked");
    if (g_t == NULL || g_empty == NULL || g_locked == NULL || g_t->n_files != 7)
        die("topics", g_root);
    g_lfd = listen_random(&g_port);
    if (g_lfd < 0)
        die("listen", "");

    test_full(RX_DIRECT, "out_direct");
    test_full(RX_PART, "out_part");
    test_fault_and_resume();
    test_resume_edges();
    test_unreadable();
    test_client_errors();
    test_server_errors();
    test_empty_topic();

    close(g_lfd);
    log_close();
    topic_map_free(&g_map);
    nftw(g_base, rm_entry, 16, FTW_DEPTH | FTW_PHYS);
    UNIT_DONE();
}
