/*
 * client.c - Part II Case 2 Enhanced client: download one topic over K
 * parallel TCP connections, with session management and recovery of every
 * connection on its own. main(), the blocking-threads engine and the helpers
 * shared with the poll() engine (client_poll.c) live here; the contract is
 * enh_client.h.
 *
 * IT305 Socket Programming Project, Group 006 + 043.
 *
 * Usage: ./client <server_ip> <server_port> <topic> <output_dir>
 *                 [connections] [max_retries] [threads|poll]
 *        connections: 1..16, default 4 (FDS_ENH_DEFAULT_K)
 *        max_retries: 1..1000000, default 20 (FDS_CASE2_MAX_RETRIES)
 *        io:          threads (default) or poll
 *
 * Files are written to output_dir/<topic exactly as typed>/ (deviation 4).
 * On success that directory holds only the topic's files.
 *
 * ---- Design (design sections 3.8, 6.1, 6.2; deviations 11, 13, 18) --------
 *
 * Why K connections. Animals-10 images are tens of KB, so one connection
 * spends a large share of its time on the per-file turnaround (FETCH out,
 * first byte back) rather than moving data, and after a fault it is idle
 * during the back-off and the new handshake. With K connections the others
 * keep the link busy while one is waiting. Beyond a few connections they only
 * compete for the same bandwidth, so K = 4 is the default and K = 1, 2, 4, 8
 * are measured (deviation 15).
 *
 * How the work is split: a DYNAMIC queue of files (workq.c). Step 1 asks for
 * the MANIFEST (every file's index, size and name) on one short connection.
 * Step 2 puts every file that is not already complete on disk into the
 * queue as (index, start offset). Step 3 starts K workers; each repeatedly
 * pops the next item and FETCHes that one file. A fast connection simply
 * takes more files, so the load balances itself without any static plan, and
 * a file never needs to be split.
 *
 * How the data is reassembled: it is not, because nothing is ever split.
 * Each file is owned by exactly one worker at a time (the one that popped
 * it) and is written to its own <name>.part, renamed to <name> on FILE_END.
 * Files are independent, so there is no byte-stream reassembly, no ordering
 * between connections and no shared write position: K workers write K
 * different files.
 *
 * How a failed connection is recovered. When a connection breaks (EOF,
 * reset, 15 s time-out, ERROR 503) its worker re-queues the item with the
 * current size of its .part file, which is exactly the number of bytes
 * stored (the checkpoint: every byte received is written before the next
 * recv()). It then backs off, reconnects, and goes on with the next item;
 * whichever worker pops the re-queued item sends FETCH(file, offset) and the
 * server sends only the missing bytes. The other K-1 connections are not
 * affected at all. A FETCH is validated by the server like a Case 2 RESUME
 * (design 5.5): RESUME_REJECT 4/5 (offset impossible) -> delete the .part and
 * fetch the file from 0; 1/2/3 (the topic changed on the server) -> throw
 * away the partial files and start over from the MANIFEST.
 *
 * Retry policy (deviation 18): each connection keeps a streak = consecutive
 * attempts that stored no new byte. Any progress resets it (the server is
 * clearly up; it was an injected fault, so the wait is the minimum 50 ms);
 * otherwise the wait doubles from 50 ms to 1 s. A connection whose streak
 * reaches max_retries aborts the whole run with exit 2.
 *
 * Why blocking threads are the main engine (design 6.1/6.2): a blocking
 * worker is a straight-line program (send FETCH, read header, read data,
 * handle the result), which is easy to get right and to defend. Its cost is
 * one thread per connection, which is nothing for K <= 16, and the kernel
 * already overlaps the K blocking reads. The poll() engine (client_poll.c,
 * io=poll) does the same work with one thread and non-blocking sockets, as
 * the measured alternative the assignment's "socket mode" asks for.
 *
 * Thread safety of the threads engine:
 *   - Read-only while the workers run: e->man, e->dir, e->topic, e->host,
 *     e->port, e->session_id, e->k, e->max_retries (all set before
 *     pthread_create(), which orders those writes before the threads start).
 *   - Every shared counter and flag (data/wire bytes, files_done, attempts,
 *     reconnects, rejects, fatal, gave_up, restart) is changed only under
 *     e->mu, through the small helpers below.
 *   - done[i] is written only by the worker that holds item i (and under mu
 *     anyway); main reads it only after pthread_join().
 *   - The queue has its own mutex and condition variable (workq.c).
 *   - Each worker owns its socket and its rx_ctx_t (receive counters), so
 *     the receive path itself shares nothing.
 *   - One FETCH outstanding per connection: never pipelined (deviation 13;
 *     see the top of server.c for why an unread request would make the
 *     server's close() an RST that destroys data already sent).
 *
 * Exit status: 0 success, 1 fatal error, 2 gave up after max_retries.
 */
#include "checkpoint.h"
#include "enh_client.h"
#include "fds.h"
#include "log.h"
#include "net.h"
#include "protocol.h"
#include "transfer.h"
#include "util.h"
#include "workq.h"

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_RETRIES_LIMIT 1000000L

/* Largest encoded MANIFEST_REQ: header, topic_len u8, topic, connections u8. */
#define MANIFEST_REQ_MAX (FDS_HDR_LEN + 1u + FDS_TOPIC_MAX + 1u)
/* Largest encoded FETCH: header, id, index u32, offset u64, topic, name. */
#define FETCH_MAX (FDS_HDR_LEN + FDS_SESSION_ID_LEN + 4u + 8u + 1u + FDS_TOPIC_MAX + \
                   2u + FDS_NAME_MAX)

/* The FETCH carries the manifest's name verbatim. */
_Static_assert(sizeof(((m_resume_t *)0)->name) == sizeof(((file_entry_t *)0)->name),
               "m_resume_t.name and file_entry_t.name must have the same size");

/* How one manifest attempt ended. */
typedef enum {
    STEP_OK,                         /* manifest received and decoded */
    STEP_RETRY,                      /* failed, another attempt may succeed */
    STEP_FATAL                       /* failed for good (already reported) */
} step_t;

/* One worker of the threads engine: everything it owns. */
typedef struct {
    enh_t   *e;
    int      id;                     /* 1..K, used in the progress lines */
    int      fd;                     /* its connection, -1 = none */
    int      streak;                 /* consecutive attempts without progress */
    int      failed;                 /* its last connection ended in a failure */
    rx_ctx_t rx;                     /* its receive context (counters are cumulative) */
} worker_t;

/* Run timing for the summary and the CSV row. */
typedef struct {
    int64_t start_ns, end_ns;
    int64_t start_wall_ns, end_wall_ns;
} run_times_t;

/* ---- messages ------------------------------------------------------------- */

static void usage(const char *prog, const char *why)
{
    if (why != NULL)
        fprintf(stderr, "client: %s\n", why);
    fprintf(stderr, "usage: %s <server_ip> <server_port> <topic> <output_dir> "
                    "[connections=%d] [max_retries=%d] [threads|poll]\n",
            prog, FDS_ENH_DEFAULT_K, FDS_CASE2_MAX_RETRIES);
    exit(FDS_EXIT_FATAL);
}

/*
 * "[client] error: ..." on stderr, always (even with FDS_QUIET). Several
 * workers may report at once; flockfile() keeps each line in one piece.
 */
static void print_error(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void print_error(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    flockfile(stderr);
    fputs("[client] error: ", stderr);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    funlockfile(stderr);
    va_end(ap);
}

/* strerror() that does not print "Success" when no errno was recorded. */
static const char *errno_text(int err)
{
    return err != 0 ? strerror(err) : "unknown error";
}

/* Why a receive ended early, from the errno right after it. An expired
 * SO_RCVTIMEO shows up as EAGAIN (== EWOULDBLOCK on Linux). */
static const char *lost_text(int err)
{
    if (err == EAGAIN)
        return "no data for 15 s";
    return err != 0 ? strerror(err) : "closed by the server";
}

/* Report an ERROR that retrying cannot cure (everything except 503). */
static void report_server_error(const enh_t *e, const m_error_t *err)
{
    if (err->code == FDS_ERR_NOT_FOUND)
        print_error("server error 404: %s (the topic '%s' does not exist on the server)",
                    err->msg, e->topic);
    else
        print_error("server error %u: %s", (unsigned)err->code, err->msg);
}

/* ---- helpers shared with the poll() engine (enh_client.h) ------------------ */

/*
 * Open one connection. Part II keeps the 15 s time-outs (deviation 1 is Part I
 * only): a server that stops sending without closing must still count as a
 * failure. TCP_NODELAY (deviation 10) because every file is a small
 * request/response turnaround that Nagle + delayed ACK could stall by ~40 ms.
 * Every try counts in attempts (also one that fails), and in reconnects when
 * it follows a failure. The returned socket is blocking.
 */
int enh_connect(enh_t *e, int is_reconnect)
{
    int fd = tcp_connect(e->host, e->port, FDS_CONNECT_TIMEOUT_S);
    int err = errno;

    pthread_mutex_lock(&e->mu);
    e->attempts++;
    if (is_reconnect)
        e->reconnects++;
    pthread_mutex_unlock(&e->mu);

    if (fd < 0) {
        errno = err;                 /* for the caller's message */
        return -1;
    }
    if (set_timeouts(fd, FDS_CLI_TIMEOUT_S, FDS_CLI_TIMEOUT_S) != 0) {
        err = errno;
        (void)close(fd);
        errno = err;
        return -1;
    }
    if (set_nodelay(fd) != 0)
        log_warn("TCP_NODELAY failed: %s", strerror(errno));
    apply_env_bufsizes(fd);          /* only if FDS_SNDBUF / FDS_RCVBUF are set */
    return fd;
}

/* File idx is complete on disk (renamed from .part). Counted once only. */
void enh_file_completed(enh_t *e, uint32_t idx)
{
    pthread_mutex_lock(&e->mu);
    if (!e->done[idx]) {
        e->done[idx] = 1;
        e->files_done++;
    }
    pthread_mutex_unlock(&e->mu);
}

void enh_add_bytes(enh_t *e, uint64_t data, uint64_t wire)
{
    pthread_mutex_lock(&e->mu);
    e->data_bytes += data;
    e->wire_bytes += wire;
    pthread_mutex_unlock(&e->mu);
}

/*
 * The re-queue offset of file idx: the size of its .part, i.e. exactly the
 * bytes stored so far (0 if there is none, e.g. the connection broke before
 * the FILE_HDR). Never larger than what the server sent, so the next FETCH
 * passes check 5.
 */
uint64_t enh_part_offset(enh_t *e, uint32_t idx)
{
    uint64_t sz = 0;

    if (part_size(e->dir, e->man.files[idx].name, &sz) != 0)
        return 0;
    return sz;
}

/* Remove <name>.part (a missing file is fine). Failure only costs a warning:
 * a FETCH from offset 0 truncates the .part anyway. */
static void unlink_part(const enh_t *e, const char *name)
{
    char part[PATH_MAX];

    if (path_join3(part, sizeof(part), e->dir, name, PART_SUFFIX) != 0)
        return;
    if (unlink(part) != 0 && errno != ENOENT)
        log_warn("cannot remove %s: %s", part, strerror(errno));
}

/* Count one RESUME_REJECT. */
static void count_reject(enh_t *e)
{
    pthread_mutex_lock(&e->mu);
    e->rejects++;
    pthread_mutex_unlock(&e->mu);
}

/*
 * Set one of the run-ending flags (fatal, gave_up, restart) and abort the
 * queue: every worker's next workq_pop() returns 0, so they all finish the
 * item in their hands (bounded by the 15 s time-outs) and stop.
 */
static void stop_run(enh_t *e, int *flag)
{
    pthread_mutex_lock(&e->mu);
    *flag = 1;
    pthread_mutex_unlock(&e->mu);
    workq_abort(e->q);
}

/* ---- step 1: the manifest ------------------------------------------------- */

/*
 * One manifest attempt on its own short connection:
 * MANIFEST_REQ(topic as typed, K) -> MANIFEST, or ERROR.
 * The payload may be up to 64 MiB (deviation 2); recv_msg() enforces that.
 */
static step_t manifest_attempt(enh_t *e, int is_reconnect, char *why, size_t cap)
{
    uint8_t req[MANIFEST_REQ_MAX];
    m_manifest_req_t mr;
    m_error_t err;
    msg_hdr_t h;
    uint8_t *payload = NULL;
    uint64_t wire = 0;
    step_t res;
    size_t n;
    int fd, rc, saved;

    memset(&mr, 0, sizeof(mr));
    memcpy(mr.topic, e->topic, strlen(e->topic) + 1);   /* validated: <= 63 bytes */
    mr.connections = (uint8_t)e->k;
    n = enc_manifest_req(req, sizeof(req), &mr);
    if (n == 0) {
        print_error("cannot encode the MANIFEST_REQ");
        return STEP_FATAL;
    }

    fd = enh_connect(e, is_reconnect);
    if (fd < 0) {
        snprintf(why, cap, "cannot connect to %s:%u: %s", e->host, (unsigned)e->port,
                 errno_text(errno));
        return STEP_RETRY;
    }
    if (send_encoded(fd, req, n, 0) != 0) {
        snprintf(why, cap, "cannot send the MANIFEST_REQ: %s", errno_text(errno));
        (void)close(fd);
        return STEP_RETRY;
    }
    errno = 0;
    rc = recv_msg(fd, &h, &payload, &wire);
    saved = errno;
    /* The server closes after the MANIFEST; nothing else will arrive. */
    if (close(fd) != 0)
        log_warn("close: %s", strerror(errno));
    enh_add_bytes(e, 0, wire);

    if (rc == NET_EOF || rc == NET_ERR) {
        snprintf(why, cap, "connection lost before the manifest arrived (%s)", lost_text(saved));
        return STEP_RETRY;
    }
    if (rc == NET_PROTO) {
        print_error("protocol error: the server sent an invalid message header");
        return STEP_FATAL;
    }
    if (h.type == MSG_MANIFEST) {
        /* dec_manifest() checks every name and that the sizes add up. */
        res = STEP_OK;
        if (dec_manifest(payload, (size_t)h.payload_len, &e->man) != 0) {
            print_error("protocol error: invalid MANIFEST");
            res = STEP_FATAL;
        }
    } else if (h.type == MSG_ERROR && dec_error(payload, (size_t)h.payload_len, &err) == 0) {
        if (err.code == FDS_ERR_BUSY) {
            /* Thread limit or session table full: the server is fine, just full. */
            snprintf(why, cap, "server busy (503: %.200s)", err.msg);
            res = STEP_RETRY;
        } else {
            report_server_error(e, &err);   /* 404 / 400 / 500 */
            res = STEP_FATAL;
        }
    } else {
        print_error("protocol error: expected MANIFEST, got %s", msg_type_name(h.type));
        res = STEP_FATAL;
    }
    free(payload);
    return res;
}

/*
 * Step 1 with retries, then choose the session id and save .session.
 * No file data flows here, so every failed attempt adds to the streak.
 */
int enh_fetch_manifest(enh_t *e)
{
    char why[512];
    ckpt_t ck;
    int lr, streak = 0;

    /*
     * A .session for THIS topic means the files in the directory belong to an
     * interrupted run of ours, so they may be trusted (step 2). A damaged one
     * is removed and treated as absent: a fresh start is always safe.
     */
    memset(&ck, 0, sizeof(ck));
    lr = ckpt_load(e->dir, &ck);
    e->had_session = lr == 0 && strcmp(ck.topic, e->topic) == 0;
    if (lr < 0 && ckpt_clear(e->dir) != 0)
        log_warn("cannot remove the damaged %s in %s: %s", CKPT_FILE, e->dir, strerror(errno));

    for (;;) {
        step_t s;
        long wait;

        why[0] = '\0';
        s = manifest_attempt(e, streak > 0, why, sizeof(why));
        if (s == STEP_OK)
            break;
        if (s == STEP_FATAL)
            return FDS_EXIT_FATAL;
        streak++;
        if (streak >= e->max_retries) {
            log_info("manifest attempt %d/%d failed: %s", streak, e->max_retries, why);
            fprintf(stderr, "[client] giving up after %d attempts without progress\n", streak);
            return FDS_EXIT_GAVE_UP;
        }
        wait = backoff_ms(streak);
        log_info("manifest attempt %d/%d failed: %s; retrying in %ld ms", streak,
                 e->max_retries, why, wait);
        sleep_ms(wait);
    }

    /*
     * Resuming: keep the OLD session id. The server either still has that
     * record and checks every FETCH against what it sent (check 5), or it
     * adopts the id (server restarted). The new id from this manifest stays
     * unused. Fresh run: use the manifest's id.
     */
    if (e->had_session)
        memcpy(e->session_id, ck.session_id, sizeof(e->session_id));
    else
        memcpy(e->session_id, e->man.session_id, sizeof(e->session_id));

    memset(&ck, 0, sizeof(ck));
    memcpy(ck.session_id, e->session_id, sizeof(ck.session_id));
    memcpy(ck.topic, e->topic, strlen(e->topic) + 1);
    ck.file_index = -1;              /* Enhanced has no single "current file" */
    ck.k = e->k;
    if (ckpt_save(e->dir, &ck) != 0) {
        print_error("cannot write %s/%s: %s", e->dir, CKPT_FILE, strerror(errno));
        return FDS_EXIT_FATAL;
    }
    log_info("%s: %u files, %llu bytes; %s session, k=%d", e->topic, e->man.file_count,
             (unsigned long long)e->man.total_bytes, e->had_session ? "resuming the" : "new",
             e->k);
    return FDS_EXIT_OK;
}

/* ---- step 2: the queue ----------------------------------------------------- */

/*
 * One item per file still to fetch (enh_client.h, step 2). Files on disk are
 * trusted only when a .session of ours existed; otherwise everything is
 * fetched from 0 and stale files are overwritten.
 *
 * Skipped (already complete) files are marked done here and counted in
 * files_done, so files_done always means "files complete on disk", and on
 * success it equals the manifest's file count.
 */
int enh_build_queue(enh_t *e)
{
    uint32_t n = e->man.file_count, skipped = 0, resumed = 0;
    uint64_t skipped_bytes = 0;

    e->done = calloc(n > 0 ? n : 1, 1);
    e->q = workq_create(n);
    if (e->done == NULL || e->q == NULL) {
        free(e->done);
        workq_destroy(e->q);
        e->done = NULL;
        e->q = NULL;
        return -1;
    }
    for (uint32_t i = 0; i < n; i++) {
        const file_entry_t *f = &e->man.files[i];
        uint64_t sz, off = 0;

        if (e->had_session) {
            if (final_size(e->dir, f->name, &sz) == 0 && sz == f->size) {
                e->done[i] = 1;
                e->files_done++;
                skipped++;
                skipped_bytes += f->size;
                /* A stale <name>.part next to a complete file would be left in
                 * the folder at the end; nobody will fetch this file again. */
                unlink_part(e, f->name);
                continue;
            }
            /* A .part longer than the file cannot be ours (the server would
             * reject that offset with reason 4): it is removed below. */
            if (part_size(e->dir, f->name, &sz) == 0 && sz <= f->size)
                off = sz;
        }
        /*
         * Invariant for the engines: from here on, a <name>.part on disk holds
         * only bytes of THIS session, so its size is a valid re-queue offset
         * (enh_part_offset). An untrusted .part must go now: if the first
         * attempt for the file were lost before its FILE_HDR (before the
         * receiver truncates the .part), the item would otherwise be
         * re-queued at the stale size, and an adopted session (server
         * restarted) skips check 5, so foreign bytes could end up in the file.
         */
        if (off == 0)
            unlink_part(e, f->name);
        /* Cannot fail: the capacity is the file count. */
        if (workq_push(e->q, i, off) != 0)
            return -1;
        if (off > 0)
            resumed++;
    }
    if (e->had_session)
        log_info("checkpoint: %u of %u files already complete (%llu bytes), %u resume from a .part",
                 skipped, n, (unsigned long long)skipped_bytes, resumed);
    return 0;
}

/* ---- step 3: the threads engine --------------------------------------------- */

/* Close the worker's connection, if any. The client closes first at the end
 * of a run, so the server's recv() sees a clean EOF. */
static void close_conn(worker_t *w)
{
    if (w->fd < 0)
        return;
    if (close(w->fd) != 0)
        log_warn("close: %s", strerror(errno));
    w->fd = -1;
    w->rx.fd = -1;
}

/*
 * 1 if the idle connection can carry another FETCH. A worker may wait a long
 * time in workq_pop() at the end of a run (another worker still owns the last
 * files), and the server closes a connection that stays silent for its 30 s
 * time-out. A FETCH sent on such a connection would only come back as a
 * "lost" connection and a false reconnect, so a cheap non-blocking peek
 * checks first: EAGAIN = alive and silent (the normal case); 0 = the server
 * closed it; data = something unsolicited (the stream is no longer in step);
 * any other error = reset.
 */
static int conn_reusable(int fd)
{
    char b;
    ssize_t r = recv(fd, &b, 1, MSG_PEEK | MSG_DONTWAIT);

    return r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR);
}

/*
 * The connection failed while this worker held item `it`. Hand the item back
 * with the offset stored so far, then count the attempt and back off.
 * Push BEFORE done: in between, the item is either queued or busy, so no
 * other worker can see "queue empty and nobody busy" and stop early.
 * Only this worker sleeps; the other K-1 keep fetching (and one of them may
 * pick the re-queued item up at once).
 */
static void connection_failed(worker_t *w, const work_item_t *it, uint64_t new_off,
                              uint64_t got, const char *why)
{
    enh_t *e = w->e;
    long wait;

    close_conn(w);
    w->failed = 1;
    if (workq_push(e->q, it->idx, new_off) != 0) {
        /* Cannot happen (only the holder re-queues an item); never lose a file. */
        print_error("work queue overflow re-queueing file %u", it->idx);
        workq_done(e->q);
        stop_run(e, &e->fatal);
        return;
    }
    workq_done(e->q);

    /* Any byte stored proves the server is up: reset (deviation 18). */
    w->streak = got > 0 ? 0 : w->streak + 1;
    if (w->streak >= e->max_retries) {
        fprintf(stderr, "[client] giving up: connection %d made no progress in %d "
                        "consecutive attempts (last: %s)\n", w->id, w->streak, why);
        stop_run(e, &e->gave_up);
        return;
    }
    wait = backoff_ms(FDS_MAX(w->streak, 1));
    log_info("[conn %d] %s; re-queued file %u at offset %llu, retry in %ld ms", w->id, why,
             it->idx, (unsigned long long)new_off, wait);
    sleep_ms(wait);
}

/* Connect if the worker has no connection. Returns 0 if connected; -1 if
 * the attempt failed (the item has been handed back). */
static int ensure_conn(worker_t *w, const work_item_t *it)
{
    char why[256];

    if (w->fd >= 0 && !conn_reusable(w->fd))
        close_conn(w);               /* closed while idle: reopen, not a failure */
    if (w->fd >= 0)
        return 0;
    w->fd = enh_connect(w->e, w->failed);
    if (w->fd >= 0) {
        w->failed = 0;
        w->rx.fd = w->fd;
        return 0;
    }
    snprintf(why, sizeof(why), "cannot connect to %s:%u (%s)", w->e->host,
             (unsigned)w->e->port, errno_text(errno));
    connection_failed(w, it, it->off, 0, why);
    return -1;
}

/*
 * One request: FETCH(session id, idx, off, topic, name) -> FILE_HDR, which
 * must describe exactly the file and offset asked for (and match the
 * manifest), -> the bytes into <name>.part -> FILE_END (rename to <name>).
 */
static rx_rc_t fetch_item(worker_t *w, const work_item_t *it)
{
    enh_t *e = w->e;
    const file_entry_t *f = &e->man.files[it->idx];
    uint8_t buf[FETCH_MAX];
    uint8_t *payload = NULL;
    m_file_hdr_t h;
    m_resume_t m;
    size_t len = 0, n;
    rx_rc_t rc;

    memset(&m, 0, sizeof(m));
    memcpy(m.session_id, e->session_id, sizeof(m.session_id));
    m.file_index = it->idx;
    m.offset = it->off;
    memcpy(m.topic, e->topic, strlen(e->topic) + 1);
    memcpy(m.name, f->name, sizeof(m.name));
    n = enc_resume(buf, sizeof(buf), MSG_FETCH, &m);
    if (n == 0) {
        errno = EINVAL;              /* cannot happen: topic and name were validated */
        return RX_PROTO;
    }
    if (send_encoded(w->fd, buf, n, 0) != 0)
        return RX_LOST;

    w->rx.next_index = it->idx;
    rc = recv_expect(&w->rx, MSG_FILE_HDR, &payload, &len);
    if (rc == RX_OK && dec_file_hdr(payload, len, &h) != 0)
        rc = RX_PROTO;
    free(payload);
    if (rc != RX_OK)
        return rc;
    /* Never write a byte that is not the one asked for. */
    if (h.file_index != it->idx || h.start_offset != it->off || h.file_size != f->size ||
        strcmp(h.name, f->name) != 0)
        return RX_PROTO;
    return recv_file(&w->rx, &h);
}

/*
 * RESUME_REJECT for item `it`.
 *   4/5: this .part cannot be resumed (longer than the file, or ahead of what
 *        the server's record says it sent): delete it and fetch the file from
 *        0 on the same connection, which the server left open.
 *   1/2/3: the topic on the server is not the one of our manifest: nothing
 *        partial can be trusted; main starts over from the MANIFEST.
 */
static void handle_reject(worker_t *w, const work_item_t *it)
{
    enh_t *e = w->e;
    uint16_t reason = w->rx.reject_reason;

    count_reject(e);
    if ((reason == FDS_REJ_OFFSET_TOO_BIG || reason == FDS_REJ_AHEAD_OF_SENT) && it->off > 0) {
        unlink_part(e, e->man.files[it->idx].name);
        log_info("[conn %d] FETCH of file %u at offset %llu rejected (%s); fetching it from 0",
                 w->id, it->idx, (unsigned long long)it->off, reject_reason_str(reason));
        if (workq_push(e->q, it->idx, 0) != 0) {
            print_error("work queue overflow re-queueing file %u", it->idx);
            workq_done(e->q);
            stop_run(e, &e->fatal);
            return;
        }
        workq_done(e->q);
        return;
    }
    workq_done(e->q);
    if (reason >= FDS_REJ_UNKNOWN_TOPIC && reason <= FDS_REJ_NAME_MISMATCH) {
        log_info("[conn %d] FETCH of file %u rejected (%s): the topic changed on the server; "
                 "starting over", w->id, it->idx, reject_reason_str(reason));
        stop_run(e, &e->restart);
        return;
    }
    /* An unknown reason, or 4/5 for offset 0, which the server never sends:
     * retrying would loop for ever. */
    print_error("protocol error: unexpected RESUME_REJECT (reason %u) for file %u at offset %llu",
                (unsigned)reason, it->idx, (unsigned long long)it->off);
    stop_run(e, &e->fatal);
}

/* Fetch item `it` on the worker's connection and act on the result. */
static void do_item(worker_t *w, const work_item_t *it)
{
    enh_t *e = w->e;
    uint64_t d0 = w->rx.data_bytes, w0 = w->rx.wire_bytes, got;
    char why[256];
    rx_rc_t rc;
    int err;

    errno = 0;
    rc = fetch_item(w, it);
    err = errno;
    got = w->rx.data_bytes - d0;
    enh_add_bytes(e, got, w->rx.wire_bytes - w0);

    switch (rc) {
    case RX_OK:
        enh_file_completed(e, it->idx);
        workq_done(e->q);
        w->streak = 0;               /* a completed file is progress */
        return;
    case RX_LOST: {
        uint64_t new_off = enh_part_offset(e, it->idx);

        snprintf(why, sizeof(why), "lost at file %u offset %llu (%s)", it->idx,
                 (unsigned long long)new_off, lost_text(err));
        connection_failed(w, it, new_off, got, why);
        return;
    }
    case RX_SERVER_ERR:
        if (w->rx.err.code == FDS_ERR_BUSY) {
            /* The server closes after a 503: same as a lost connection. */
            snprintf(why, sizeof(why), "server busy (503: %.200s)", w->rx.err.msg);
            connection_failed(w, it, enh_part_offset(e, it->idx), got, why);
            return;
        }
        report_server_error(e, &w->rx.err);
        break;
    case RX_REJECT:
        handle_reject(w, it);
        return;
    case RX_PROTO:
        print_error("protocol error on connection %d: invalid or unexpected reply to the "
                    "FETCH of file %u", w->id, it->idx);
        break;
    case RX_IOERR:
        print_error("cannot write file %s into %s: %s", e->man.files[it->idx].name, e->dir,
                    errno_text(err));
        break;
    }
    /* Fatal: retrying cannot cure it. */
    workq_done(e->q);
    stop_run(e, &e->fatal);
}

/*
 * A worker thread: pop, (re)connect, fetch, until the queue reports the
 * topic complete or the run was aborted (workq_pop() returns 0 in both
 * cases). The connection is opened lazily, so a worker that never gets an
 * item (K > number of files) never connects.
 */
static void *worker_main(void *arg)
{
    worker_t *w = arg;
    work_item_t it;

    while (workq_pop(w->e->q, &it)) {
        if (ensure_conn(w, &it) != 0)
            continue;                /* item handed back; the next pop decides */
        do_item(w, &it);
    }
    close_conn(w);
    return NULL;
}

/* fatal -> 1, gave_up -> 2, otherwise 0 (main checks restart itself). */
static int engine_exit_code(enh_t *e)
{
    int code = FDS_EXIT_OK;

    pthread_mutex_lock(&e->mu);
    if (e->fatal)
        code = FDS_EXIT_FATAL;
    else if (e->gave_up)
        code = FDS_EXIT_GAVE_UP;
    pthread_mutex_unlock(&e->mu);
    return code;
}

int enh_run_threads(enh_t *e)
{
    worker_t w[FDS_ENH_MAX_K];
    pthread_t tid[FDS_ENH_MAX_K];
    int started = 0;

    for (int i = 0; i < e->k; i++) {
        int rc;

        memset(&w[i], 0, sizeof(w[i]));
        w[i].e = e;
        w[i].id = i + 1;
        w[i].fd = -1;
        w[i].rx.fd = -1;
        w[i].rx.dir = e->dir;
        w[i].rx.mode = RX_PART;      /* <name>.part, renamed on FILE_END */
        rc = pthread_create(&tid[i], NULL, worker_main, &w[i]);
        if (rc != 0) {
            /* Fewer connections still finish the topic, just more slowly. */
            log_warn("cannot start worker %d: %s; continuing with %d", i + 1, strerror(rc),
                     started);
            break;
        }
        started++;
    }
    if (started == 0) {
        print_error("cannot start any worker thread");
        pthread_mutex_lock(&e->mu);
        e->fatal = 1;
        pthread_mutex_unlock(&e->mu);
        return FDS_EXIT_FATAL;
    }
    /* Join every worker on every path: no thread outlives the queue. */
    for (int i = 0; i < started; i++) {
        int rc = pthread_join(tid[i], NULL);

        if (rc != 0)
            log_warn("pthread_join(worker %d): %s", i + 1, strerror(rc));
    }
    return engine_exit_code(e);
}

/* ---- main ----------------------------------------------------------------- */

static void parse_args(int argc, char **argv, enh_t *e)
{
    long k = FDS_ENH_DEFAULT_K, retries = FDS_CASE2_MAX_RETRIES;
    const char *out_root;

    if (argc < 5 || argc > 8)
        usage(argv[0], NULL);
    e->host = argv[1];
    if (e->host[0] == '\0')
        usage(argv[0], "server_ip must not be empty");
    if (parse_port(argv[2], &e->port) != 0)
        usage(argv[0], "server_port must be an integer in 1..65535");
    e->topic = argv[3];
    if (!topic_is_valid(e->topic))
        usage(argv[0], "topic must be 1..63 bytes, without '/' or control characters");
    out_root = argv[4];
    /* "" would turn output_dir/topic into /topic at the file-system root. */
    if (out_root[0] == '\0')
        usage(argv[0], "output_dir must not be empty");
    if (path_join(e->dir, sizeof(e->dir), out_root, e->topic) != 0)
        usage(argv[0], "output_dir is too long");
    if (argc >= 6 && parse_long(argv[5], 1, FDS_ENH_MAX_K, &k) != 0)
        usage(argv[0], "connections must be an integer in 1..16");
    if (argc >= 7 && parse_long(argv[6], 1, MAX_RETRIES_LIMIT, &retries) != 0)
        usage(argv[0], "max_retries must be an integer in 1..1000000");
    if (argc >= 8) {
        if (strcmp(argv[7], "threads") == 0)
            e->use_poll = 0;
        else if (strcmp(argv[7], "poll") == 0)
            e->use_poll = 1;
        else
            usage(argv[0], "io must be 'threads' or 'poll'");
    }
    e->k = (int)k;
    e->max_retries = (int)retries;
}

static const char *io_name(const enh_t *e)
{
    return e->use_poll ? "poll" : "threads";
}

/* Free what one pass of steps 1-3 allocated. */
static void free_pass(enh_t *e)
{
    workq_destroy(e->q);             /* NULL-safe; all threads are joined */
    e->q = NULL;
    free(e->done);
    e->done = NULL;
    manifest_free(&e->man);
}

/*
 * Steps 1-3, repeated after a RESUME_REJECT 1/2/3. A restart counts as an
 * attempt without progress (its partial data is thrown away), so at most
 * max_retries restarts happen. Returns the exit code; t->end_ns is set when
 * the engine returns (the last FILE_END has been stored by then).
 */
static int run(enh_t *e, run_times_t *t)
{
    int restarts = 0;

    for (;;) {
        long wait;
        int rc = enh_fetch_manifest(e);

        if (rc != FDS_EXIT_OK)
            return rc;
        if (enh_build_queue(e) != 0) {
            print_error("out of memory building the work queue");
            return FDS_EXIT_FATAL;
        }
        rc = e->use_poll ? enh_run_poll(e) : enh_run_threads(e);
        t->end_ns = now_ns();
        t->end_wall_ns = wall_ns();
        /* The engine has stopped every thread: the flags can be read freely. */
        if (e->fatal)
            return FDS_EXIT_FATAL;
        if (e->gave_up)
            return FDS_EXIT_GAVE_UP;
        if (!e->restart)
            return rc;

        restarts++;
        free_pass(e);
        e->files_done = 0;
        e->restart = 0;
        e->had_session = 0;
        /* Every .part and the .session describe the abandoned run. */
        if (remove_partials(e->dir) != 0) {
            print_error("cannot remove the partial files in %s: %s", e->dir, strerror(errno));
            return FDS_EXIT_FATAL;
        }
        if (restarts >= e->max_retries) {
            fprintf(stderr, "[client] giving up after %d restarts\n", restarts);
            return FDS_EXIT_GAVE_UP;
        }
        wait = backoff_ms(restarts);
        log_info("starting over from the manifest in %ld ms (restart %d/%d)", wait, restarts,
                 e->max_retries);
        sleep_ms(wait);
    }
}

/*
 * Success needs every file on disk with its manifest size: the count and the
 * byte total (files stored now plus files kept from an interrupted run) must
 * both match the MANIFEST.
 */
static int verify_topic(const enh_t *e)
{
    uint64_t sum = 0;
    uint32_t n = 0;

    for (uint32_t i = 0; i < e->man.file_count; i++) {
        const file_entry_t *f = &e->man.files[i];
        uint64_t sz = 0;

        if (!e->done[i]) {
            print_error("file #%u (%s) was not received", i, f->name);
            return -1;
        }
        if (final_size(e->dir, f->name, &sz) != 0 || sz != f->size) {
            print_error("%s/%s: expected %llu bytes on disk, found %llu", e->dir, f->name,
                        (unsigned long long)f->size, (unsigned long long)sz);
            return -1;
        }
        sum += sz;
        n++;
    }
    if (n != e->man.file_count || e->files_done != n || sum != e->man.total_bytes) {
        print_error("inconsistent transfer: MANIFEST %u files / %llu bytes, stored %u / %llu",
                    e->man.file_count, (unsigned long long)e->man.total_bytes, n,
                    (unsigned long long)sum);
        return -1;
    }
    return 0;
}

/* Verify, then remove .session so the folder holds only the topic files. */
static int finish_success(enh_t *e)
{
    if (verify_topic(e) != 0)
        return FDS_EXIT_FATAL;
    if (ckpt_clear(e->dir) != 0) {
        print_error("cannot remove %s/%s: %s", e->dir, CKPT_FILE, strerror(errno));
        return FDS_EXIT_FATAL;
    }
    return FDS_EXIT_OK;
}

/* The one stdout line of a successful run (Part I format + k and io). */
static void print_summary(const enh_t *e, const run_times_t *t)
{
    double secs = (double)(t->end_ns - t->start_ns) / 1e9;
    double mbps = secs > 0 ? (double)e->man.total_bytes / 1e6 / secs : 0.0;

    printf("%s: %u files, %llu bytes in %.3f s (%.2f MB/s), k=%d, io=%s\n", e->topic,
           e->man.file_count, (unsigned long long)e->man.total_bytes, secs, mbps, e->k,
           io_name(e));
    log_info("%u connection(s) opened (%u after a failure), %u FETCH(es) rejected, "
             "%llu data bytes received", e->attempts, e->reconnects, e->rejects,
             (unsigned long long)e->data_bytes);
}

/* Bytes of the topic complete on disk (for a failed run's CSV row). */
static uint64_t stored_bytes(const enh_t *e)
{
    uint64_t sum = 0;

    if (e->done == NULL || e->man.files == NULL)
        return 0;
    for (uint32_t i = 0; i < e->man.file_count; i++)
        if (e->done[i])
            sum += e->man.files[i].size;
    return sum;
}

/*
 * One CSV row (only if FDS_LOG is set). useful_bytes = the topic size on
 * success; data_bytes_rx - useful_bytes is then the redundant data (bytes
 * received again after a fault inside a chunk, or after a reject).
 */
static void write_csv(const enh_t *e, const run_times_t *t, int code)
{
    log_client_row_t row = {
        .topic = e->topic,
        .k = e->k,
        .io = io_name(e),
        .start_ns = t->start_ns,
        .end_ns = t->end_ns,
        .start_wall_ns = t->start_wall_ns,
        .end_wall_ns = t->end_wall_ns,
        .useful_bytes = code == FDS_EXIT_OK ? e->man.total_bytes : stored_bytes(e),
        .data_bytes_rx = e->data_bytes,
        .wire_bytes_rx = e->wire_bytes,
        .files = e->files_done,
        .attempts = e->attempts,
        .reconnects = e->reconnects,
        .rejects = e->rejects,
        .status = code == FDS_EXIT_OK ? "OK" : code == FDS_EXIT_GAVE_UP ? "DNF" : "FAIL",
    };

    log_client(&row);
}

int main(int argc, char **argv)
{
    enh_t e;
    run_times_t t;
    log_ctx_t lc;
    int code, rc;

    memset(&e, 0, sizeof(e));
    parse_args(argc, argv, &e);
    /* mkdir -p output_dir/topic before connecting: a bad path costs no transfer. */
    if (mkdir_p(e.dir, 0755) != 0) {
        print_error("cannot create directory %s: %s", e.dir, strerror(errno));
        return FDS_EXIT_FATAL;
    }
    lc = (log_ctx_t){ "client", "enhanced", io_name(&e), -1.0, 0, 0 };
    log_init(&lc);
    rc = pthread_mutex_init(&e.mu, NULL);
    if (rc != 0) {
        print_error("pthread_mutex_init: %s", strerror(rc));
        log_close();
        return FDS_EXIT_FATAL;
    }

    memset(&t, 0, sizeof(t));
    t.start_ns = now_ns();           /* just before the first connect() */
    t.start_wall_ns = wall_ns();
    code = run(&e, &t);
    if (t.end_ns == 0) {             /* failed before any engine ran */
        t.end_ns = now_ns();
        t.end_wall_ns = wall_ns();
    }
    if (code == FDS_EXIT_OK)
        code = finish_success(&e);
    if (code == FDS_EXIT_OK)
        print_summary(&e, &t);
    write_csv(&e, &t, code);

    free_pass(&e);
    pthread_mutex_destroy(&e.mu);
    log_close();
    return code;
}
