/*
 * client_poll.c - Part II Case 2 Enhanced, io=poll: the NON-BLOCKING client
 * engine. One thread drives all K worker connections with O_NONBLOCK sockets
 * and poll() (design sections 3.8, 6.1, 6.2 and 9; deviations 9, 10, 13, 18).
 *
 * IT305 Socket Programming Project, Group 006 + 043.
 *
 * The client flow and the failure rules are the contract in enh_client.h.
 * main(), the manifest, the work queue set-up and the threaded engine live in
 * client.c; main() calls enh_run_poll() once the queue is ready.
 *
 * Why a non-blocking engine
 *   The assignment lets Enhanced use blocking or non-blocking sockets ("the
 *   socket APIs return immediately even if they cannot complete right away").
 *   Our main engine is blocking: K threads, each asleep in recv() on its own
 *   socket. This engine is the measured alternative (design 6.1): ONE thread
 *   owns all K sockets, asks the kernel with poll() which of them can make
 *   progress right now, and services exactly those. There is no thread and no
 *   stack per connection, no context switch between workers and no lock
 *   contention. The mutexes of enh_t and of the work queue are still taken,
 *   because the client.c helpers take them, but nobody else ever waits on them.
 *
 * What EAGAIN means
 *   On an O_NONBLOCK socket, recv() with nothing queued and send() with a full
 *   send buffer do not sleep. They fail with EAGAIN (the same value as
 *   EWOULDBLOCK on Linux). That is not an error: it means "nothing can be done
 *   on this socket now, come back when poll() says it is ready". A
 *   non-blocking connect() answers EINPROGRESS in the same spirit: the socket
 *   becomes writable when the handshake is over, and SO_ERROR then tells
 *   whether it worked. This is also why sendall()/recvall() are not used here:
 *   they treat EAGAIN as a failure and would lose count of a partial transfer.
 *
 * Why poll() and not epoll
 *   poll() is handed the whole descriptor set on every call, so it costs O(n)
 *   per call. epoll keeps the set inside the kernel and reports only the ready
 *   descriptors, O(1) per event. That only pays off with hundreds or thousands
 *   of sockets. Here K <= FDS_ENH_MAX_K = 16, the pollfd array is rebuilt from
 *   the slots in a few nanoseconds, and poll() is plain POSIX (design 6.1).
 *
 * The price: explicit state
 *   A blocking worker keeps its progress implicitly, in its stack and program
 *   counter ("inside recvall() of a header, 7 of 16 bytes read"). Here any
 *   recv() can return any number of bytes, and the rest may arrive only after
 *   other sockets were serviced, so every partial read and write and every
 *   timer (connect time-out, inactivity time-out, back-off) has to be stored
 *   in a per-connection slot, and every transition is written out by hand.
 *   That is more code and more ways to get it wrong. It is the trade-off we
 *   measure against the simpler threaded engine.
 *
 * Slot state machine (one slot per connection, K slots, all in one thread)
 *
 *   IDLE ---pop, no socket---> CONNECTING      connect() said EINPROGRESS
 *    |                           | POLLOUT, SO_ERROR == 0
 *    +---pop, socket open----+   |
 *                            v   v
 *                           SENDING            FETCH; partial sends move req_off
 *                              | all sent
 *                              v
 *        +--------------->  RECV_HDR           16 bytes, then hdr_decode()
 *        |                     |
 *        |                     v
 *        |                RECV_PAYLOAD         payload_len bytes, then by type:
 *        |                     |-- FILE_HDR: open the .part --------> RECV_DATA
 *        |                     |-- FILE_END: close, rename, done ---> IDLE
 *        |                     |-- RESUME_REJECT 4/5: re-queue at 0 > IDLE
 *        |                     |-- RESUME_REJECT 1/2/3 -------------> restart
 *        |                     '-- ERROR 503 -> lost; 400/404/500 --> fatal
 *        |
 *        +-- remaining == 0 -- RECV_DATA       raw bytes -> .part
 *
 *   any busy state --lost (EOF, reset, time-out, 503)--> BACKOFF
 *   BACKOFF --deadline--> IDLE (no socket: connects when it next gets an item)
 *
 *   A slot pops an item only when it is IDLE, and holds it while CONNECTING,
 *   SENDING and RECV_*. On a failure the item goes back to the queue at once,
 *   BEFORE the back-off, so a healthy connection can take it over while this
 *   one waits. Idle slots with an open connection are preferred over idle
 *   slots that would have to connect first (one handshake saved). One FETCH
 *   is outstanding per connection, never pipelined (deviation 13).
 *
 *   (Re)connecting is lazy: a slot opens a connection only once it holds an
 *   item, and sends the FETCH as soon as the handshake is done. So no socket
 *   is opened that has nothing to fetch, and an ERROR 503 that the server
 *   sends straight after accept() is read as the reply to that FETCH. After a
 *   failure, if the healthy connections drain the queue first, this slot
 *   never reconnects, so `reconnects` counts only connections really opened
 *   again.
 *
 * Main loop
 *   1. expire timers: back-off over -> IDLE; connect time-out
 *      (FDS_CONNECT_TIMEOUT_S) or no I/O for FDS_CLI_TIMEOUT_S (the blocking
 *      engine's SO_RCVTIMEO/SO_SNDTIMEO) -> the connection is lost;
 *   2. give items to IDLE slots with workq_trypop() (never the blocking pop:
 *      this thread owns every socket, so it must never sleep on the queue);
 *   3. poll() on the sockets that wait for something, with the time-out set to
 *      the nearest deadline of step 1;
 *   4. service the ready sockets. On POLLIN a slot reads until EAGAIN, but at
 *      most RECV_BUDGET bytes per wake-up: poll() is level-triggered, so data
 *      left behind is reported again at once, and one fast connection cannot
 *      starve the others.
 *   The loop ends when workq_outstanding() == 0 (topic complete) or when
 *   fatal / gave_up / restart is set.
 *
 * Storage: byte-identical to recv_file() in RX_PART mode (transfer.h), because
 * the size of <name>.part IS the checkpoint (resume offset). Every received
 * byte is written before the next recv(). Disk writes are ordinary blocking
 * write()s: they go to the page cache and return in microseconds, and poll()
 * cannot wait on regular files anyway (they always count as ready).
 *
 * Failures follow step 4 of enh_client.h. "Lost" (EOF, reset, time-out,
 * ERROR 503): close the socket and the .part, count the bytes, re-queue
 * (file, current .part size), streak = 0 if this attempt stored data else
 * streak + 1 (deviation 18), give up at max_retries, otherwise back off
 * backoff_ms(max(streak, 1)) while the other slots carry on. RESUME_REJECT
 * 4/5: delete the .part and re-queue from 0 on the same connection. Reject
 * 1/2/3 (or an unknown reason): e->restart, main() starts over. ERROR
 * 404/400/500, protocol and local I/O errors: e->fatal.
 */
#include "enh_client.h"
#include "checkpoint.h"
#include "fds.h"
#include "log.h"
#include "net.h"
#include "protocol.h"
#include "util.h"
#include "workq.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* Largest FETCH: 16 header + 16 id + 4 index + 8 offset + 1 + 63 topic
 * + 2 + 255 name = 365 bytes. */
#define REQ_BUF        512

/* Fairness cap: bytes read from one socket per poll() wake-up. */
#define RECV_BUDGET    (1024u * 1024u)

#define NS_PER_MS      1000000LL
#define NS_PER_S       1000000000LL

_Static_assert(sizeof(((m_resume_t *)0)->name) == sizeof(((file_entry_t *)0)->name),
               "FETCH and manifest name fields must have the same size");

typedef enum {
    S_IDLE,          /* no item; fd >= 0: connection kept open for the next FETCH */
    S_BACKOFF,       /* no item, no socket: waiting until `deadline` */
    S_CONNECTING,    /* non-blocking connect() in progress; item held */
    S_SENDING,       /* FETCH partly sent; item held */
    S_RECV_HDR,      /* reading a 16-byte header; item held */
    S_RECV_PAYLOAD,  /* reading the payload of that header; item held */
    S_RECV_DATA      /* reading the raw bytes of the file; item held */
} slot_state_t;

/* Everything one connection needs to resume where the last recv()/send()
 * left it: the explicit state that a blocking thread keeps on its stack. */
typedef struct {
    int          id;            /* 1..K, for messages */
    slot_state_t st;
    int          fd;            /* socket, or -1 */
    int          reconnect;     /* the previous connection of this slot failed */
    int          streak;        /* consecutive attempts without progress */

    int          have_item;     /* popped and not yet workq_done() */
    work_item_t  item;
    uint64_t     item_data;     /* file bytes stored for `item` on this attempt */

    int64_t      deadline;      /* BACKOFF: wake-up; CONNECTING: give up (now_ns) */
    int64_t      last_io;       /* SENDING / RECV_*: last byte moved (now_ns) */

    uint8_t      req[REQ_BUF];  /* encoded FETCH */
    size_t       req_len;
    size_t       req_off;       /* bytes of req already accepted by send() */

    uint8_t      hdr[FDS_HDR_LEN];
    size_t       hdr_got;
    msg_hdr_t    h;             /* decoded header of the message being read */
    size_t       pay_got;
    int          expect_end;    /* 0: reply to FETCH (FILE_HDR); 1: after the data (FILE_END) */

    int          file_fd;       /* open <name>.part, or -1 */
    uint64_t     remaining;     /* RECV_DATA: raw bytes still to come */

    uint64_t     pend_data;     /* received, not yet added to enh_t (enh_add_bytes) */
    uint64_t     pend_wire;

    /* Room for the largest payload hdr_decode() accepts for the types we
     * read (FDS_MAX_CTRL_PAYLOAD), so no message needs a malloc mid-run. */
    uint8_t      pay[FDS_MAX_CTRL_PAYLOAD];
} slot_t;

typedef struct {
    enh_t             *e;
    int                k;
    slot_t            *slots;
    uint8_t           *buf;     /* FDS_IO_BUF data buffer shared by all slots: data
                                   is on disk before the next recv(), so nothing
                                   is kept in it between two calls */
    struct sockaddr_in addr;    /* server, resolved once */
    int                stop;    /* fatal / gave_up / restart raised: leave the loop */
} engine_t;

/* ---- Shared flags and counters --------------------------------------------- */

/* Set one of e->fatal / gave_up / restart under mu and stop the loop. */
static void raise_flag(engine_t *g, int *flag)
{
    pthread_mutex_lock(&g->e->mu);
    *flag = 1;
    pthread_mutex_unlock(&g->e->mu);
    g->stop = 1;
}

/* Protocol error, local I/O error or server ERROR 400/404/500: exit 1. */
static void fail_fatal(engine_t *g, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void fail_fatal(engine_t *g, const char *fmt, ...)
{
    char msg[768];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    log_warn("%s", msg);
    raise_flag(g, &g->e->fatal);
}

/* Add the slot's pending byte counts to the run totals. Done once per item
 * (and on exit) rather than per recv(), to take e->mu less often. */
static void flush_bytes(engine_t *g, slot_t *s)
{
    if (s->pend_data == 0 && s->pend_wire == 0)
        return;
    enh_add_bytes(g->e, s->pend_data, s->pend_wire);
    s->pend_data = 0;
    s->pend_wire = 0;
}

/* ---- Small helpers ------------------------------------------------------------ */

static int is_receiving(slot_state_t st)
{
    return st == S_RECV_HDR || st == S_RECV_PAYLOAD || st == S_RECV_DATA;
}

/* "Not now, retry when poll() says ready". POSIX allows two different values;
 * on Linux they are the same number. */
static int would_block(int err)
{
#if EAGAIN == EWOULDBLOCK
    return err == EAGAIN;
#else
    return err == EAGAIN || err == EWOULDBLOCK;
#endif
}

static void close_socket(slot_t *s)
{
    if (s->fd < 0)
        return;
    /* On Linux the descriptor is released even if close() reports EINTR. */
    if (close(s->fd) != 0 && errno != EINTR)
        log_warn("[poll slot %d] close socket: %s", s->id, strerror(errno));
    s->fd = -1;
}

/* Close an unfinished .part (failure or exit). What is on disk stays: its size
 * is the checkpoint the next FETCH resumes from. */
static void drop_part_fd(slot_t *s)
{
    if (s->file_fd < 0)
        return;
    if (close(s->file_fd) != 0)
        log_warn("[poll slot %d] close partial file: %s", s->id, strerror(errno));
    s->file_fd = -1;
}

/* write() all n bytes: a regular file may take fewer per call. */
static int write_all(int fd, const uint8_t *p, size_t n)
{
    while (n > 0) {
        ssize_t w = write(fd, p, n);

        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0) {
            if (w == 0)
                errno = EIO;    /* no progress: report it instead of spinning */
            return -1;
        }
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

/* dir/<name>.part and dir/<name> of manifest file idx. */
static int item_paths(engine_t *g, uint32_t idx, char *part, char *final)
{
    const char *name = g->e->man.files[idx].name;

    if (path_join3(part, PATH_MAX, g->e->dir, name, PART_SUFFIX) != 0 ||
        path_join3(final, PATH_MAX, g->e->dir, name, "") != 0) {
        fail_fatal(g, "output path for '%s' is too long", name);
        return -1;
    }
    return 0;
}

/* Hand the held item back to the queue at offset off. push before done, so the
 * queue never looks complete (count + busy == 0) while the file is missing. */
static void requeue_item(engine_t *g, slot_t *s, uint64_t off)
{
    if (!s->have_item)
        return;
    if (workq_push(g->e->q, s->item.idx, off) != 0)
        fail_fatal(g, "internal error: work queue full when re-queueing file %" PRIu32,
                   s->item.idx);
    workq_done(g->e->q);
    s->have_item = 0;
    s->item_data = 0;
}

/* ---- Failure of one connection (enh_client.h step 4) -------------------------- */

/*
 * The connection is lost (EOF, reset, time-out, connect failure, ERROR 503).
 * Only this slot backs off; the other K-1 connections keep going.
 */
static void slot_lost(engine_t *g, slot_t *s, const char *why)
{
    enh_t   *e = g->e;
    int      had_item = s->have_item;
    uint32_t idx = s->item.idx;
    int      progress = s->item_data > 0;
    uint64_t off = 0;
    long     wait_ms;

    close_socket(s);
    drop_part_fd(s);
    flush_bytes(g, s);
    if (had_item) {
        /* Every byte that arrived is already in the .part, so its size is
         * exactly where the next FETCH of this file must start. */
        off = enh_part_offset(e, idx);
        requeue_item(g, s, off);
    }
    s->reconnect = 1;
    /* Deviation 18: the streak counts consecutive attempts that stored no
     * file data. A cut after some data was stored is followed by 50 ms only. */
    s->streak = progress ? 0 : s->streak + 1;
    if (s->streak >= e->max_retries) {
        log_warn("[poll slot %d] %s; giving up after %d attempts without progress", s->id, why,
                 s->streak);
        s->st = S_IDLE;
        raise_flag(g, &e->gave_up);
        return;
    }
    wait_ms = backoff_ms(s->streak > 0 ? s->streak : 1);
    s->st = S_BACKOFF;
    s->deadline = now_ns() + (int64_t)wait_ms * NS_PER_MS;
    if (had_item)
        log_info("[poll slot %d] lost at file %" PRIu32 " offset %" PRIu64
                 " (%s); re-queued, reconnecting in %ld ms", s->id, idx, off, why, wait_ms);
    else
        log_info("[poll slot %d] %s; reconnecting in %ld ms", s->id, why, wait_ms);
}

/* slot_lost() with "what: strerror(errno)". */
static void lost_errno(engine_t *g, slot_t *s, const char *what)
{
    char why[160];

    snprintf(why, sizeof(why), "%s: %s", what, strerror(errno));
    slot_lost(g, s, why);
}

/* ---- Sending the FETCH --------------------------------------------------------- */

static void begin_header(slot_t *s, int expect_end)
{
    s->st = S_RECV_HDR;
    s->hdr_got = 0;
    s->expect_end = expect_end;
}

/*
 * Push the rest of the FETCH into the socket. A partial send() just moves
 * req_off forward; EAGAIN means the send buffer is full and poll() will
 * report POLLOUT when there is room again. Once the whole request is out, the
 * reply is awaited (one FETCH outstanding per connection: deviation 13).
 */
static void do_send(engine_t *g, slot_t *s)
{
    while (s->req_off < s->req_len) {
        /* MSG_NOSIGNAL: a dead server gives EPIPE, not a fatal SIGPIPE. */
        ssize_t w = send(s->fd, s->req + s->req_off, s->req_len - s->req_off, MSG_NOSIGNAL);

        if (w < 0) {
            if (errno == EINTR)
                continue;
            if (would_block(errno))
                return;
            lost_errno(g, s, "send");
            return;
        }
        if (w == 0) {
            errno = EPIPE;      /* cannot happen for a non-empty buffer */
            lost_errno(g, s, "send");
            return;
        }
        s->req_off += (size_t)w;
        s->last_io = now_ns();
    }
    begin_header(s, 0);
}

/* Build FETCH(session id, file, offset, topic, name) for the held item. */
static void start_fetch(engine_t *g, slot_t *s)
{
    enh_t      *e = g->e;
    m_resume_t  m;

    memset(&m, 0, sizeof(m));
    memcpy(m.session_id, e->session_id, sizeof(m.session_id));
    m.file_index = s->item.idx;
    m.offset = s->item.off;
    memcpy(m.topic, e->topic, strlen(e->topic) + 1);   /* length checked at start */
    memcpy(m.name, e->man.files[s->item.idx].name, sizeof(m.name));
    s->req_len = enc_resume(s->req, sizeof(s->req), MSG_FETCH, &m);
    if (s->req_len == 0) {
        fail_fatal(g, "internal error: cannot encode FETCH for file %" PRIu32, s->item.idx);
        return;
    }
    s->req_off = 0;
    s->item_data = 0;
    s->st = S_SENDING;
    s->last_io = now_ns();
    /* The send buffer of an idle socket is empty, so this almost always goes
     * out at once, without waiting for a POLLOUT round. */
    do_send(g, s);
}

/* ---- Connecting -------------------------------------------------------------- */

/* The handshake is complete: set the socket up like enh_connect() does and
 * count the connection, then send the FETCH of the held item. */
static void on_connected(engine_t *g, slot_t *s)
{
    enh_t *e = g->e;

    /* Deviation 10: Nagle + delayed ACK would stall each small FETCH. */
    if (set_nodelay(s->fd) != 0)
        log_warn("[poll slot %d] cannot set TCP_NODELAY: %s", s->id, strerror(errno));
    apply_env_bufsizes(s->fd);
    pthread_mutex_lock(&e->mu);
    e->attempts++;
    if (s->reconnect)
        e->reconnects++;
    pthread_mutex_unlock(&e->mu);
    s->reconnect = 0;
    start_fetch(g, s);
}

/*
 * Start a non-blocking connect(). It normally answers EINPROGRESS: the SYN is
 * out, and poll() reports POLLOUT when the handshake has finished one way or
 * the other. EINTR means the same for a connect() (POSIX: it then completes
 * asynchronously). It may also succeed at once, or fail at once (e.g.
 * ECONNREFUSED on the loopback interface).
 */
static void start_connect(engine_t *g, slot_t *s)
{
    /* SOCK_NONBLOCK sets O_NONBLOCK atomically with the socket; SOCK_CLOEXEC
     * keeps it out of any child process. */
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

    if (fd < 0) {
        lost_errno(g, s, "socket");
        return;
    }
    s->fd = fd;
    if (connect(fd, (const struct sockaddr *)&g->addr, sizeof(g->addr)) == 0) {
        on_connected(g, s);
        return;
    }
    if (errno == EINPROGRESS || errno == EINTR) {
        s->st = S_CONNECTING;
        s->deadline = now_ns() + (int64_t)FDS_CONNECT_TIMEOUT_S * NS_PER_S;
        return;
    }
    lost_errno(g, s, "connect");
}

/* POLLOUT (or POLLERR/POLLHUP) on a connecting socket: the attempt is over.
 * Writability alone does not say how it ended; SO_ERROR does. */
static void finish_connect(engine_t *g, slot_t *s)
{
    int       err = 0;
    socklen_t len = sizeof(err);

    if (getsockopt(s->fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0) {
        lost_errno(g, s, "getsockopt(SO_ERROR)");
        return;
    }
    if (err != 0) {
        errno = err;
        lost_errno(g, s, "connect");
        return;
    }
    on_connected(g, s);
}

/* ---- Storing a file (same semantics as recv_file() in RX_PART mode) ------------ */

/*
 * Open the output for FILE_HDR fh (already checked against the item):
 *   offset 0  -> create/truncate <name>.part;
 *   offset >0 -> the existing <name>.part, cut back to exactly offset bytes
 *                (it can only be longer if bytes were written after the
 *                checkpoint was taken; the server sends them again);
 *   no .part, offset == size and <name> complete with that size -> nothing
 *                to write, only FILE_END follows (file_fd stays -1);
 *   anything else is inconsistent: local I/O error (fatal).
 */
static int open_output(engine_t *g, slot_t *s, const m_file_hdr_t *fh)
{
    char        part[PATH_MAX], final[PATH_MAX];
    struct stat st;
    int         fd;

    if (item_paths(g, s->item.idx, part, final) != 0)
        return -1;
    if (fh->start_offset == 0) {
        fd = open(part, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd < 0) {
            fail_fatal(g, "cannot create %s: %s", part, strerror(errno));
            return -1;
        }
        s->file_fd = fd;
        return 0;
    }

    fd = open(part, O_WRONLY | O_CLOEXEC);
    if (fd >= 0) {
        /* fstat on the opened file: no window between the check and the use. */
        if (fstat(fd, &st) != 0) {
            fail_fatal(g, "cannot stat %s: %s", part, strerror(errno));
            goto close_fail;
        }
        /* The offset came from this very file's size: a shorter file means
         * something else changed it. Never invent the missing bytes. */
        if ((uint64_t)st.st_size < fh->start_offset) {
            fail_fatal(g, "%s has %lld bytes but the transfer resumes at %" PRIu64, part,
                       (long long)st.st_size, fh->start_offset);
            goto close_fail;
        }
        if (ftruncate(fd, (off_t)fh->start_offset) != 0 ||
            lseek(fd, (off_t)fh->start_offset, SEEK_SET) == (off_t)-1) {
            fail_fatal(g, "cannot resize %s: %s", part, strerror(errno));
            goto close_fail;
        }
        s->file_fd = fd;
        return 0;
    }
    if (errno != ENOENT) {
        fail_fatal(g, "cannot open %s: %s", part, strerror(errno));
        return -1;
    }
    /* No .part: finished and renamed earlier, but FILE_END never arrived.
     * Only FILE_END is left to read. */
    if (fh->start_offset == fh->file_size && stat(final, &st) == 0 && S_ISREG(st.st_mode) &&
        (uint64_t)st.st_size == fh->file_size) {
        s->file_fd = -1;
        return 0;
    }
    fail_fatal(g, "cannot resume %s at offset %" PRIu64 ": no partial file", final,
               fh->start_offset);
    return -1;

close_fail:
    if (close(fd) != 0)
        log_warn("close %s: %s", part, strerror(errno));
    return -1;
}

/* Raw file bytes: written before the next recv(), so the .part size is
 * always exactly the number of bytes received (the checkpoint). */
static void store_data(engine_t *g, slot_t *s, const uint8_t *p, size_t n)
{
    if (s->file_fd < 0) {
        fail_fatal(g, "internal error: [poll slot %d] data without an open file", s->id);
        return;
    }
    if (write_all(s->file_fd, p, n) != 0) {
        fail_fatal(g, "write %s%s: %s", g->e->man.files[s->item.idx].name, PART_SUFFIX,
                   strerror(errno));
        return;
    }
    s->pend_data += n;
    s->item_data += n;
    s->remaining -= n;
    if (s->remaining == 0)
        begin_header(s, 1);
}

/* ---- Handling a complete message ------------------------------------------------ */

static void on_file_hdr(engine_t *g, slot_t *s, size_t len)
{
    const file_entry_t *f = &g->e->man.files[s->item.idx];
    m_file_hdr_t        fh;

    /* dec_file_hdr also checks name_is_safe() and start_offset <= file_size. */
    if (dec_file_hdr(s->pay, len, &fh) != 0) {
        fail_fatal(g, "[poll slot %d] protocol error: malformed FILE_HDR", s->id);
        return;
    }
    /* The reply must be for exactly what was asked, and agree with the
     * MANIFEST; otherwise bytes would land in the wrong place. */
    if (fh.file_index != s->item.idx || fh.start_offset != s->item.off ||
        fh.file_size != f->size || strcmp(fh.name, f->name) != 0) {
        fail_fatal(g, "[poll slot %d] protocol error: FILE_HDR #%" PRIu32 " '%s' size %" PRIu64
                   " offset %" PRIu64 " does not answer FETCH #%" PRIu32 " '%s' size %" PRIu64
                   " offset %" PRIu64, s->id, fh.file_index, fh.name, fh.file_size,
                   fh.start_offset, s->item.idx, f->name, f->size, s->item.off);
        return;
    }
    if (open_output(g, s, &fh) != 0)
        return;
    s->remaining = fh.file_size - fh.start_offset;
    if (s->remaining == 0)
        begin_header(s, 1);     /* empty file, or nothing left: FILE_END is next */
    else
        s->st = S_RECV_DATA;
}

/* FILE_END: the file is complete. Close, give it its final name, count it,
 * and the slot is free for the next item on the same connection. */
static void on_file_end(engine_t *g, slot_t *s, size_t len)
{
    enh_t       *e = g->e;
    m_file_end_t fe;

    if (dec_file_end(s->pay, len, &fe) != 0 || fe.file_index != s->item.idx) {
        fail_fatal(g, "[poll slot %d] protocol error: bad FILE_END for file %" PRIu32, s->id,
                   s->item.idx);
        return;
    }
    if (s->file_fd >= 0) {
        char part[PATH_MAX], final[PATH_MAX];
        int  fd = s->file_fd;

        s->file_fd = -1;
        if (item_paths(g, s->item.idx, part, final) != 0) {
            if (close(fd) != 0)
                log_warn("close %s: %s", e->man.files[s->item.idx].name, strerror(errno));
            return;
        }
        /* close() is where some file systems report a failed write-back
         * (NFS, quota): the file would be incomplete. */
        if (close(fd) != 0) {
            fail_fatal(g, "close %s: %s", part, strerror(errno));
            return;
        }
        /* Atomic: a file under its final name is always complete. */
        if (rename(part, final) != 0) {
            fail_fatal(g, "rename %s: %s", part, strerror(errno));
            return;
        }
    }
    enh_file_completed(e, s->item.idx);
    flush_bytes(g, s);
    workq_done(e->q);
    s->have_item = 0;
    s->item_data = 0;
    s->streak = 0;              /* a completed file is progress */
    s->st = S_IDLE;             /* connection stays open for the next FETCH */
}

static void on_reject(engine_t *g, slot_t *s, size_t len)
{
    enh_t   *e = g->e;
    uint16_t reason;

    if (dec_resume_reject(s->pay, len, &reason) != 0) {
        fail_fatal(g, "[poll slot %d] protocol error: malformed RESUME_REJECT", s->id);
        return;
    }
    pthread_mutex_lock(&e->mu);
    e->rejects++;
    pthread_mutex_unlock(&e->mu);

    if (reason == FDS_REJ_OFFSET_TOO_BIG || reason == FDS_REJ_AHEAD_OF_SENT) {
        /* Only this file's checkpoint is wrong: drop it and fetch the file
         * from 0. The server keeps the connection open for that FETCH. */
        char part[PATH_MAX], final[PATH_MAX];

        log_info("[poll slot %d] file %" PRIu32 " offset %" PRIu64
                 " rejected (%s); fetching it again from 0", s->id, s->item.idx, s->item.off,
                 reject_reason_str(reason));
        drop_part_fd(s);
        if (item_paths(g, s->item.idx, part, final) != 0)
            return;
        /* A failed unlink is harmless: offset 0 opens the .part with O_TRUNC. */
        if (unlink(part) != 0 && errno != ENOENT)
            log_warn("cannot remove %s: %s", part, strerror(errno));
        flush_bytes(g, s);
        requeue_item(g, s, 0);
        s->st = S_IDLE;
        return;
    }
    /* 1/2/3 (or a reason we do not know): the topic on the server is not the
     * one in our MANIFEST. main() removes the partials and starts over. */
    log_info("[poll slot %d] server rejected file %" PRIu32 " (%s); starting over", s->id,
             s->item.idx, reject_reason_str(reason));
    raise_flag(g, &e->restart);
}

static void on_error(engine_t *g, slot_t *s, size_t len)
{
    m_error_t er;
    char      why[sizeof(er.msg) + 32];

    if (dec_error(s->pay, len, &er) != 0) {
        fail_fatal(g, "[poll slot %d] protocol error: malformed ERROR", s->id);
        return;
    }
    if (er.code == FDS_ERR_BUSY) {
        /* Thread or session limit on the server: retry like a lost connection. */
        snprintf(why, sizeof(why), "server busy (503 %s)", er.msg);
        slot_lost(g, s, why);
        return;
    }
    fail_fatal(g, "server error %u: %s", (unsigned)er.code, er.msg);
}

static void on_payload(engine_t *g, slot_t *s)
{
    size_t len = (size_t)s->h.payload_len;

    switch (s->h.type) {
    case MSG_FILE_HDR:
        on_file_hdr(g, s, len);
        break;
    case MSG_FILE_END:
        on_file_end(g, s, len);
        break;
    case MSG_RESUME_REJECT:
        on_reject(g, s, len);
        break;
    case MSG_ERROR:
        on_error(g, s, len);
        break;
    default:                    /* on_header() let no other type through */
        fail_fatal(g, "internal error: [poll slot %d] payload of type %u", s->id,
                   (unsigned)s->h.type);
        break;
    }
}

/* A complete 16-byte header: validate it and decide what may follow. */
static void on_header(engine_t *g, slot_t *s)
{
    int ok;

    if (hdr_decode(s->hdr, &s->h) != 0) {
        fail_fatal(g, "[poll slot %d] protocol error: invalid message header", s->id);
        return;
    }
    switch (s->h.type) {
    case MSG_FILE_HDR:
        ok = !s->expect_end;
        break;
    case MSG_FILE_END:
        ok = s->expect_end;
        break;
    case MSG_RESUME_REJECT:     /* like recv_expect(): accepted wherever a reply */
    case MSG_ERROR:             /* is expected */
        ok = 1;
        break;
    default:
        ok = 0;
        break;
    }
    if (!ok) {
        fail_fatal(g, "[poll slot %d] protocol error: unexpected %s (expected %s)", s->id,
                   msg_type_name(s->h.type), s->expect_end ? "FILE_END" : "FILE_HDR");
        return;
    }
    if (s->h.payload_len > sizeof(s->pay)) {    /* hdr_decode already capped it */
        fail_fatal(g, "[poll slot %d] protocol error: %s payload too long", s->id,
                   msg_type_name(s->h.type));
        return;
    }
    s->pay_got = 0;
    s->st = S_RECV_PAYLOAD;
    if (s->h.payload_len == 0)
        on_payload(g, s);       /* nothing to read; the decoder judges it */
}

/* ---- Receiving -------------------------------------------------------------------- */

/*
 * POLLIN (or POLLERR/POLLHUP) on a busy socket. Each recv() asks for exactly
 * what the current state still needs (the rest of the header, of the payload,
 * or of the file), so it never reads into the next message. That is the same
 * pattern of reads as the blocking recv_msg()/recv_file(), which keeps the
 * comparison between the two engines about the I/O model only.
 */
static void do_recv(engine_t *g, slot_t *s, short revents)
{
    size_t budget = RECV_BUDGET;
    int    moved = 0;

    while (!g->stop && budget > 0 && is_receiving(s->st)) {
        uint8_t *dst;
        size_t   want;
        ssize_t  r;

        switch (s->st) {
        case S_RECV_HDR:
            dst = s->hdr + s->hdr_got;
            want = FDS_HDR_LEN - s->hdr_got;
            break;
        case S_RECV_PAYLOAD:
            dst = s->pay + s->pay_got;
            want = (size_t)s->h.payload_len - s->pay_got;
            break;
        default:                /* S_RECV_DATA */
            dst = g->buf;
            want = (size_t)FDS_MIN(FDS_MIN((uint64_t)FDS_IO_BUF, s->remaining),
                                   (uint64_t)budget);
            break;
        }
        r = recv(s->fd, dst, want, 0);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            if (would_block(errno)) {
                /* Drained: wait for the next POLLIN. But an error/hang-up
                 * report with nothing to read would make poll() return at
                 * once forever, so treat it as a lost connection. */
                if (!moved && (revents & (POLLERR | POLLHUP))) {
                    errno = ECONNRESET;
                    lost_errno(g, s, "recv");
                }
                return;
            }
            lost_errno(g, s, "recv");   /* ECONNRESET, ... */
            return;
        }
        if (r == 0) {
            /* EOF inside a reply: the server cut the connection (an injected
             * fault: shutdown + close, deviation 9). */
            slot_lost(g, s, "connection closed by the server");
            return;
        }
        moved = 1;
        s->pend_wire += (uint64_t)r;
        s->last_io = now_ns();
        budget -= FDS_MIN((size_t)r, budget);
        switch (s->st) {
        case S_RECV_HDR:
            s->hdr_got += (size_t)r;
            if (s->hdr_got == FDS_HDR_LEN)
                on_header(g, s);
            break;
        case S_RECV_PAYLOAD:
            s->pay_got += (size_t)r;
            if (s->pay_got == s->h.payload_len)
                on_payload(g, s);
            break;
        default:
            store_data(g, s, dst, (size_t)r);
            break;
        }
    }
}

/*
 * Readable while IDLE: no request is outstanding, so this can only be the
 * server closing the connection (its idle time-out, or a graceful shutdown).
 * No item was in flight, so it is not a failed attempt: close and connect
 * again when there is work. Data without a request is a protocol error.
 */
static void idle_readable(engine_t *g, slot_t *s, short revents)
{
    uint8_t b;
    ssize_t r;

    do {
        r = recv(s->fd, &b, 1, 0);
    } while (r < 0 && errno == EINTR);
    if (r > 0) {
        s->pend_wire += (uint64_t)r;
        fail_fatal(g, "[poll slot %d] protocol error: data from the server without a request",
                   s->id);
        return;
    }
    if (r < 0 && would_block(errno) && !(revents & (POLLERR | POLLHUP)))
        return;                 /* spurious wake-up */
    log_info("[poll slot %d] idle connection closed by the server", s->id);
    close_socket(s);
    s->reconnect = 1;
}

/* ---- The loop ------------------------------------------------------------------- */

/* Step 1: back-off over, connect time-out, inactivity time-out. */
static void expire_timers(engine_t *g, int64_t now)
{
    char why[64];

    for (int i = 0; i < g->k && !g->stop; i++) {
        slot_t *s = &g->slots[i];

        switch (s->st) {
        case S_BACKOFF:
            if (now >= s->deadline)
                s->st = S_IDLE;
            break;
        case S_CONNECTING:
            if (now >= s->deadline) {
                errno = ETIMEDOUT;
                lost_errno(g, s, "connect");
            }
            break;
        case S_SENDING:
        case S_RECV_HDR:
        case S_RECV_PAYLOAD:
        case S_RECV_DATA:
            /* The blocking engine's SO_RCVTIMEO / SO_SNDTIMEO. */
            if (now - s->last_io >= (int64_t)FDS_CLI_TIMEOUT_S * NS_PER_S) {
                snprintf(why, sizeof(why), "timed out (%d s without progress)",
                         FDS_CLI_TIMEOUT_S);
                slot_lost(g, s, why);
            }
            break;
        case S_IDLE:
            break;
        }
    }
}

/*
 * Step 2: give queued items to IDLE slots. First to slots whose connection is
 * still open (no handshake needed), then to slots that must connect.
 * workq_trypop() counts the slot as busy in the queue until workq_done().
 */
static void assign_work(engine_t *g)
{
    for (int pass = 0; pass < 2; pass++) {
        for (int i = 0; i < g->k && !g->stop; i++) {
            slot_t *s = &g->slots[i];

            if (s->st != S_IDLE || (pass == 0) != (s->fd >= 0))
                continue;
            if (!workq_trypop(g->e->q, &s->item))
                return;         /* empty now; failures will refill it */
            s->have_item = 1;
            s->item_data = 0;
            if (s->item.idx >= g->e->man.file_count) {
                fail_fatal(g, "internal error: queued file index %" PRIu32 " out of range",
                           s->item.idx);
                return;
            }
            if (s->fd >= 0)
                start_fetch(g, s);
            else
                start_connect(g, s);
        }
    }
}

/* Step 3: what each slot waits for, and the nearest deadline as the poll()
 * time-out (-1 = none). Returns the number of pollfd entries. */
static int build_pollset(engine_t *g, struct pollfd *pfd, int *who, int64_t now,
                         int *timeout_ms)
{
    const int64_t io_limit = (int64_t)FDS_CLI_TIMEOUT_S * NS_PER_S;
    int64_t       next = INT64_MAX;
    int           n = 0;

    for (int i = 0; i < g->k; i++) {
        slot_t *s = &g->slots[i];
        short   ev = 0;
        int64_t d = INT64_MAX;

        switch (s->st) {
        case S_IDLE:
            if (s->fd >= 0)
                ev = POLLIN;    /* only to notice the server closing it */
            break;
        case S_BACKOFF:
            d = s->deadline;
            break;
        case S_CONNECTING:
            ev = POLLOUT;
            d = s->deadline;
            break;
        case S_SENDING:
            ev = POLLOUT;
            d = s->last_io + io_limit;
            break;
        case S_RECV_HDR:
        case S_RECV_PAYLOAD:
        case S_RECV_DATA:
            ev = POLLIN;
            d = s->last_io + io_limit;
            break;
        }
        if (ev != 0) {
            pfd[n].fd = s->fd;
            pfd[n].events = ev;
            pfd[n].revents = 0;
            who[n] = i;
            n++;
        }
        if (d < next)
            next = d;
    }
    if (next == INT64_MAX) {
        *timeout_ms = -1;
    } else if (next <= now) {
        *timeout_ms = 0;
    } else {
        /* Round up: rounding down would wake up just before the deadline
         * and spin with poll(0) for the last fraction of a millisecond. */
        int64_t ms = (next - now + NS_PER_MS - 1) / NS_PER_MS;

        *timeout_ms = ms > INT_MAX ? INT_MAX : (int)ms;
    }
    return n;
}

/* Step 4: one ready socket. */
static void service_slot(engine_t *g, slot_t *s, short revents)
{
    if (revents & POLLNVAL) {
        fail_fatal(g, "internal error: [poll slot %d] descriptor not open", s->id);
        return;
    }
    switch (s->st) {
    case S_CONNECTING:
        finish_connect(g, s);
        break;
    case S_SENDING:
        do_send(g, s);          /* a reset socket makes send() fail: handled there */
        break;
    case S_RECV_HDR:
    case S_RECV_PAYLOAD:
    case S_RECV_DATA:
        do_recv(g, s, revents);
        break;
    case S_IDLE:
        if (s->fd >= 0)
            idle_readable(g, s, revents);
        break;
    case S_BACKOFF:
        break;
    }
}

/* Resolve the server once (getaddrinfo, IPv4, like tcp_connect()). A name
 * with several addresses uses the first; the experiments use dotted IPs. */
static int resolve_server(engine_t *g)
{
    struct addrinfo hints, *res = NULL;
    char            port[16];
    int             rc;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICSERV;    /* never look the port up in /etc/services */
    snprintf(port, sizeof(port), "%u", (unsigned)g->e->port);
    rc = getaddrinfo(g->e->host, port, &hints, &res);
    if (rc != 0) {
        log_warn("cannot resolve host '%s': %s", g->e->host,
                 rc == EAI_SYSTEM ? strerror(errno) : gai_strerror(rc));
        return -1;
    }
    if (res == NULL || res->ai_family != AF_INET || res->ai_addrlen != sizeof(g->addr)) {
        log_warn("cannot resolve host '%s': no IPv4 address", g->e->host);
        if (res != NULL)
            freeaddrinfo(res);
        return -1;
    }
    memcpy(&g->addr, res->ai_addr, sizeof(g->addr));
    freeaddrinfo(res);
    return 0;
}

/*
 * Leave nothing behind: close every socket (an idle one gets a normal FIN,
 * which the server treats as the end of the client's work) and every .part
 * descriptor (the .part files stay on disk as checkpoints), add the pending
 * byte counts, and give back any item still held so that count + busy in the
 * queue stay consistent for main().
 */
static void cleanup(engine_t *g)
{
    for (int i = 0; i < g->k; i++) {
        slot_t *s = &g->slots[i];

        close_socket(s);
        drop_part_fd(s);
        flush_bytes(g, s);
        if (s->have_item)
            requeue_item(g, s, enh_part_offset(g->e, s->item.idx));
        s->st = S_IDLE;
    }
}

int enh_run_poll(enh_t *e)
{
    struct pollfd pfd[FDS_ENH_MAX_K];
    int           who[FDS_ENH_MAX_K];
    engine_t      g;
    int           code;

    memset(&g, 0, sizeof(g));
    g.e = e;
    g.k = e->k;
    if (e->k < 1 || e->k > FDS_ENH_MAX_K || e->q == NULL || e->topic == NULL ||
        strlen(e->topic) > FDS_TOPIC_MAX) {
        fail_fatal(&g, "internal error: invalid poll engine configuration");
        return FDS_EXIT_FATAL;
    }
    pthread_mutex_lock(&e->mu);
    g.stop = e->fatal || e->gave_up || e->restart;
    pthread_mutex_unlock(&e->mu);

    if (!g.stop && workq_outstanding(e->q) > 0) {
        g.slots = calloc((size_t)g.k, sizeof(*g.slots));
        g.buf = malloc(FDS_IO_BUF);
        if (g.slots == NULL || g.buf == NULL) {
            fail_fatal(&g, "out of memory");
        } else {
            for (int i = 0; i < g.k; i++) {
                g.slots[i].id = i + 1;
                g.slots[i].st = S_IDLE;
                g.slots[i].fd = -1;
                g.slots[i].file_fd = -1;
            }
            if (resolve_server(&g) != 0)
                raise_flag(&g, &e->fatal);
            else
                log_info("poll engine: %d connections, %" PRIu32 " files to fetch", g.k,
                         workq_outstanding(e->q));
        }
    }

    while (!g.stop && g.slots != NULL && workq_outstanding(e->q) > 0) {
        int64_t now = now_ns();
        int     n, r, timeout_ms;

        expire_timers(&g, now);
        if (g.stop)
            break;
        assign_work(&g);
        if (g.stop)
            break;
        n = build_pollset(&g, pfd, who, now_ns(), &timeout_ms);
        if (n == 0 && timeout_ms < 0) {
            /* Cannot happen: queued items go to idle slots, and a held item
             * always means a socket with a deadline. Never block forever. */
            fail_fatal(&g, "internal error: %" PRIu32 " items outstanding but nothing to wait for",
                       workq_outstanding(e->q));
            break;
        }
        r = poll(pfd, (nfds_t)n, timeout_ms);
        if (r < 0) {
            if (errno == EINTR)
                continue;       /* deadlines are absolute: recomputed next round */
            fail_fatal(&g, "poll: %s", strerror(errno));
            break;
        }
        for (int i = 0; i < n && !g.stop; i++) {
            if (pfd[i].revents != 0)
                service_slot(&g, &g.slots[who[i]], pfd[i].revents);
        }
    }

    if (g.slots != NULL)
        cleanup(&g);
    free(g.slots);
    free(g.buf);

    pthread_mutex_lock(&e->mu);
    code = e->fatal ? FDS_EXIT_FATAL : e->gave_up ? FDS_EXIT_GAVE_UP : FDS_EXIT_OK;
    pthread_mutex_unlock(&e->mu);
    return code;            /* restart: 0 with e->restart set; main() starts over */
}
