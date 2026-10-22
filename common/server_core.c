/*
 * server_core.c - the part of the server that is the same in all four
 * versions (design sections 7 and 8.3, deviation 14). The contract is in
 * server_core.h; this comment explains how it is met.
 *
 * Threads
 *   main thread    loads the topic map, listens, then runs the accept loop.
 *                  Single mode: it serves each client itself, so the next
 *                  client is accepted only after the current one is done.
 *                  Multi mode: it starts one detached thread per client.
 *   signal thread  sigwait()s for SIGINT/SIGTERM and starts the shutdown.
 *   worker threads (multi mode) run cfg->serve_client() for one connection.
 *
 * Signals
 *   SIGINT and SIGTERM are blocked BEFORE any thread exists, so every thread
 *   inherits the blocked mask and the kernel can deliver them only to the
 *   one thread that sigwait()s for them. The shutdown logic therefore runs as
 *   ordinary code in that thread, not in an async signal handler where almost
 *   nothing (locks, malloc, stdio) is allowed. SIGPIPE is ignored: a client
 *   that disappears (or a fault we inject) must turn a send into EPIPE, not
 *   kill the whole server. Every send also uses MSG_NOSIGNAL anyway.
 *
 * Shutdown (SIGINT/SIGTERM; exit status 0)
 *   1. The signal thread sets `stopping` and calls shutdown() on the listening
 *      socket. On Linux that wakes a thread blocked in accept() (EINVAL), and
 *      new clients are refused from now on.
 *   2. The main thread leaves the accept loop, closes the listening socket and
 *      waits up to FDS_SHUTDOWN_GRACE_S for the active connections to end.
 *      It then prints a summary, closes the CSV logs and returns 0.
 *   3. The signal thread waits for step 2 for FDS_SHUTDOWN_GRACE_S + 1 s. If
 *      the main thread is still busy (single mode in the middle of a long
 *      transfer, where the main thread itself is the worker), it closes the
 *      logs and calls _exit(0), so a shutdown never hangs.
 *
 * Shared state and locking
 *   g_mu protects every non-atomic global below: the active-connection count,
 *   the statistics, the listening fd (so the signal thread never shuts down a
 *   descriptor number the main thread has already closed and the kernel may
 *   have reused) and g_main_done. g_cv is signalled whenever g_active drops or
 *   g_main_done is set; it uses CLOCK_MONOTONIC so that a wall-clock change
 *   cannot stretch or shorten the grace period.
 *   `stopping` and the connection-id counter are C11 atomics because they are
 *   read without the lock (server_stopping() is called from worker loops).
 */
#define _GNU_SOURCE                     /* accept4() */
#include "server_core.h"
#include "log.h"
#include "net.h"
#include "protocol.h"
#include "topic_map.h"
#include "util.h"

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define NS_PER_S 1000000000LL

/* ---- Global state ---------------------------------------------------------- */

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_cv;            /* initialised in server_run() (needs CLOCK_MONOTONIC) */
static int             g_active;        /* connections being served right now */
static int             g_main_done;     /* main thread finished its part of the shutdown */
static int             g_listen_fd = -1;

/* Totals over the whole run, for the summary line. */
static uint64_t g_conns, g_busy, g_data_bytes, g_wire_bytes, g_faults;

static atomic_int        g_stopping;
static _Atomic uint64_t  g_next_conn_id = 1;

/* The map lives as long as the process; worker threads hold pointers into it. */
static topic_map_t g_map;
static pthread_attr_t g_detached;

/*
 * The pthread mutex/cond calls fail only on programming errors (a lock that
 * was never initialised, or is already held by this thread). Continuing would
 * mean running without mutual exclusion, so stop loudly instead.
 */
static void sync_failed(const char *what, int err)
{
    fprintf(stderr, "[server] FATAL: %s failed: %s\n", what, strerror(err));
    abort();
}

static void lock(void)
{
    int rc = pthread_mutex_lock(&g_mu);

    if (rc != 0)
        sync_failed("pthread_mutex_lock", rc);
}

static void unlock(void)
{
    int rc = pthread_mutex_unlock(&g_mu);

    if (rc != 0)
        sync_failed("pthread_mutex_unlock", rc);
}

static void broadcast(void)
{
    int rc = pthread_cond_broadcast(&g_cv);

    if (rc != 0)
        sync_failed("pthread_cond_broadcast", rc);
}

/*
 * Wait on g_cv (g_mu held) until woken or until the absolute CLOCK_MONOTONIC
 * time deadline_ns. Returns 0 when woken (the caller re-checks its condition:
 * wake-ups may be spurious) or ETIMEDOUT.
 */
static int wait_until(int64_t deadline_ns)
{
    struct timespec ts;
    int rc;

    ts.tv_sec = (time_t)(deadline_ns / NS_PER_S);
    ts.tv_nsec = (long)(deadline_ns % NS_PER_S);
    rc = pthread_cond_timedwait(&g_cv, &g_mu, &ts);
    if (rc != 0 && rc != ETIMEDOUT)
        sync_failed("pthread_cond_timedwait", rc);
    return rc;
}

/* The two signals that mean "shut down gracefully". */
static void stop_signal_set(sigset_t *set)
{
    sigemptyset(set);
    sigaddset(set, SIGINT);
    sigaddset(set, SIGTERM);
}

/* ---- Per-connection helpers (public) -------------------------------------- */

void conn_abort(conn_t *c)
{
    /* shutdown() before close() sends FIN at once even if the kernel still
     * holds queued data, which is what a fault must look like to the client:
     * the bytes already sent arrive, then end-of-stream. fd = -1 makes a
     * second call, and the close() in run_conn(), harmless. */
    if (c->fd >= 0) {
        shutdown_close(c->fd);
        c->fd = -1;
    }
}

int conn_send(conn_t *c, const uint8_t *buf, size_t len, int more)
{
    if (c->fd < 0) {
        errno = EBADF;
        return -1;
    }
    if (send_encoded(c->fd, buf, len, more) != 0)
        return -1;
    /* Counted only once the whole message is in the socket; a failed send
     * ends the connection anyway, so its partial bytes are not worth a
     * separate count. */
    c->wire_bytes += len;
    return 0;
}

/* Status column of the CSV row for an ERROR code. Static strings, because
 * conn_t.status is a pointer that outlives this call. */
static const char *status_for_code(uint16_t code)
{
    switch (code) {
    case FDS_ERR_BAD_REQUEST: return "ERR_400";
    case FDS_ERR_NOT_FOUND:   return "ERR_404";
    case FDS_ERR_IO:          return "ERR_500";
    case FDS_ERR_BUSY:        return "BUSY";
    default:                  return "ERR_OTHER";
    }
}

void conn_send_error(conn_t *c, uint16_t code, const char *msg)
{
    int saved = errno;
    uint8_t buf[FDS_HDR_LEN + 4 + sizeof(((m_error_t *)0)->msg)];
    size_t n = enc_error(buf, sizeof(buf), code, msg);

    /* Best effort: an ERROR is always followed by closing the connection, so
     * if the peer is already gone there is nobody left to tell. */
    if (n > 0)
        (void)conn_send(c, buf, n, 0);
    c->status = status_for_code(code);
    errno = saved;                      /* callers may still report their own error */
}

int server_stopping(void)
{
    return atomic_load(&g_stopping);
}

/* ---- Connection life cycle -------------------------------------------------- */

/*
 * The end of every connection, served or refused: write its CSV row, close
 * the socket, add it to the totals and free it. was_active says whether it
 * was counted in g_active (refused connections never were).
 */
static void finish_conn(conn_t *c, int was_active, int was_busy)
{
    log_conn_row_t row;

    memset(&row, 0, sizeof(row));
    row.conn_id = c->conn_id;
    row.kind = c->kind;
    row.session_id = c->have_session ? c->session_id : NULL;
    row.topic = c->topic;
    row.k = c->k;
    row.peer = c->peer;
    row.start_ns = c->start_ns;
    row.end_ns = now_ns();
    row.start_wall_ns = c->start_wall_ns;
    row.data_bytes = c->data_bytes;
    row.wire_bytes = c->wire_bytes;
    row.chunks = c->chunks;
    row.files_completed = c->files_completed;
    row.faulted = c->faulted;
    row.status = c->status;
    log_conn(&row);

    /* A plain close() (FIN, not RST): after a complete transfer the client has
     * read up to TOPIC_END and sent nothing more, so no unread data is
     * pending. close() on Linux releases the descriptor even when it reports
     * EINTR, so it is never retried (that could close a reused number). */
    if (c->fd >= 0 && close(c->fd) != 0 && errno != EINTR)
        log_warn("conn %llu: close: %s", (unsigned long long)c->conn_id, strerror(errno));
    c->fd = -1;

    lock();
    g_conns++;
    if (was_busy)
        g_busy++;
    g_data_bytes += c->data_bytes;
    g_wire_bytes += c->wire_bytes;
    if (c->faulted)
        g_faults++;
    if (was_active) {
        g_active--;
        broadcast();                    /* the shutdown may be waiting for g_active == 0 */
    }
    unlock();
    free(c);
}

/*
 * After a normal end of service, half-close and wait for the client's own
 * close before the connection's end time is taken. send()/sendfile() return
 * as soon as the kernel has queued the data, so without this wait the last
 * few MB still sitting in the socket buffers would fall outside the measured
 * interval and inflate the throughput (the Part I metric is "time required to
 * send data to the requesting client"). Every client closes right after its
 * last message (TOPIC_END / MANIFEST / last FILE_END), so the wait is about
 * one round trip. The 5 s cap bounds a misbehaving client. It also means the
 * final close() is a clean FIN with no unread data, never an RST.
 */
/*
 * Wait until fd is readable or the absolute deadline (now_ns() clock) passes.
 * Returns 1 if readable, 0 on time-out, -1 on error.
 */
static int wait_readable(int fd, int64_t deadline)
{
    for (;;) {
        int64_t left_ms = (deadline - now_ns()) / 1000000;
        struct pollfd pfd = { fd, POLLIN, 0 };
        int r;

        if (left_ms <= 0)
            return 0;
        r = poll(&pfd, 1, left_ms > INT_MAX ? INT_MAX : (int)left_ms);
        if (r < 0 && errno == EINTR)
            continue;
        return r < 0 ? -1 : r > 0;
    }
}

static void drain_until_peer_closes(conn_t *c)
{
    /* The deadline is absolute: a peer that keeps trickling bytes cannot
     * stretch the wait beyond FDS_SHUTDOWN_GRACE_S (it would otherwise hold
     * the single-mode server's only thread). */
    int64_t deadline = now_ns() + (int64_t)FDS_SHUTDOWN_GRACE_S * 1000000000LL;
    char buf[256];

    if (c->fd < 0 || shutdown(c->fd, SHUT_WR) != 0)
        return;
    while (wait_readable(c->fd, deadline) == 1) {
        ssize_t n = recv(c->fd, buf, sizeof(buf), MSG_DONTWAIT);
        if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
            continue;
        break;    /* 0 = client closed (the normal case); data or error: stop waiting */
    }
}

/*
 * Read exactly n bytes before the absolute deadline. Returns NET_OK, NET_EOF
 * (closed before the first byte), or NET_ERR (errno: EAGAIN = deadline,
 * ECONNRESET = closed part-way, or the recv error). *got = bytes read.
 */
static int read_by_deadline(int fd, uint8_t *buf, size_t n, int64_t deadline, size_t *got)
{
    *got = 0;
    while (*got < n) {
        int w = wait_readable(fd, deadline);
        ssize_t r;

        if (w == 0) {
            errno = EAGAIN;
            return NET_ERR;
        }
        if (w < 0)
            return NET_ERR;
        r = recv(fd, buf + *got, n - *got, MSG_DONTWAIT);
        if (r > 0) {
            *got += (size_t)r;
            continue;
        }
        if (r == 0) {
            if (*got == 0)
                return NET_EOF;
            errno = ECONNRESET;
            return NET_ERR;
        }
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
            continue;
        return NET_ERR;
    }
    return NET_OK;
}

int conn_recv_request(conn_t *c, msg_hdr_t *h, uint8_t **payload, uint64_t *wire)
{
    int64_t deadline = now_ns() + (int64_t)FDS_SRV_TIMEOUT_S * 1000000000LL;
    uint8_t hdr[FDS_HDR_LEN];
    uint8_t *buf = NULL;
    size_t got;
    int rc;

    *payload = NULL;
    if (c->fd < 0) {
        errno = EBADF;
        return NET_ERR;
    }
    rc = read_by_deadline(c->fd, hdr, sizeof(hdr), deadline, &got);
    if (wire != NULL)
        *wire += got;
    if (rc == NET_EOF)
        return NET_EOF;
    if (rc != NET_OK)
        return NET_ERR;
    if (hdr_decode(hdr, h) != 0)
        return NET_PROTO;
    /* Only requests may come from a client, and they are small: refuse
     * anything else before allocating a single byte for it. */
    if ((h->type != MSG_GET && h->type != MSG_RESUME && h->type != MSG_MANIFEST_REQ &&
         h->type != MSG_FETCH) || h->payload_len > FDS_MAX_REQUEST_PAYLOAD)
        return NET_PROTO;
    if (h->payload_len == 0)
        return NET_OK;
    buf = malloc((size_t)h->payload_len);
    if (buf == NULL)
        return NET_ERR;
    rc = read_by_deadline(c->fd, buf, (size_t)h->payload_len, deadline, &got);
    if (wire != NULL)
        *wire += got;
    if (rc != NET_OK) {
        free(buf);
        if (rc == NET_EOF)
            errno = ECONNRESET;    /* the header arrived, so this is mid-message */
        return NET_ERR;
    }
    *payload = buf;
    return NET_OK;
}

/* Serve one admitted connection to the end (any thread). */
static void run_conn(conn_t *c)
{
    /* The return value is documented as ignored: serve_client() reports its
     * outcome through c->status and the counters. */
    (void)c->cfg->serve_client(c);
    if (strcmp(c->status, "OK") == 0)
        drain_until_peer_closes(c);
    finish_conn(c, 1, 0);
}

static void *conn_thread(void *arg)
{
    run_conn(arg);
    return NULL;
}

/*
 * Refuse a connection with ERROR 503 (thread limit reached, or no thread
 * could be created). Runs on the main thread, so it must never block long:
 * the socket is fresh, so the small ERROR fits in its send buffer at once.
 */
static void refuse_busy(conn_t *c)
{
    char drain[512];

    conn_send_error(c, FDS_ERR_BUSY, "server busy");
    /* Closing a socket that still holds unread data (the client's request)
     * makes Linux reset the connection with RST and drop anything not yet
     * transmitted, which could include the ERROR itself. So: queue the FIN
     * behind the ERROR, then discard whatever request bytes have already
     * arrived, without waiting for more (MSG_DONTWAIT, a few reads at most). */
    if (c->fd >= 0 && shutdown(c->fd, SHUT_WR) == 0) {
        for (int i = 0; i < 8; i++)
            if (recv(c->fd, drain, sizeof(drain), MSG_DONTWAIT) <= 0)
                break;
    }
    finish_conn(c, 0, 1);
}

/* Set up an accepted socket and hand it to a worker (or serve it inline). */
static void handle_new_conn(int fd, const server_cfg_t *cfg)
{
    conn_t *c;
    int admitted = 0, rc;

    /* 30 s send/receive time-outs: a client that stops reading or sending
     * cannot pin a worker thread forever (design 6.1). Failure only loses
     * that protection, so the client is still served. */
    if (set_timeouts(fd, FDS_SRV_TIMEOUT_S, FDS_SRV_TIMEOUT_S) != 0)
        log_warn("set_timeouts: %s", strerror(errno));
    if (set_nodelay(fd) != 0)
        log_warn("TCP_NODELAY: %s", strerror(errno));
    apply_env_bufsizes(fd);

    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        log_warn("out of memory: connection refused");
        send_error(fd, FDS_ERR_BUSY, "server busy");
        if (close(fd) != 0 && errno != EINTR)
            log_warn("close: %s", strerror(errno));
        return;
    }
    c->fd = fd;
    c->conn_id = atomic_fetch_add(&g_next_conn_id, 1);
    peer_to_str(fd, c->peer, sizeof(c->peer));
    c->map = &g_map;
    c->cfg = cfg;
    c->start_ns = now_ns();
    c->start_wall_ns = wall_ns();
    c->kind = "-";
    c->status = "OK";
    c->topic[0] = '\0';                 /* calloc() did it; explicit for the reader */

    if (!cfg->multi) {
        /* Single-threaded mode (design 8.3): serve inline, so the next accept()
         * happens only after this client is completely done. g_active is
         * still counted so that the shutdown logic is the same in both modes. */
        lock();
        g_active++;
        unlock();
        run_conn(c);
        return;
    }

    /* Thread cap (design 7): beyond FDS_MAX_THREADS concurrent workers the
     * server answers ERROR 503 instead of starting yet another thread. The
     * slot is reserved under the lock before the thread exists, so the count
     * can never overshoot. */
    lock();
    if (g_active < FDS_MAX_THREADS) {
        g_active++;
        admitted = 1;
    }
    unlock();
    if (admitted) {
        pthread_t tid;

        rc = pthread_create(&tid, &g_detached, conn_thread, c);
        if (rc != 0) {
            /* Out of threads or memory: same answer as the cap. */
            log_warn("pthread_create: %s; connection refused", strerror(rc));
            lock();
            g_active--;
            broadcast();
            unlock();
            admitted = 0;
        }
    }
    if (!admitted)
        refuse_busy(c);
}

/*
 * Accept until shutdown. Returns FDS_EXIT_OK after a shutdown request, or
 * FDS_EXIT_FATAL if the listening socket itself has become unusable.
 */
static int accept_loop(int lfd, const server_cfg_t *cfg)
{
    while (!atomic_load(&g_stopping)) {
        /* SOCK_CLOEXEC: client sockets must not leak into any child process. */
        int fd = accept4(lfd, NULL, NULL, SOCK_CLOEXEC);
        int err;

        if (fd >= 0) {
            handle_new_conn(fd, cfg);
            continue;
        }
        err = errno;
        /* The signal thread's shutdown() makes accept() fail (EINVAL): that
         * is the normal way out of this loop. */
        if (atomic_load(&g_stopping))
            break;
        switch (err) {
        case EINTR:
        case ECONNABORTED:              /* client gave up while queued: nothing to do */
            continue;
        case ENETDOWN: case EPROTO: case ENOPROTOOPT: case EHOSTDOWN:
        case ENONET: case EHOSTUNREACH: case EOPNOTSUPP: case ENETUNREACH:
            /* Linux reports pending network errors of the NEW connection
             * through accept(); accept(2) says to treat them like EAGAIN. */
            continue;
        case EMFILE:
        case ENFILE:
        case ENOBUFS:
        case ENOMEM:
            /* Out of descriptors or memory. Retrying at once would spin at
             * 100% CPU; a short pause lets running transfers finish and
             * release theirs. Clients wait in the listen backlog meanwhile. */
            log_warn("accept: %s; retrying in 100 ms", strerror(err));
            sleep_ms(100);
            continue;
        case EBADF:
        case EINVAL:
        case ENOTSOCK:
        case EFAULT:
            log_warn("accept: %s; the listening socket is unusable, stopping", strerror(err));
            return FDS_EXIT_FATAL;
        default:
            log_warn("accept: %s; retrying in 100 ms", strerror(err));
            sleep_ms(100);
            continue;
        }
    }
    return FDS_EXIT_OK;
}

/* ---- Signal thread ----------------------------------------------------------- */

static void *signal_thread(void *arg)
{
    sigset_t set;
    int sig = 0, rc, done, left, shut_err = 0;
    int64_t deadline;

    (void)arg;
    stop_signal_set(&set);
    for (;;) {
        rc = sigwait(&set, &sig);
        if (rc == 0)
            break;
        if (rc != EINTR) {
            log_warn("sigwait: %s; SIGINT/SIGTERM will not shut down the server", strerror(rc));
            return NULL;
        }
    }

    lock();
    if (g_main_done) {
        /* Woken by server_run() itself (pthread_kill) after the accept loop
         * failed: the shutdown is already done, just end the thread. */
        unlock();
        return NULL;
    }
    unlock();
    log_info("%s received: shutting down (no new connections, up to %d s for active ones)",
             sig == SIGINT ? "SIGINT" : "SIGTERM", FDS_SHUTDOWN_GRACE_S);

    lock();
    atomic_store(&g_stopping, 1);
    /* Under g_mu: the main thread closes this fd under g_mu too, so the
     * number cannot have been reused for something else. */
    if (g_listen_fd >= 0 && shutdown(g_listen_fd, SHUT_RDWR) != 0)
        shut_err = errno;
    deadline = now_ns() + (int64_t)(FDS_SHUTDOWN_GRACE_S + 1) * NS_PER_S;
    while (!g_main_done && wait_until(deadline) == 0)
        ;
    done = g_main_done;
    left = g_active;
    unlock();

    if (shut_err != 0)
        log_warn("shutdown of the listening socket: %s", strerror(shut_err));
    if (!done) {
        /* The main thread is still inside a connection (single mode) or stuck.
         * A shutdown must never hang an experiment script, so end the process
         * here; the logs are flushed first so no row is lost. */
        log_warn("forcing exit with %d active connection(s)", left);
        log_close();
        _exit(FDS_EXIT_OK);
    }
    return NULL;
}

/* ---- server_run ----------------------------------------------------------------- */

/* Reset the globals and create the condition variable and thread attributes. */
static int init_state(void)
{
    pthread_condattr_t ca;
    int rc;

    g_active = 0;
    g_main_done = 0;
    g_listen_fd = -1;
    g_conns = g_busy = g_data_bytes = g_wire_bytes = g_faults = 0;
    atomic_store(&g_stopping, 0);
    atomic_store(&g_next_conn_id, 1);

    if ((rc = pthread_condattr_init(&ca)) != 0)
        goto fail;
    /* Grace periods are intervals: measure them on the monotonic clock. */
    if ((rc = pthread_condattr_setclock(&ca, CLOCK_MONOTONIC)) != 0 ||
        (rc = pthread_cond_init(&g_cv, &ca)) != 0) {
        pthread_condattr_destroy(&ca);
        goto fail;
    }
    pthread_condattr_destroy(&ca);
    /* Workers are detached: nobody joins them, their resources are released
     * when they end, and the shutdown waits on g_active instead. */
    if ((rc = pthread_attr_init(&g_detached)) != 0) {
        pthread_cond_destroy(&g_cv);
        goto fail;
    }
    if ((rc = pthread_attr_setdetachstate(&g_detached, PTHREAD_CREATE_DETACHED)) != 0) {
        pthread_attr_destroy(&g_detached);
        pthread_cond_destroy(&g_cv);
        goto fail;
    }
    return 0;

fail:
    log_warn("server start-up: %s", strerror(rc));
    return -1;
}

int server_run(const server_cfg_t *cfg)
{
    log_ctx_t lc;
    sigset_t set, old_mask;
    pthread_t sig_tid;
    int lfd, rc, exit_code, left;
    uint64_t conns, busy, data, wire, faults;
    int64_t deadline;

    lc.role = "server";
    lc.case_name = cfg->case_name;
    lc.mode = cfg->multi ? "multi" : "single";
    lc.p = cfg->p;
    lc.have_seed = cfg->have_seed;
    lc.seed = cfg->seed;
    log_init(&lc);

    if (cfg->serve_client == NULL) {
        log_warn("server_run: no serve_client handler");
        log_close();
        return FDS_EXIT_FATAL;
    }
    /* Not fatal if it fails: every send uses MSG_NOSIGNAL anyway. */
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR)
        log_warn("cannot ignore SIGPIPE: %s", strerror(errno));

    /* Before ANY thread is created, so that all of them inherit the mask. */
    stop_signal_set(&set);
    rc = pthread_sigmask(SIG_BLOCK, &set, &old_mask);
    if (rc != 0) {
        log_warn("pthread_sigmask: %s", strerror(rc));
        log_close();
        return FDS_EXIT_FATAL;
    }
    if (init_state() != 0)
        goto fail_mask;
    if (topic_map_load(&g_map, cfg->root) != 0)
        goto fail_state;
    lfd = tcp_listen(cfg->port);
    if (lfd < 0)
        goto fail_map;
    g_listen_fd = lfd;              /* no other thread exists yet */

    if (cfg->p >= 0)
        log_info("listening on port %u: %zu topic(s), %s mode, p = %g",
                 (unsigned)cfg->port, g_map.n_topics, lc.mode, cfg->p);
    else
        log_info("listening on port %u: %zu topic(s), %s mode",
                 (unsigned)cfg->port, g_map.n_topics, lc.mode);

    rc = pthread_create(&sig_tid, NULL, signal_thread, NULL);
    if (rc != 0) {
        log_warn("cannot start the signal thread: %s", strerror(rc));
        if (close(lfd) != 0)
            log_warn("close: %s", strerror(errno));
        g_listen_fd = -1;
        goto fail_map;
    }

    exit_code = accept_loop(lfd, cfg);

    /* ---- graceful shutdown, main-thread part ---- */
    lock();
    atomic_store(&g_stopping, 1);       /* also after a fatal accept error */
    if (close(g_listen_fd) != 0)
        log_warn("close of the listening socket: %s", strerror(errno));
    g_listen_fd = -1;
    deadline = now_ns() + (int64_t)FDS_SHUTDOWN_GRACE_S * NS_PER_S;
    while (g_active > 0 && wait_until(deadline) == 0)
        ;
    left = g_active;
    conns = g_conns;
    busy = g_busy;
    data = g_data_bytes;
    wire = g_wire_bytes;
    faults = g_faults;
    unlock();

    if (left > 0)
        log_warn("%d connection(s) still active after %d s; exiting without them",
                 left, FDS_SHUTDOWN_GRACE_S);
    log_info("summary: %llu connection(s) served (%llu refused busy), %llu data bytes sent "
             "(%llu on the wire), %llu fault(s) injected",
             (unsigned long long)conns, (unsigned long long)busy, (unsigned long long)data,
             (unsigned long long)wire, (unsigned long long)faults);
    log_close();
    /* Detached workers that are still running hold pointers into the map
     * (and to the condition variable), so only free them if none is left. */
    if (left == 0)
        topic_map_free(&g_map);

    lock();
    g_main_done = 1;
    broadcast();                        /* releases the signal thread's wait */
    unlock();
    /* If the loop ended without a signal (fatal accept error), the signal
     * thread is still in sigwait(): a thread-directed SIGTERM ends that wait.
     * Otherwise the signal stays pending on that thread and is discarded when
     * it exits. SIGINT/SIGTERM stay blocked on purpose: unblocking them with
     * a second Ctrl-C pending would kill the process with a non-zero status. */
    rc = pthread_kill(sig_tid, SIGTERM);
    if (rc != 0 && rc != ESRCH)
        log_warn("pthread_kill: %s", strerror(rc));
    rc = pthread_join(sig_tid, NULL);
    if (rc != 0)
        log_warn("pthread_join: %s", strerror(rc));
    /* The attribute was only needed by pthread_create() on this thread; the
     * condition variable may still be used by workers that outlived the grace
     * period, so it is destroyed only when none is left. */
    pthread_attr_destroy(&g_detached);
    if (left == 0)
        pthread_cond_destroy(&g_cv);
    return exit_code;

fail_map:
    topic_map_free(&g_map);
fail_state:
    pthread_attr_destroy(&g_detached);
    pthread_cond_destroy(&g_cv);
fail_mask:
    /* No thread was started: restore the caller's mask (a SIGINT that came in
     * meanwhile is then delivered normally, which is what the user wanted). */
    rc = pthread_sigmask(SIG_SETMASK, &old_mask, NULL);
    if (rc != 0)
        log_warn("pthread_sigmask: %s", strerror(rc));
    log_close();
    return FDS_EXIT_FATAL;
}
