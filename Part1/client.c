/*
 * client.c - Part I client: download every file of one topic.
 *
 * IT305 Socket Programming Project, Group 006 + 043.
 *
 * Usage: ./client <server_ip> <server_port> <topic> <output_dir>
 *
 * The files are written to output_dir/<topic exactly as typed>/, which is
 * created like `mkdir -p` (deviation 4: the server may match "Cane" to the
 * folder "cane", but the grader looks for the name it passed). Files are
 * written in place with O_TRUNC (RX_DIRECT), so on success that directory
 * holds only the topic's files.
 *
 * Message exchange (design section 3.5):
 *
 *   client                                     server
 *     connect, GET <topic>   ------------->
 *                            <-------------    TOPIC_BEGIN(session 0, file_count, total_bytes)
 *                            <-------------    FILE_HDR(#i, size, offset 0, name)
 *                            <-------------    size raw bytes, written to output_dir/topic/name
 *                            <-------------    FILE_END(#i)
 *                                  ... one FILE_HDR/data/FILE_END per file, in index order ...
 *                            <-------------    TOPIC_END(file_count, total_bytes)
 *                                              server closes
 *     check: TOPIC_BEGIN, TOPIC_END and what was stored all agree; exit 0
 *
 *   Unknown topic: ERROR 404 instead of TOPIC_BEGIN -> message, exit 1.
 *
 * No receive time-out (deviation 1): against the single-threaded server a
 * client legitimately waits, connected but silent, until every client
 * accepted before it has been served. With 32 clients that is far longer
 * than any fixed time-out, so Part I waits as long as the server needs.
 *
 * Measurement: the interval runs from just before connect() to just after
 * TOPIC_END (CLOCK_MONOTONIC). Success prints one line on stdout:
 *   "<topic>: <n> files, <bytes> bytes in <s> s (<MB/s> MB/s)"   (MB = 10^6 bytes)
 * and, if FDS_LOG is set, one CSV row (log.h).
 *
 * Exit status: 0 success; 1 usage error, unknown topic, connection lost,
 * protocol or file error. Part I never retries.
 */
#include "fds.h"
#include "log.h"
#include "net.h"
#include "protocol.h"
#include "transfer.h"
#include "util.h"

#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* The validated command line. */
typedef struct {
    const char *ip;
    uint16_t    port;
    const char *topic;              /* exactly as typed: it names the output folder */
    const char *out_root;
    char        dir[PATH_MAX];      /* out_root/topic, where the files go */
} args_t;

/* Everything one run measured: the summary line and the CSV row. */
typedef struct {
    int64_t         start_ns, end_ns;
    int64_t         start_wall_ns, end_wall_ns;
    rx_ctx_t        rx;             /* receive state and byte counters */
    m_topic_begin_t begin;          /* what the server announced */
    m_topic_end_t   end;            /* what the server reported at the end */
} run_t;

static void usage(const char *prog, const char *why)
{
    if (why != NULL)
        fprintf(stderr, "client: %s\n", why);
    fprintf(stderr, "usage: %s <server_ip> <server_port> <topic> <output_dir>\n", prog);
    exit(FDS_EXIT_FATAL);
}

/* "[client] error: ..." on stderr, always (even with FDS_QUIET). */
static void print_error(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void print_error(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    fputs("[client] error: ", stderr);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    va_end(ap);
}

/* strerror() that does not print "Success" when no errno was recorded. */
static const char *errno_text(int err)
{
    return err != 0 ? strerror(err) : "unknown error";
}

static void parse_args(int argc, char **argv, args_t *a)
{
    if (argc != 5)
        usage(argv[0], NULL);
    memset(a, 0, sizeof(*a));
    a->ip = argv[1];
    if (a->ip[0] == '\0')
        usage(argv[0], "server_ip must not be empty");
    if (parse_port(argv[2], &a->port) != 0)
        usage(argv[0], "server_port must be an integer in 1..65535");
    a->topic = argv[3];
    if (!topic_is_valid(a->topic))
        usage(argv[0], "topic must be 1..63 bytes, without '/' or control characters");
    a->out_root = argv[4];
    /* "" would turn output_dir/topic into /topic at the file-system root. */
    if (a->out_root[0] == '\0')
        usage(argv[0], "output_dir must not be empty");
    if (path_join(a->dir, sizeof(a->dir), a->out_root, a->topic) != 0)
        usage(argv[0], "output_dir is too long");
}

/* mkdir -p output_dir/topic before connecting, so a bad path costs no transfer. */
static int make_output_dir(const args_t *a)
{
    if (mkdir_p(a->dir, 0755) != 0) {
        print_error("cannot create directory %s: %s", a->dir, strerror(errno));
        return -1;
    }
    return 0;
}

/*
 * Connect and set the socket up. No SO_RCVTIMEO (deviation 1, see the top
 * of the file). TCP_NODELAY (deviation 10) only affects latency, so its
 * failure is a warning, not an error.
 */
static int connect_to_server(const args_t *a)
{
    int fd = tcp_connect(a->ip, a->port, FDS_CONNECT_TIMEOUT_S);

    if (fd < 0) {
        print_error("cannot connect to %s:%u: %s", a->ip, (unsigned)a->port, strerror(errno));
        return -1;
    }
    if (set_nodelay(fd) != 0)
        log_warn("TCP_NODELAY failed: %s", strerror(errno));
    apply_env_bufsizes(fd);          /* only if FDS_SNDBUF / FDS_RCVBUF are set */
    return fd;
}

/* Send the request "GET <topic>", framed by the standard header. */
static int send_get(int fd, const char *topic)
{
    uint8_t buf[FDS_HDR_LEN + 4 + FDS_TOPIC_MAX];
    size_t n = enc_get(buf, sizeof(buf), topic);

    if (n == 0 || send_encoded(fd, buf, n, 0) != 0) {
        print_error("cannot send the GET request: %s", errno_text(errno));
        return -1;
    }
    return 0;
}

/*
 * TOPIC_BEGIN, then every FILE_HDR/data/FILE_END up to TOPIC_END
 * (recv_topic() checks that the indices arrive in order from 0).
 * An ERROR in place of TOPIC_BEGIN (e.g. 404) comes back as RX_SERVER_ERR.
 */
static rx_rc_t receive_topic(run_t *r)
{
    uint8_t *payload = NULL;
    size_t len = 0;
    rx_rc_t rc = recv_expect(&r->rx, MSG_TOPIC_BEGIN, &payload, &len);

    if (rc == RX_OK && dec_topic_begin(payload, len, &r->begin) != 0)
        rc = RX_PROTO;
    free(payload);
    if (rc != RX_OK)
        return rc;
    r->rx.next_index = 0;
    r->rx.stored_bytes = 0;
    return recv_topic(&r->rx, &r->end);
}

/* Explain a failed receive. err = errno right after the receive returned. */
static void report_rx_failure(rx_rc_t rc, const rx_ctx_t *rx, const args_t *a, int err)
{
    switch (rc) {
    case RX_OK:
        break;
    case RX_SERVER_ERR:
        if (rx->err.code == FDS_ERR_NOT_FOUND)
            print_error("server error 404: %s (the topic '%s' does not exist on the server)",
                        rx->err.msg, a->topic);
        else
            print_error("server error %u: %s", (unsigned)rx->err.code, rx->err.msg);
        break;
    case RX_LOST:
        print_error("connection to %s:%u lost before the topic was complete (%s)",
                    a->ip, (unsigned)a->port,
                    err != 0 ? strerror(err) : "closed by the server");
        break;
    case RX_PROTO:
        print_error("protocol error: the server sent an invalid or unexpected message");
        break;
    case RX_IOERR:
        print_error("cannot write the received files into %s: %s", a->dir, errno_text(err));
        break;
    case RX_REJECT:
        print_error("protocol error: unexpected RESUME_REJECT (reason %u)",
                    (unsigned)rx->reject_reason);
        break;
    }
}

/*
 * The three views of the topic must agree: what TOPIC_BEGIN announced, what
 * TOPIC_END reports, and what was actually received and stored.
 */
static int totals_agree(const run_t *r)
{
    const m_topic_begin_t *b = &r->begin;
    const m_topic_end_t *e = &r->end;

    if (e->file_count == b->file_count && r->rx.next_index == b->file_count &&
        e->total_bytes == b->total_bytes && r->rx.stored_bytes == b->total_bytes)
        return 1;
    print_error("inconsistent transfer: TOPIC_BEGIN announced %u files / %llu bytes, "
                "TOPIC_END reported %u / %llu, received %u / %llu",
                b->file_count, (unsigned long long)b->total_bytes,
                e->file_count, (unsigned long long)e->total_bytes,
                r->rx.next_index, (unsigned long long)r->rx.stored_bytes);
    return 0;
}

/* The one stdout line of a successful run. */
static void print_summary(const args_t *a, const run_t *r)
{
    double secs = (double)(r->end_ns - r->start_ns) / 1e9;
    double mbps = secs > 0 ? (double)r->rx.stored_bytes / 1e6 / secs : 0.0;

    printf("%s: %u files, %llu bytes in %.3f s (%.2f MB/s)\n", a->topic, r->rx.next_index,
           (unsigned long long)r->rx.stored_bytes, secs, mbps);
}

/* One CSV row (only if FDS_LOG is set). Part I: one connection, k = 1. */
static void write_csv(const args_t *a, const run_t *r, const char *status)
{
    log_client_row_t row = {
        .topic = a->topic,
        .k = 1,
        .io = "-",
        .start_ns = r->start_ns,
        .end_ns = r->end_ns,
        .start_wall_ns = r->start_wall_ns,
        .end_wall_ns = r->end_wall_ns,
        .useful_bytes = r->rx.stored_bytes,
        .data_bytes_rx = r->rx.data_bytes,
        .wire_bytes_rx = r->rx.wire_bytes,
        .files = r->rx.next_index,
        .attempts = 1,
        .reconnects = 0,
        .rejects = 0,
        .status = status,
    };

    log_client(&row);
}

/* Stop the clock (if the transfer did not already), log, return the exit code. */
static int finish(const args_t *a, run_t *r, int ok)
{
    if (r->end_ns == 0) {
        r->end_ns = now_ns();
        r->end_wall_ns = wall_ns();
    }
    if (ok)
        print_summary(a, r);
    write_csv(a, r, ok ? "OK" : "FAIL");
    log_close();
    return ok ? FDS_EXIT_OK : FDS_EXIT_FATAL;
}

/* fetch_topic() result: the server refused with ERROR 503 (thread limit). */
#define FETCH_BUSY 2

/* One connection: GET, receive everything, verify. Returns 1 on success,
 * FETCH_BUSY if the server is at its thread limit, 0 on any other failure. */
static int fetch_topic(const args_t *a, run_t *r)
{
    rx_rc_t rc;
    int err, fd = connect_to_server(a);

    if (fd < 0)
        return 0;
    if (send_get(fd, a->topic) != 0) {
        (void)close(fd);
        return 0;
    }
    r->rx.fd = fd;
    errno = 0;                       /* so err below belongs to this receive */
    rc = receive_topic(r);
    err = errno;
    r->end_ns = now_ns();            /* just after TOPIC_END (or the failure) */
    r->end_wall_ns = wall_ns();
    if (close(fd) != 0)
        log_warn("close: %s", strerror(errno));
    r->rx.fd = -1;

    if (rc == RX_SERVER_ERR && r->rx.err.code == FDS_ERR_BUSY)
        return FETCH_BUSY;
    if (rc != RX_OK) {
        report_rx_failure(rc, &r->rx, a, err);
        return 0;
    }
    return totals_agree(r);
}

/*
 * Part I has no fault tolerance, but ERROR 503 is not a failure of the
 * transfer: the multi-threaded server is at its FDS_MAX_THREADS limit and
 * refused before sending anything. The client waits (the usual back-off) and
 * asks again, so a burst of more than 256 clients is served rather than
 * having the extra clients exit 1. Every other error stays fatal.
 */
#define PART1_BUSY_TRIES 100

static int fetch_topic_when_free(const args_t *a, run_t *r)
{
    for (int tries = 1;; tries++) {
        int ok = fetch_topic(a, r);

        if (ok != FETCH_BUSY)
            return ok;
        if (tries >= PART1_BUSY_TRIES) {
            print_error("server busy (503) on %d attempts; giving up", tries);
            return 0;
        }
        log_info("server busy (503); retrying in %ld ms", backoff_ms(tries));
        sleep_ms(backoff_ms(tries));
    }
}

int main(int argc, char **argv)
{
    static const log_ctx_t log_ctx = { "client", "part1", "-", -1.0, 0, 0 };
    args_t a;
    run_t r;

    parse_args(argc, argv, &a);
    if (make_output_dir(&a) != 0)
        return FDS_EXIT_FATAL;
    log_init(&log_ctx);

    memset(&r, 0, sizeof(r));
    r.rx.fd = -1;
    r.rx.dir = a.dir;
    r.rx.mode = RX_DIRECT;           /* write <name> directly, O_TRUNC */

    r.start_ns = now_ns();           /* just before connect() */
    r.start_wall_ns = wall_ns();
    return finish(&a, &r, fetch_topic_when_free(&a, &r));
}
