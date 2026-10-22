/*
 * client.c - Part II Case 1 client: download one topic over a connection
 * that the server may cut at any time, with NO session management.
 *
 * IT305 Socket Programming Project, Group 006 + 043.
 *
 * Usage: ./client <server_ip> <server_port> <topic> <output_dir> [max_retries]
 *        max_retries: 1..1000000, default 100 (FDS_CASE1_MAX_RETRIES)
 *
 * The files are written to output_dir/<topic exactly as typed>/ (deviation 4)
 * in place with O_TRUNC (RX_DIRECT): no .part or .session file ever exists,
 * so on success that directory holds only the topic's files.
 *
 * Message exchange (design section 3.6): the Part I messages, repeated.
 *
 *   client                                     server
 *     connect, GET <topic>   ------------->
 *                            <-------------    TOPIC_BEGIN(session 0, file_count, total_bytes)
 *                            <-------------    files #0 ... #56, part of #57
 *                                              (fault: shutdown + close)
 *     recv() returns 0 / reset / 15 s time-out before TOPIC_END: attempt failed
 *     back-off, reconnect
 *     connect, GET <topic>   ------------->
 *                            <-------------    TOPIC_BEGIN, files #0 ... #last (the WHOLE topic again)
 *                            <-------------    TOPIC_END
 *     check: TOPIC_BEGIN, TOPIC_END and what was stored all agree; exit 0
 *
 * No session state (design 3.6): neither side remembers an interrupted
 * transfer. Every attempt sends a plain GET and receives every file from
 * byte 0; each file is reopened with O_TRUNC, so the restart simply
 * overwrites whatever the failed attempt left behind. The bytes received by
 * the failed attempts are the redundant data that Case 2 is compared against,
 * so data_bytes_rx and wire_bytes_rx in the CSV accumulate over ALL attempts.
 *
 * Retry policy:
 *   - max_retries counts EVERY attempt, successful or not (design 4.4).
 *     A failed attempt is: connect failure, connection lost (EOF, reset,
 *     15 s receive time-out) or ERROR 503 (server at its thread limit).
 *     When max_retries attempts have failed the client prints
 *     "giving up after N attempts" and exits 2 (CSV status DNF).
 *   - ERROR 404/400/500, a protocol violation or a local file error cannot be
 *     cured by retrying: exit 1 at once.
 *   - Back-off before the next attempt: backoff_ms(streak), 50 ms doubling up
 *     to 1 s, where streak counts consecutive attempts that received NO file
 *     data. The back-off exists so that a client does not hammer a server
 *     that is down or overloaded. An attempt that did receive file data
 *     proves the server is up and serving; it was cut by an injected fault,
 *     not by overload, so waiting longer would only inflate the completion
 *     time. After such an attempt the streak resets and the wait is 50 ms
 *     ("back-off resets after progress", deviation 13).
 *
 * Exit status: 0 success, 1 fatal error, 2 gave up after max_retries.
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

#define MAX_RETRIES_LIMIT 1000000L

/* The validated command line. */
typedef struct {
    const char *ip;
    uint16_t    port;
    const char *topic;              /* exactly as typed: it names the output folder */
    const char *out_root;
    char        dir[PATH_MAX];      /* out_root/topic, where the files go */
    uint32_t    max_retries;        /* total attempts allowed */
} args_t;

/* Everything the run measured: the summary line and the CSV row. */
typedef struct {
    int64_t         start_ns, end_ns;
    int64_t         start_wall_ns, end_wall_ns;
    rx_ctx_t        rx;             /* one context for the whole run: its byte
                                       counters accumulate over all attempts */
    m_topic_begin_t begin;          /* what the server announced (last attempt) */
    m_topic_end_t   end;            /* what the server reported at the end */
    uint32_t        attempts;       /* connections tried so far */
} run_t;

/* How one attempt ended. */
typedef enum {
    ATTEMPT_OK,                     /* whole topic received and verified */
    ATTEMPT_RETRY,                  /* failed, but another attempt may succeed */
    ATTEMPT_FATAL                   /* failed for good (already reported) */
} attempt_t;

static void usage(const char *prog, const char *why)
{
    if (why != NULL)
        fprintf(stderr, "client: %s\n", why);
    fprintf(stderr, "usage: %s <server_ip> <server_port> <topic> <output_dir> [max_retries]\n",
            prog);
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
    long retries = FDS_CASE1_MAX_RETRIES;

    if (argc < 5 || argc > 6)
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
    if (argc == 6 && parse_long(argv[5], 1, MAX_RETRIES_LIMIT, &retries) != 0)
        usage(argv[0], "max_retries must be an integer in 1..1000000");
    a->max_retries = (uint32_t)retries;
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
 * Connect and set the socket up. Returns fd, or -1 with the reason in why.
 * Part II keeps the 15 s time-outs (deviation 1 applies to Part I only): a
 * server that stops sending without closing must still count as a failed
 * attempt. TCP_NODELAY (deviation 10) only affects latency: a warning.
 */
static int connect_to_server(const args_t *a, char *why, size_t cap)
{
    int fd = tcp_connect(a->ip, a->port, FDS_CONNECT_TIMEOUT_S);

    if (fd < 0) {
        snprintf(why, cap, "cannot connect to %s:%u: %s", a->ip, (unsigned)a->port,
                 strerror(errno));
        return -1;
    }
    if (set_timeouts(fd, FDS_CLI_TIMEOUT_S, FDS_CLI_TIMEOUT_S) != 0) {
        snprintf(why, cap, "cannot set socket time-outs: %s", strerror(errno));
        (void)close(fd);
        return -1;
    }
    if (set_nodelay(fd) != 0)
        log_warn("TCP_NODELAY failed: %s", strerror(errno));
    apply_env_bufsizes(fd);          /* only if FDS_SNDBUF / FDS_RCVBUF are set */
    return fd;
}

/* Send the request "GET <topic>". Returns 0, or -1 with the reason in why. */
static int send_get(int fd, const char *topic, char *why, size_t cap)
{
    uint8_t buf[FDS_HDR_LEN + 4 + FDS_TOPIC_MAX];
    size_t n = enc_get(buf, sizeof(buf), topic);

    if (n == 0 || send_encoded(fd, buf, n, 0) != 0) {
        snprintf(why, cap, "cannot send the GET request: %s", errno_text(errno));
        return -1;
    }
    return 0;
}

/*
 * TOPIC_BEGIN, then every FILE_HDR/data/FILE_END up to TOPIC_END.
 * Each attempt starts from scratch (file 0, nothing stored), but the byte
 * counters of r->rx keep growing across attempts.
 * An ERROR in place of TOPIC_BEGIN (e.g. 404, 503) comes back as RX_SERVER_ERR.
 */
static rx_rc_t receive_topic(run_t *r)
{
    uint8_t *payload = NULL;
    size_t len = 0;
    rx_rc_t rc;

    r->rx.next_index = 0;
    r->rx.stored_bytes = 0;
    rc = recv_expect(&r->rx, MSG_TOPIC_BEGIN, &payload, &len);
    if (rc == RX_OK && dec_topic_begin(payload, len, &r->begin) != 0)
        rc = RX_PROTO;
    free(payload);
    if (rc != RX_OK)
        return rc;
    return recv_topic(&r->rx, &r->end);
}

/*
 * The three views of the topic must agree: what TOPIC_BEGIN announced, what
 * TOPIC_END reports, and what this attempt received and stored.
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

/*
 * Decide what a receive result means for the retry loop.
 * Retryable failures only fill why (the caller prints the progress line);
 * fatal ones are reported here. err = errno right after the receive returned.
 */
static attempt_t classify(rx_rc_t rc, const run_t *r, const args_t *a, int err,
                          char *why, size_t cap)
{
    switch (rc) {
    case RX_OK:
        return totals_agree(r) ? ATTEMPT_OK : ATTEMPT_FATAL;
    case RX_LOST:
        /* EOF, reset or time-out before TOPIC_END: e.g. an injected fault.
         * An expired SO_RCVTIMEO shows up as EAGAIN (== EWOULDBLOCK on Linux). */
        snprintf(why, cap, "connection lost (%s)",
                 err == EAGAIN ? "no data for 15 s" :
                 err != 0 ? strerror(err) : "closed by the server");
        return ATTEMPT_RETRY;
    case RX_SERVER_ERR:
        if (r->rx.err.code == FDS_ERR_BUSY) {
            /* Thread limit reached: the server is fine, just full. */
            snprintf(why, cap, "server busy (503: %.200s)", r->rx.err.msg);
            return ATTEMPT_RETRY;
        }
        if (r->rx.err.code == FDS_ERR_NOT_FOUND)
            print_error("server error 404: %s (the topic '%s' does not exist on the server)",
                        r->rx.err.msg, a->topic);
        else
            print_error("server error %u: %s", (unsigned)r->rx.err.code, r->rx.err.msg);
        return ATTEMPT_FATAL;
    case RX_PROTO:
        print_error("protocol error: the server sent an invalid or unexpected message");
        return ATTEMPT_FATAL;
    case RX_IOERR:
        print_error("cannot write the received files into %s: %s", a->dir, errno_text(err));
        return ATTEMPT_FATAL;
    case RX_REJECT:
        print_error("protocol error: unexpected RESUME_REJECT (reason %u)",
                    (unsigned)r->rx.reject_reason);
        return ATTEMPT_FATAL;
    }
    return ATTEMPT_FATAL;            /* not reached: every rx_rc_t is handled */
}

/* One attempt: connect, GET, receive the whole topic, verify. */
static attempt_t run_attempt(run_t *r, const args_t *a, char *why, size_t cap)
{
    rx_rc_t rc;
    int err, fd = connect_to_server(a, why, cap);

    if (fd < 0)
        return ATTEMPT_RETRY;
    if (send_get(fd, a->topic, why, cap) != 0) {
        (void)close(fd);
        return ATTEMPT_RETRY;
    }
    r->rx.fd = fd;
    errno = 0;                       /* so err below belongs to this receive */
    rc = receive_topic(r);
    err = errno;
    if (rc == RX_OK) {
        r->end_ns = now_ns();        /* just after TOPIC_END */
        r->end_wall_ns = wall_ns();
    }
    if (close(fd) != 0)
        log_warn("close: %s", strerror(errno));
    r->rx.fd = -1;
    return classify(rc, r, a, err, why, cap);
}

/*
 * Attempt until success, a fatal error or max_retries attempts.
 * Returns the process exit code.
 */
static int fetch_with_retries(run_t *r, const args_t *a)
{
    char why[512];
    int streak = 0;                  /* consecutive attempts without file data */

    for (;;) {
        uint64_t data_before = r->rx.data_bytes;
        attempt_t res;
        long wait;

        r->attempts++;
        why[0] = '\0';
        res = run_attempt(r, a, why, sizeof(why));
        if (res == ATTEMPT_OK)
            return FDS_EXIT_OK;
        if (res == ATTEMPT_FATAL)
            return FDS_EXIT_FATAL;

        if (r->attempts >= a->max_retries) {
            log_info("attempt %u/%u failed: %s", r->attempts, a->max_retries, why);
            fprintf(stderr, "[client] giving up after %u attempts\n", r->attempts);
            return FDS_EXIT_GAVE_UP;
        }
        /* Progress (any file data) means the server is up: reset the back-off. */
        streak = r->rx.data_bytes > data_before ? 0 : streak + 1;
        wait = backoff_ms(FDS_MAX(streak, 1));
        log_info("attempt %u/%u failed after %llu data bytes: %s; retrying in %ld ms",
                 r->attempts, a->max_retries,
                 (unsigned long long)(r->rx.data_bytes - data_before), why, wait);
        sleep_ms(wait);
    }
}

/* The one stdout line of a successful run (same format as Part I). */
static void print_summary(const args_t *a, const run_t *r)
{
    double secs = (double)(r->end_ns - r->start_ns) / 1e9;
    double mbps = secs > 0 ? (double)r->rx.stored_bytes / 1e6 / secs : 0.0;

    printf("%s: %u files, %llu bytes in %.3f s (%.2f MB/s)\n", a->topic, r->rx.next_index,
           (unsigned long long)r->rx.stored_bytes, secs, mbps);
    log_info("%u attempt(s), %llu data bytes received in total (%llu redundant)",
             r->attempts, (unsigned long long)r->rx.data_bytes,
             (unsigned long long)(r->rx.data_bytes - r->rx.stored_bytes));
}

/*
 * One CSV row (only if FDS_LOG is set). useful_bytes = bytes of complete
 * files stored by the last attempt (the topic size on success);
 * data_bytes_rx counts every file byte of every attempt, so
 * data_bytes_rx - useful_bytes is the redundant data of Case 1.
 */
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
        .attempts = r->attempts,
        .reconnects = r->attempts > 0 ? r->attempts - 1 : 0,
        .rejects = 0,
        .status = status,
    };

    log_client(&row);
}

/* Stop the clock (if the transfer did not already), log, return the exit code. */
static int finish(const args_t *a, run_t *r, int code)
{
    if (r->end_ns == 0) {
        r->end_ns = now_ns();
        r->end_wall_ns = wall_ns();
    }
    if (code == FDS_EXIT_OK)
        print_summary(a, r);
    write_csv(a, r, code == FDS_EXIT_OK ? "OK" : code == FDS_EXIT_GAVE_UP ? "DNF" : "FAIL");
    log_close();
    return code;
}

int main(int argc, char **argv)
{
    static const log_ctx_t log_ctx = { "client", "case1", "-", -1.0, 0, 0 };
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

    r.start_ns = now_ns();           /* just before the first connect() */
    r.start_wall_ns = wall_ns();
    return finish(&a, &r, fetch_with_retries(&r, &a));
}
