/*
 * test_server_core.c - unit tests for server_core.c.
 *
 * server_run() never returns before a signal and installs process-wide
 * signal settings, so every server runs in a fork()ed child with a toy
 * serve_client(); the parent is the client and checks exit codes, timing and
 * the FDS_LOG CSV. The parent creates no threads, so fork() is safe, and
 * every child is forked BEFORE the parent opens client sockets (a child
 * holding a copy of a client socket would hide the client's close).
 *
 * Toy protocol: the client sends GET <topic>; the toy answers with an ERROR
 * message whose code and text tell the parent what happened:
 *   "hold"  ERROR 200 "holding", then blocks until the client closes;
 *   "nap"   sleeps 300 ms, then ERROR 404 "nap <start_ns> <end_ns>";
 *   other   ERROR 404 "no such topic" at once.
 *
 * Covers: conn_send/conn_send_error/conn_abort on a live socket, start-up
 * failures (missing root, port in use), SIGINT on an idle server, the
 * 256-thread cap (ERROR 503, recovery afterwards), single mode serving
 * strictly one client after the other (and multi mode overlapping), the
 * grace period in multi mode and the forced exit in single mode, and one
 * CSV row per connection.
 *
 * Expected warnings on stderr: the "cannot listen" and missing-root errors,
 * "N connection(s) still active after 5 s" and "forcing exit with 1 active
 * connection(s)".
 */
#define _GNU_SOURCE                      /* nftw() flags */
#include "unit.h"
#include "fds.h"
#include "log.h"
#include "net.h"
#include "protocol.h"
#include "server_core.h"
#include "util.h"

#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <limits.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define N_CAP_CLIENTS (FDS_MAX_THREADS + 5)

static char g_base[PATH_MAX];
static char g_root[PATH_MAX];

static void die(const char *what)
{
    fprintf(stderr, "test setup failed: %s: %s\n", what, strerror(errno));
    exit(1);
}

static double secs_since(int64_t t0)
{
    return (double)(now_ns() - t0) / 1e9;
}

/* ---- The toy serve_client (runs in the child) -------------------------------- */

static int toy_serve(conn_t *c)
{
    char topic[FDS_TOPIC_MAX + 1], text[128], junk[256];
    uint8_t buf[512], *p = NULL;
    msg_hdr_t h;
    size_t n;

    if (recv_msg(c->fd, &h, &p, NULL) != NET_OK || h.type != MSG_GET ||
        dec_get(p, (size_t)h.payload_len, topic) != 0) {
        free(p);
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "bad request");
        return 0;
    }
    free(p);
    c->kind = "get";
    snprintf(c->topic, sizeof(c->topic), "%s", topic);

    if (strcmp(topic, "hold") == 0) {
        n = enc_error(buf, sizeof(buf), 200, "holding");
        if (conn_send(c, buf, n, 0) == 0)
            while (recv(c->fd, junk, sizeof(junk), 0) > 0)
                ;
        return 0;                       /* status stays "OK" */
    }
    if (strcmp(topic, "nap") == 0) {
        int64_t t0 = now_ns(), t1;

        sleep_ms(300);
        t1 = now_ns();
        snprintf(text, sizeof(text), "nap %lld %lld", (long long)t0, (long long)t1);
        conn_send_error(c, FDS_ERR_NOT_FOUND, text);
        return 0;
    }
    conn_send_error(c, FDS_ERR_NOT_FOUND, "no such topic");
    return 0;
}

/* ---- Process helpers (parent) ---------------------------------------------------- */

/* A port that was free a moment ago (the child binds it right after). */
static uint16_t free_port(void)
{
    for (int i = 0; i < 100; i++) {
        uint16_t r, port;
        int fd;

        random_bytes((uint8_t *)&r, sizeof(r));
        port = (uint16_t)(20000 + r % 40001);
        fd = tcp_listen(port);
        if (fd >= 0) {
            close(fd);
            return port;
        }
    }
    die("no free port");
    return 0;
}

static pid_t start_server(uint16_t port, const char *root, int multi, const char *log_path)
{
    pid_t pid;

    fflush(stdout);
    fflush(stderr);
    pid = fork();
    if (pid < 0)
        die("fork");
    if (pid == 0) {
        server_cfg_t cfg;

        if (log_path != NULL)
            setenv("FDS_LOG", log_path, 1);
        else
            unsetenv("FDS_LOG");
        memset(&cfg, 0, sizeof(cfg));
        cfg.port = port;
        cfg.root = root;
        cfg.multi = multi;
        cfg.p = -1;
        cfg.case_name = "test";
        cfg.serve_client = toy_serve;
        _exit(server_run(&cfg));        /* _exit: never flush the parent's stdio copies */
    }
    return pid;
}

/* Wait for the child to exit for at most timeout_s. Returns 0 with *status
 * and *elapsed set, or -1 after killing a child that did not exit. */
static int wait_exit(pid_t pid, double timeout_s, int *status, double *elapsed)
{
    int64_t t0 = now_ns();

    for (;;) {
        pid_t r = waitpid(pid, status, WNOHANG);

        if (r == pid) {
            if (elapsed != NULL)
                *elapsed = secs_since(t0);
            return 0;
        }
        if (r < 0 && errno != EINTR)
            return -1;
        if (secs_since(t0) > timeout_s) {
            fprintf(stderr, "child %d did not exit within %.1f s\n", (int)pid, timeout_s);
            kill(pid, SIGKILL);
            waitpid(pid, status, 0);
            return -1;
        }
        sleep_ms(10);
    }
}

static int exited_with(int status, int code)
{
    return WIFEXITED(status) && WEXITSTATUS(status) == code;
}

/* Connect and send GET <topic>. Returns the socket or -1. */
static int client_get(uint16_t port, const char *topic)
{
    uint8_t buf[128];
    size_t n = enc_get(buf, sizeof(buf), topic);
    int fd = tcp_connect("127.0.0.1", port, 2);

    if (fd < 0)
        return -1;
    set_timeouts(fd, 15, 15);
    if (send_encoded(fd, buf, n, 0) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* Read one ERROR message. Returns 0 or -1. */
static int read_error(int fd, m_error_t *e)
{
    msg_hdr_t h;
    uint8_t *p = NULL;
    int rc = -1;

    memset(e, 0, sizeof(*e));
    if (recv_msg(fd, &h, &p, NULL) == NET_OK && h.type == MSG_ERROR &&
        dec_error(p, (size_t)h.payload_len, e) == 0)
        rc = 0;
    free(p);
    return rc;
}

/* One complete GET <topic> exchange; returns the ERROR code or -1. */
static int get_code(uint16_t port, const char *topic)
{
    m_error_t e;
    int fd = client_get(port, topic), code = -1;

    if (fd < 0)
        return -1;
    if (read_error(fd, &e) == 0)
        code = e.code;
    close(fd);
    return code;
}

/* Wait until the child serves requests (one GET -> 404 connection), at most
 * 5 s. Returns 0, or -1 if the child died or never answered. */
static int wait_ready(uint16_t port, pid_t pid)
{
    int64_t t0 = now_ns();

    while (secs_since(t0) < 5.0) {
        int status;

        if (get_code(port, "probe") == FDS_ERR_NOT_FOUND)
            return 0;
        if (waitpid(pid, &status, WNOHANG) == pid)
            return -1;
        sleep_ms(20);
    }
    return -1;
}

/* Lines of a file and how many contain `needle` (NULL: count nothing). */
static int csv_lines(const char *path, const char *needle, int *matching, char *first, size_t cap)
{
    char line[2048];
    int n = 0;
    FILE *f = fopen(path, "r");

    if (matching != NULL)
        *matching = 0;
    if (f == NULL)
        return -1;
    while (fgets(line, sizeof(line), f) != NULL) {
        if (n == 0 && first != NULL)
            snprintf(first, cap, "%s", line);
        n++;
        if (needle != NULL && matching != NULL && strstr(line, needle) != NULL)
            (*matching)++;
    }
    fclose(f);
    return n;
}

static int rm_entry(const char *path, const struct stat *st, int flag, struct FTW *ftw)
{
    (void)st; (void)flag; (void)ftw;
    return remove(path);
}

/* ---- Tests ------------------------------------------------------------------------- */

/* conn_* helpers on a live loopback socket (no server_run needed). */
static void test_conn_helpers(void)
{
    uint16_t port = 0;
    int lfd = -1, cli, srv;
    conn_t c;
    m_error_t e;
    char b;

    for (int i = 0; i < 100 && lfd < 0; i++) {
        port = free_port();
        lfd = tcp_listen(port);
    }
    if (lfd < 0)
        die("listen");
    cli = tcp_connect("127.0.0.1", port, 2);
    srv = accept(lfd, NULL, NULL);
    if (cli < 0 || srv < 0)
        die("loopback pair");
    set_timeouts(cli, 5, 5);

    memset(&c, 0, sizeof(c));
    c.fd = srv;
    c.status = "OK";
    conn_send_error(&c, FDS_ERR_BAD_REQUEST, "bad");
    CHECK_STR(c.status, "ERR_400");
    CHECK_EQ(c.wire_bytes, FDS_HDR_LEN + 4 + 3);
    conn_send_error(&c, FDS_ERR_NOT_FOUND, "nf");
    CHECK_STR(c.status, "ERR_404");
    conn_send_error(&c, FDS_ERR_IO, "io");
    CHECK_STR(c.status, "ERR_500");
    conn_send_error(&c, FDS_ERR_BUSY, "busy");
    CHECK_STR(c.status, "BUSY");
    CHECK_EQ(c.wire_bytes, 4 * (FDS_HDR_LEN + 4) + 3 + 2 + 2 + 4);
    CHECK(read_error(cli, &e) == 0 && e.code == FDS_ERR_BAD_REQUEST && strcmp(e.msg, "bad") == 0);
    CHECK(read_error(cli, &e) == 0 && e.code == FDS_ERR_NOT_FOUND);
    CHECK(read_error(cli, &e) == 0 && e.code == FDS_ERR_IO);
    CHECK(read_error(cli, &e) == 0 && e.code == FDS_ERR_BUSY && strcmp(e.msg, "busy") == 0);

    conn_abort(&c);
    CHECK_EQ(c.fd, -1);
    conn_abort(&c);                     /* second call is harmless */
    CHECK_EQ(recv(cli, &b, 1, 0), 0);   /* the peer sees end-of-stream */
    errno = 0;
    CHECK_EQ(conn_send(&c, (const uint8_t *)"0123456789abcdef", 16, 0), -1);
    CHECK_EQ(errno, EBADF);
    conn_send_error(&c, FDS_ERR_IO, "x"); /* on a closed conn: only the status changes */
    CHECK_STR(c.status, "ERR_500");
    CHECK_EQ(c.wire_bytes, 4 * (FDS_HDR_LEN + 4) + 3 + 2 + 2 + 4);
    CHECK_EQ(server_stopping(), 0);
    close(cli);
    close(lfd);
}

static void test_startup_failures(void)
{
    char missing[PATH_MAX];
    uint16_t port = free_port();
    int status = 0, lfd;
    pid_t pid;

    path_join(missing, sizeof(missing), g_base, "no_such_root");
    pid = start_server(port, missing, 1, NULL);
    CHECK_EQ(wait_exit(pid, 5.0, &status, NULL), 0);
    CHECK(exited_with(status, FDS_EXIT_FATAL));

    /* the port is held by this process: bind() fails in the child */
    lfd = tcp_listen(port);
    CHECK(lfd >= 0);
    pid = start_server(port, g_root, 1, NULL);
    CHECK_EQ(wait_exit(pid, 5.0, &status, NULL), 0);
    CHECK(exited_with(status, FDS_EXIT_FATAL));
    if (lfd >= 0)
        close(lfd);
}

static void test_sigint_idle(void)
{
    uint16_t port = free_port();
    pid_t pid = start_server(port, g_root, 1, NULL);
    int status = 0;
    double t = 0;

    CHECK_EQ(wait_ready(port, pid), 0);
    CHECK_EQ(kill(pid, SIGINT), 0);
    CHECK_EQ(wait_exit(pid, 2.0, &status, &t), 0);
    CHECK(exited_with(status, 0));
    printf("idle server exited %.3f s after SIGINT\n", t);
}

static void test_csv_rows(void)
{
    char log_path[PATH_MAX], first[512];
    uint16_t port = free_port();
    pid_t pid;
    int status = 0, rows, err404 = 0;

    path_join(log_path, sizeof(log_path), g_base, "conns.csv");
    pid = start_server(port, g_root, 1, log_path);
    CHECK_EQ(wait_ready(port, pid), 0);             /* connection 1 */
    for (int i = 0; i < 3; i++)                     /* connections 2..4 */
        CHECK_EQ(get_code(port, "nosuch"), FDS_ERR_NOT_FOUND);
    CHECK_EQ(kill(pid, SIGTERM), 0);
    CHECK_EQ(wait_exit(pid, 3.0, &status, NULL), 0);
    CHECK(exited_with(status, 0));

    rows = csv_lines(log_path, ",get,", NULL, first, sizeof(first));
    CHECK_EQ(rows, 1 + 4);                          /* header + one row per connection */
    CHECK_STR(first, "run_id,case,mode,p,seed,conn_id,kind,session_id,topic,k,peer,"
                     "start_ns,end_ns,start_wall_ns,data_bytes,wire_bytes,chunks,"
                     "files_completed,faulted,status\n");
    csv_lines(log_path, ",ERR_404\n", &err404, NULL, 0);
    CHECK_EQ(err404, 4);
    /* conn ids 1..4, in order, after "run_id,case,mode,p,seed," = "-,test,multi,-1,-," */
    for (int id = 1; id <= 4; id++) {
        char needle[64];
        int m = 0;

        snprintf(needle, sizeof(needle), "-,test,multi,-1,-,%d,get,-,", id);
        csv_lines(log_path, needle, &m, NULL, 0);
        CHECK_EQ(m, 1);
    }
}

static void test_thread_cap(void)
{
    char log_path[PATH_MAX];
    uint16_t port = free_port();
    int fds[N_CAP_CLIENTS], held = 0, busy = 0, other = 0, connects = 1, status = 0;
    int busy_rows = 0, rows, code = -1;
    int64_t t0;
    pid_t pid;

    path_join(log_path, sizeof(log_path), g_base, "cap.csv");
    pid = start_server(port, g_root, 1, log_path);
    CHECK_EQ(wait_ready(port, pid), 0);

    for (int i = 0; i < N_CAP_CLIENTS; i++) {
        fds[i] = client_get(port, "hold");
        CHECK(fds[i] >= 0);
        if (fds[i] >= 0)
            connects++;
    }
    for (int i = 0; i < N_CAP_CLIENTS; i++) {
        m_error_t e;

        if (fds[i] < 0 || read_error(fds[i], &e) != 0)
            other++;
        else if (e.code == 200)
            held++;
        else if (e.code == FDS_ERR_BUSY)
            busy++;
        else
            other++;
    }
    printf("thread cap: %d held, %d refused with 503, %d other\n", held, busy, other);
    CHECK(busy >= 1);
    CHECK_EQ(held, FDS_MAX_THREADS);                /* every slot was used */
    CHECK_EQ(busy, N_CAP_CLIENTS - FDS_MAX_THREADS);
    CHECK_EQ(other, 0);

    /* After the clients close, the handlers end and new clients get served. */
    for (int i = 0; i < N_CAP_CLIENTS; i++)
        if (fds[i] >= 0)
            close(fds[i]);
    t0 = now_ns();
    while (secs_since(t0) < 5.0) {
        code = get_code(port, "nosuch");
        if (code >= 0)
            connects++;
        if (code == FDS_ERR_NOT_FOUND)
            break;
        sleep_ms(20);
    }
    CHECK_EQ(code, FDS_ERR_NOT_FOUND);

    CHECK_EQ(kill(pid, SIGTERM), 0);
    CHECK_EQ(wait_exit(pid, 3.0, &status, NULL), 0);
    CHECK(exited_with(status, 0));
    rows = csv_lines(log_path, ",BUSY\n", &busy_rows, NULL, 0);
    CHECK_EQ(rows, 1 + connects);
    CHECK_EQ(busy_rows, busy);
}

/* Two clients at once; returns 1 if their handler intervals overlapped. */
static int two_naps(uint16_t port, double *total)
{
    long long s[2] = { 0, 0 }, e[2] = { 0, 0 };
    int64_t t0 = now_ns();
    int fd[2];

    fd[0] = client_get(port, "nap");
    fd[1] = client_get(port, "nap");
    CHECK(fd[0] >= 0 && fd[1] >= 0);
    for (int i = 0; i < 2; i++) {
        m_error_t m;

        CHECK(fd[i] >= 0 && read_error(fd[i], &m) == 0 &&
              sscanf(m.msg, "nap %lld %lld", &s[i], &e[i]) == 2);
        if (fd[i] >= 0)
            close(fd[i]);
    }
    *total = secs_since(t0);
    CHECK(s[0] < e[0] && s[1] < e[1]);
    return s[0] < e[1] && s[1] < e[0];
}

static void test_single_vs_multi(void)
{
    uint16_t p_single = free_port(), p_multi = free_port();
    pid_t single = start_server(p_single, g_root, 0, NULL);
    pid_t multi = start_server(p_multi, g_root, 1, NULL);
    int status = 0;
    double t_single = 0, t_multi = 0;

    CHECK_EQ(wait_ready(p_single, single), 0);
    CHECK_EQ(wait_ready(p_multi, multi), 0);
    CHECK_EQ(two_naps(p_single, &t_single), 0);     /* strictly one after the other */
    CHECK_EQ(two_naps(p_multi, &t_multi), 1);       /* concurrent */
    printf("two 300 ms handlers: single mode %.3f s, multi mode %.3f s\n", t_single, t_multi);
    CHECK(t_single >= 0.6);
    CHECK(t_multi < 0.6);

    kill(single, SIGINT);
    kill(multi, SIGINT);
    CHECK_EQ(wait_exit(single, 3.0, &status, NULL), 0);
    CHECK(exited_with(status, 0));
    CHECK_EQ(wait_exit(multi, 3.0, &status, NULL), 0);
    CHECK(exited_with(status, 0));
}

/* A handler that never ends: multi mode waits the grace period and exits 0;
 * single mode (handler on the main thread) is ended by the signal thread. */
static void test_shutdown_busy(void)
{
    uint16_t p_multi = free_port(), p_single = free_port();
    pid_t multi = start_server(p_multi, g_root, 1, NULL);
    pid_t single = start_server(p_single, g_root, 0, NULL);
    int fm, fs, status = 0;
    double tm = 0, ts = 0;
    m_error_t e;

    CHECK_EQ(wait_ready(p_multi, multi), 0);
    CHECK_EQ(wait_ready(p_single, single), 0);
    fm = client_get(p_multi, "hold");
    fs = client_get(p_single, "hold");
    CHECK(fm >= 0 && read_error(fm, &e) == 0 && e.code == 200);     /* handler running */
    CHECK(fs >= 0 && read_error(fs, &e) == 0 && e.code == 200);

    CHECK_EQ(kill(multi, SIGTERM), 0);
    CHECK_EQ(kill(single, SIGINT), 0);
    CHECK_EQ(wait_exit(multi, FDS_SHUTDOWN_GRACE_S + 2.0, &status, &tm), 0);
    CHECK(exited_with(status, 0));
    CHECK_EQ(wait_exit(single, FDS_SHUTDOWN_GRACE_S + 2.0, &status, &ts), 0);
    ts += tm;                           /* both clocks started at the signals */
    CHECK(exited_with(status, 0));
    printf("busy shutdown: multi mode exited after %.2f s, single mode after %.2f s\n", tm, ts);
    CHECK(tm >= FDS_SHUTDOWN_GRACE_S - 0.5);        /* it did wait for the transfer */
    CHECK(ts >= FDS_SHUTDOWN_GRACE_S + 0.5);        /* forced exit after grace + 1 s */
    CHECK(ts <= FDS_SHUTDOWN_GRACE_S + 2.0);
    if (fm >= 0)
        close(fm);
    if (fs >= 0)
        close(fs);
}

int main(void)
{
    const char *tmp = env_str("TMPDIR");
    char topic[PATH_MAX];

    if (path_join(g_base, sizeof(g_base), tmp != NULL ? tmp : "/tmp", "fds_core_XXXXXX") != 0 ||
        mkdtemp(g_base) == NULL)
        die("mkdtemp");
    if (path_join(g_root, sizeof(g_root), g_base, "root") != 0 ||
        path_join(topic, sizeof(topic), g_root, "t") != 0 || mkdir_p(topic, 0755) != 0)
        die("mkdir");
    /* The children inherit this: start/summary lines would only be noise. */
    setenv("FDS_QUIET", "1", 1);
    unsetenv("FDS_RUN_ID");             /* the CSV checks expect the default "-" */
    unsetenv("FDS_EVENTS");
    signal(SIGPIPE, SIG_IGN);

    test_conn_helpers();
    test_startup_failures();
    test_sigint_idle();
    test_csv_rows();
    test_thread_cap();
    test_single_vs_multi();
    test_shutdown_busy();

    nftw(g_base, rm_entry, 16, FTW_DEPTH | FTW_PHYS);
    UNIT_DONE();
}
