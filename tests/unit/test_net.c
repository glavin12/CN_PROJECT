/*
 * test_net.c - unit tests for net.c (and the socket half of protocol.c:
 * recv_msg, send_encoded, send_error) over real loopback TCP connections.
 *
 * Covers: listen/connect (IP and host name), full-length I/O of a 5 MB buffer
 * across threads, EOF in the middle of a read, every recv_msg outcome with
 * exact wire-byte counts, connection refused, connect time-out, SO_RCVTIMEO
 * and SO_SNDTIMEO, EPIPE instead of SIGPIPE, socket options and peer_to_str.
 */
#include "unit.h"
#include "fds.h"
#include "net.h"
#include "protocol.h"
#include "util.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

/* ---- Helpers --------------------------------------------------------------- */

/* tcp_listen() on a random free port in 20000..60000. A port that is taken
 * makes tcp_listen() print one "cannot listen" line, which is harmless. */
static int listen_random(uint16_t *port)
{
    for (int i = 0; i < 100; i++) {
        uint16_t r, p;
        int fd;

        random_bytes((uint8_t *)&r, sizeof(r));
        p = (uint16_t)(20000 + r % 40001);
        fd = tcp_listen(p);
        if (fd >= 0) {
            *port = p;
            return fd;
        }
    }
    return -1;
}

/* A connected loopback pair: *c from tcp_connect(), *s from accept(). */
static int tcp_pair(int lfd, uint16_t port, int *c, int *s)
{
    *c = tcp_connect("127.0.0.1", port, 5);
    if (*c < 0)
        return -1;
    *s = accept(lfd, NULL, NULL);
    if (*s < 0) {
        close(*c);
        return -1;
    }
    return 0;
}

static uint16_t local_port(int fd)
{
    struct sockaddr_in a;
    socklen_t len = sizeof(a);

    if (getsockname(fd, (struct sockaddr *)&a, &len) != 0)
        return 0;
    return ntohs(a.sin_port);
}

static double secs_since(int64_t t0)
{
    return (double)(now_ns() - t0) / 1e9;
}

static int sockopt_int(int fd, int level, int opt)
{
    int v = -1;
    socklen_t len = sizeof(v);

    if (getsockopt(fd, level, opt, &v, &len) != 0)
        return -1;
    return v;
}

/* Deterministic, non-periodic test pattern. */
static void fill_pattern(uint8_t *p, size_t n, uint32_t seed)
{
    uint32_t x = seed | 1;

    for (size_t i = 0; i < n; i++) {
        x ^= x << 13; x ^= x >> 17; x ^= x << 5;
        p[i] = (uint8_t)x;
    }
}

/* ---- Listen / connect ------------------------------------------------------ */

static void test_listen_connect(int lfd, uint16_t port)
{
    static const char *hosts[] = { "127.0.0.1", "localhost" };
    char str[64], exp[64];
    uint8_t b[8];
    size_t got;

    CHECK(fcntl(lfd, F_GETFD) & FD_CLOEXEC);
    CHECK(sockopt_int(lfd, SOL_SOCKET, SO_REUSEADDR) > 0);
    CHECK(sockopt_int(lfd, SOL_SOCKET, SO_ACCEPTCONN) == 1);
    CHECK_EQ(local_port(lfd), port);

    for (size_t i = 0; i < 2; i++) {
        int c = tcp_connect(hosts[i], port, 5), s;

        CHECK(c >= 0);
        if (c < 0)
            continue;
        s = accept(lfd, NULL, NULL);
        CHECK(s >= 0);
        /* The returned socket is blocking again and close-on-exec. */
        CHECK((fcntl(c, F_GETFL) & O_NONBLOCK) == 0);
        CHECK(fcntl(c, F_GETFD) & FD_CLOEXEC);

        peer_to_str(c, str, sizeof(str));
        snprintf(exp, sizeof(exp), "127.0.0.1:%u", (unsigned)port);
        CHECK_STR(str, exp);
        peer_to_str(s, str, sizeof(str));
        snprintf(exp, sizeof(exp), "127.0.0.1:%u", (unsigned)local_port(c));
        CHECK_STR(str, exp);

        CHECK_EQ(sendall(c, "ping", 4, 0), 0);
        CHECK_EQ(recvall(s, b, 4, &got), NET_OK);
        CHECK_EQ(got, 4);
        CHECK(memcmp(b, "ping", 4) == 0);
        CHECK_EQ(sendall(s, "pong", 4, 0), 0);
        CHECK_EQ(recvall(c, b, 4, NULL), NET_OK);
        CHECK(memcmp(b, "pong", 4) == 0);
        close(s);
        close(c);
    }

    /* timeout_sec 0 means "no limit": still connects at once. */
    {
        int c = tcp_connect("127.0.0.1", port, 0), s;

        CHECK(c >= 0);
        s = accept(lfd, NULL, NULL);
        CHECK(s >= 0);
        close(s);
        close(c);
    }

    /* A second listener on a port in use fails (and prints one line). */
    errno = 0;
    CHECK_EQ(tcp_listen(port), -1);
    CHECK_EQ(errno, EADDRINUSE);
}

static void test_connect_refused(void)
{
    uint16_t port;
    int lfd = listen_random(&port);
    int64_t t0;

    CHECK(lfd >= 0);
    close(lfd);                                   /* nobody listens there now */
    t0 = now_ns();
    errno = 0;
    CHECK_EQ(tcp_connect("127.0.0.1", port, 5), -1);
    CHECK_EQ(errno, ECONNREFUSED);
    CHECK(secs_since(t0) < 1.0);
}

/*
 * Connect time-out: a listener with backlog 0 accepts one connection into
 * its queue; while that one is not accept()ed, Linux drops further SYNs, so
 * the next connect can only end by our poll() time-out.
 */
static void test_connect_timeout(void)
{
    struct sockaddr_in a;
    socklen_t len = sizeof(a);
    int lfd = socket(AF_INET, SOCK_STREAM, 0), fds[16], nfds = 0, err = 0;
    double elapsed = 0;

    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;                               /* any free port */
    CHECK(lfd >= 0);
    CHECK_EQ(bind(lfd, (struct sockaddr *)&a, sizeof(a)), 0);
    CHECK_EQ(listen(lfd, 0), 0);
    CHECK_EQ(getsockname(lfd, (struct sockaddr *)&a, &len), 0);

    for (int i = 0; i < 16; i++) {
        int64_t t0 = now_ns();
        int fd = tcp_connect("127.0.0.1", ntohs(a.sin_port), 1);

        if (fd >= 0) {
            fds[nfds++] = fd;
            continue;
        }
        err = errno;
        elapsed = secs_since(t0);
        break;
    }
    CHECK_EQ(err, ETIMEDOUT);
    CHECK(elapsed >= 0.9 && elapsed < 3.0);
    for (int i = 0; i < nfds; i++)
        close(fds[i]);
    close(lfd);
}

static void test_resolve_fail(void)
{
    /* .invalid is reserved (RFC 6761) and never resolves. One warning line. */
    errno = 0;
    CHECK_EQ(tcp_connect("no-such-host.invalid", 9, 2), -1);
    CHECK_EQ(errno, EHOSTUNREACH);
}

/* ---- Full-length I/O -------------------------------------------------------- */

#define BIG (5u * 1024u * 1024u)

struct reader {
    int      fd;
    uint8_t *buf;
    size_t   n;
    size_t   got;
    int      rc;
};

static void *reader_main(void *arg)
{
    struct reader *r = arg;

    r->rc = recvall(r->fd, r->buf, r->n, &r->got);
    return NULL;
}

/* Sends buf in irregular pieces (1, 7, 4096, 65537, ...) with MSG_MORE on
 * all but the last, so the receiver's single recvall() has to reassemble. */
struct writer {
    int            fd;
    const uint8_t *buf;
    size_t         n;
    int            rc;
};

static void *writer_main(void *arg)
{
    static const size_t pieces[] = { 1, 7, 4096, 65537, 3, 1000000 };
    struct writer *w = arg;
    size_t off = 0, i = 0;

    w->rc = 0;
    while (off < w->n && w->rc == 0) {
        size_t k = FDS_MIN(pieces[i++ % 6], w->n - off);

        w->rc = sendall(w->fd, w->buf + off, k, off + k < w->n ? MSG_MORE : 0);
        off += k;
    }
    return NULL;
}

static void test_big_transfer(int lfd, uint16_t port)
{
    uint8_t *out = malloc(BIG), *in = malloc(BIG);
    int c, s;
    pthread_t th;

    fill_pattern(out, BIG, 12345);
    CHECK_EQ(tcp_pair(lfd, port, &c, &s), 0);

    /* One sendall() of 5 MB (far more than the socket buffers, so send()
     * returns short counts) against one recvall() in another thread. */
    {
        struct reader rd = { s, in, BIG, 0, 99 };

        memset(in, 0, BIG);
        CHECK_EQ(pthread_create(&th, NULL, reader_main, &rd), 0);
        CHECK_EQ(sendall(c, out, BIG, 0), 0);
        CHECK_EQ(pthread_join(th, NULL), 0);
        CHECK_EQ(rd.rc, NET_OK);
        CHECK_EQ(rd.got, BIG);
        CHECK(memcmp(in, out, BIG) == 0);
    }
    /* The other direction, written in odd pieces. */
    {
        struct writer wr = { s, out, BIG, 99 };
        size_t got = 0;

        memset(in, 0, BIG);
        CHECK_EQ(pthread_create(&th, NULL, writer_main, &wr), 0);
        CHECK_EQ(recvall(c, in, BIG, &got), NET_OK);
        CHECK_EQ(pthread_join(th, NULL), 0);
        CHECK_EQ(wr.rc, 0);
        CHECK_EQ(got, BIG);
        CHECK(memcmp(in, out, BIG) == 0);
    }
    close(c);
    close(s);
    free(out);
    free(in);
}

static void test_recvall_eof(int lfd, uint16_t port)
{
    uint8_t out[100], in[1000];
    size_t got = 12345;
    int c, s;

    fill_pattern(out, sizeof(out), 7);
    CHECK_EQ(tcp_pair(lfd, port, &c, &s), 0);
    CHECK_EQ(sendall(s, out, sizeof(out), 0), 0);
    shutdown_close(s);

    /* Asked for 1000, the peer sent 100 and closed. */
    CHECK_EQ(recvall(c, in, sizeof(in), &got), NET_EOF);
    CHECK_EQ(got, 100);
    CHECK(memcmp(in, out, 100) == 0);
    got = 12345;
    CHECK_EQ(recvall(c, in, 10, &got), NET_EOF);
    CHECK_EQ(got, 0);
    CHECK_EQ(recvall(c, in, 10, NULL), NET_EOF);
    got = 12345;
    CHECK_EQ(recvall(c, in, 0, &got), NET_OK);     /* nothing asked: OK */
    CHECK_EQ(got, 0);
    close(c);

    /* Error path: errno from recv() is kept, got is still set. */
    got = 12345;
    errno = 0;
    CHECK_EQ(recvall(-1, in, 10, &got), NET_ERR);
    CHECK_EQ(errno, EBADF);
    CHECK_EQ(got, 0);
    errno = 0;
    CHECK_EQ(sendall(-1, out, 10, 0), -1);
    CHECK_EQ(errno, EBADF);
    CHECK_EQ(sendall(-1, out, 0, 0), 0);          /* nothing to send */
}

/* ---- recv_msg ----------------------------------------------------------------- */

static void test_recv_msg(void)
{
    int sv[2];
    uint8_t m[512], *payload = (uint8_t *)1;
    msg_hdr_t h;
    uint64_t wire = 0;
    size_t n;

    /* A good conversation: FILE_END, FILE_HDR + raw data, an empty payload,
     * then a clean close. */
    CHECK_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    n = enc_file_end(m, sizeof(m), 42);
    CHECK_EQ(send_encoded(sv[0], m, n, 0), 0);
    {
        m_file_hdr_t fh;

        memset(&fh, 0, sizeof(fh));
        fh.file_index = 7;
        fh.file_size = 5;
        strcpy(fh.name, "a.jpg");
        n = enc_file_hdr(m, sizeof(m), &fh);
        CHECK_EQ(n, 16 + 27);
        CHECK_EQ(send_encoded(sv[0], m, n, 0), 0);
        CHECK_EQ(sendall(sv[0], "abcde", 5, 0), 0);
    }
    hdr_encode(m, MSG_ERROR, 0);
    CHECK_EQ(sendall(sv[0], m, FDS_HDR_LEN, 0), 0);
    CHECK_EQ(shutdown(sv[0], SHUT_WR), 0);

    CHECK_EQ(recv_msg(sv[1], &h, &payload, &wire), NET_OK);
    CHECK_EQ(h.type, MSG_FILE_END);
    CHECK_EQ(h.payload_len, 4);
    CHECK(payload != NULL);
    CHECK_EQ(wire, 20);
    {
        m_file_end_t fe;

        CHECK_EQ(dec_file_end(payload, (size_t)h.payload_len, &fe), 0);
        CHECK_EQ(fe.file_index, 42);
    }
    free(payload);

    CHECK_EQ(recv_msg(sv[1], &h, &payload, &wire), NET_OK);
    CHECK_EQ(h.type, MSG_FILE_HDR);
    CHECK_EQ(h.payload_len, 27);
    CHECK_EQ(wire, 20 + 43);
    {
        m_file_hdr_t fh;
        char raw[6] = { 0 };

        CHECK_EQ(dec_file_hdr(payload, 27, &fh), 0);
        CHECK_STR(fh.name, "a.jpg");
        /* Only the metadata was consumed: the raw bytes are still there. */
        CHECK_EQ(recvall(sv[1], raw, 5, NULL), NET_OK);
        CHECK_STR(raw, "abcde");
        wire += 5;
    }
    free(payload);

    CHECK_EQ(recv_msg(sv[1], &h, &payload, &wire), NET_OK);
    CHECK_EQ(h.type, MSG_ERROR);
    CHECK_EQ(h.payload_len, 0);
    CHECK(payload == NULL);                        /* no allocation for 0 bytes */
    CHECK_EQ(wire, 20 + 43 + 5 + 16);

    payload = (uint8_t *)1;
    CHECK_EQ(recv_msg(sv[1], &h, &payload, &wire), NET_EOF);
    CHECK(payload == NULL);
    CHECK_EQ(wire, 20 + 43 + 5 + 16);              /* clean close adds nothing */
    CHECK_EQ(recv_msg(sv[1], &h, &payload, NULL), NET_EOF);   /* wire optional */
    close(sv[0]);
    close(sv[1]);

    /* Partial header, then EOF: a cut connection, not a clean close. */
    CHECK_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    n = enc_file_end(m, sizeof(m), 1);
    CHECK_EQ(sendall(sv[0], m, 7, 0), 0);
    CHECK_EQ(shutdown(sv[0], SHUT_WR), 0);
    wire = 100;
    errno = 0;
    CHECK_EQ(recv_msg(sv[1], &h, &payload, &wire), NET_ERR);
    CHECK_EQ(errno, ECONNRESET);
    CHECK_EQ(wire, 107);
    CHECK(payload == NULL);
    close(sv[0]);
    close(sv[1]);

    /* Full header, half the payload, then EOF. */
    CHECK_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    {
        m_topic_end_t te = { 3, 300 };

        n = enc_topic_end(m, sizeof(m), &te);
        CHECK_EQ(n, 28);
    }
    CHECK_EQ(sendall(sv[0], m, 16 + 5, 0), 0);
    CHECK_EQ(shutdown(sv[0], SHUT_WR), 0);
    wire = 0;
    errno = 0;
    payload = (uint8_t *)1;
    CHECK_EQ(recv_msg(sv[1], &h, &payload, &wire), NET_ERR);
    CHECK_EQ(errno, ECONNRESET);
    CHECK_EQ(wire, 21);                            /* failed partial read counted */
    CHECK(payload == NULL);                        /* buffer freed */
    close(sv[0]);
    close(sv[1]);

    /* Bad headers: NET_PROTO, and nothing beyond the header is read. */
    {
        static const struct { int byte; uint8_t value; uint8_t type; uint64_t len; } bad[] = {
            { 0, 0x00, MSG_GET, 4 },               /* bad magic */
            { 2, 0x02, MSG_GET, 4 },               /* bad version */
            { -1, 0, 0x33, 4 },                    /* unknown type */
            { -1, 0, MSG_GET, 65537 },             /* over the cap: no malloc */
            { -1, 0, MSG_MANIFEST, 64ULL * 1024 * 1024 + 1 },
        };

        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            uint8_t rest[8];

            CHECK_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
            hdr_encode(m, bad[i].type, bad[i].len);
            if (bad[i].byte >= 0)
                m[bad[i].byte] = bad[i].value;
            memcpy(m + 16, "GET x", 5);
            CHECK_EQ(sendall(sv[0], m, 21, 0), 0);
            wire = 0;
            payload = (uint8_t *)1;
            CHECK_EQ(recv_msg(sv[1], &h, &payload, &wire), NET_PROTO);
            CHECK_EQ(wire, 16);
            CHECK(payload == NULL);
            CHECK_EQ(shutdown(sv[0], SHUT_WR), 0);
            CHECK_EQ(recvall(sv[1], rest, 5, NULL), NET_OK);   /* still unread */
            close(sv[0]);
            close(sv[1]);
        }
    }

    /* Receive error (bad descriptor): NET_ERR with recv()'s errno. */
    errno = 0;
    CHECK_EQ(recv_msg(-1, &h, &payload, NULL), NET_ERR);
    CHECK_EQ(errno, EBADF);

    /* send_error() -> recv_msg() -> dec_error(); errno is left alone. */
    CHECK_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    errno = 4242;
    send_error(sv[0], FDS_ERR_NOT_FOUND, "no such topic: x");
    CHECK_EQ(errno, 4242);
    wire = 0;
    CHECK_EQ(recv_msg(sv[1], &h, &payload, &wire), NET_OK);
    CHECK_EQ(h.type, MSG_ERROR);
    CHECK_EQ(wire, 16 + 4 + 16);
    {
        m_error_t e;

        CHECK_EQ(dec_error(payload, (size_t)h.payload_len, &e), 0);
        CHECK_EQ(e.code, 404);
        CHECK_STR(e.msg, "no such topic: x");
    }
    free(payload);

    /* send_encoded() refuses what cannot be a message (e.g. an encoder's 0). */
    errno = 0;
    CHECK_EQ(send_encoded(sv[0], m, 0, 0), -1);
    CHECK_EQ(errno, EINVAL);
    CHECK_EQ(send_encoded(sv[0], m, FDS_HDR_LEN - 1, 0), -1);
    CHECK_EQ(send_encoded(sv[0], NULL, 20, 0), -1);
    close(sv[0]);
    close(sv[1]);
}

/* ---- Time-outs -------------------------------------------------------------- */

static void test_timeouts(int lfd, uint16_t port)
{
    struct timeval tv;
    socklen_t len = sizeof(tv);
    uint8_t b[16];
    size_t got = 99;
    uint64_t wire = 0;
    uint8_t *payload;
    msg_hdr_t h;
    int64_t t0;
    double el;
    int c, s;

    CHECK_EQ(tcp_pair(lfd, port, &c, &s), 0);
    CHECK_EQ(set_timeouts(c, 1, 2), 0);
    CHECK_EQ(getsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, &len), 0);
    CHECK(tv.tv_sec == 1 && tv.tv_usec == 0);
    CHECK_EQ(getsockopt(c, SOL_SOCKET, SO_SNDTIMEO, &tv, &len), 0);
    CHECK(tv.tv_sec == 2 && tv.tv_usec == 0);

    /* Idle peer: recvall gives up after ~1 s with EAGAIN/EWOULDBLOCK. */
    t0 = now_ns();
    errno = 0;
    CHECK_EQ(recvall(c, b, 1, &got), NET_ERR);
    el = secs_since(t0);
    CHECK(errno == EAGAIN || errno == EWOULDBLOCK);
    CHECK_EQ(got, 0);
    CHECK(el >= 0.9 && el < 2.5);

    /* The same through recv_msg. */
    t0 = now_ns();
    errno = 0;
    CHECK_EQ(recv_msg(c, &h, &payload, &wire), NET_ERR);
    el = secs_since(t0);
    CHECK(errno == EAGAIN || errno == EWOULDBLOCK);
    CHECK_EQ(wire, 0);
    CHECK(el >= 0.9 && el < 2.5);

    /* 0 and negative values mean "no time-out". */
    CHECK_EQ(set_timeouts(c, 0, 0), 0);
    CHECK_EQ(getsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, &len), 0);
    CHECK(tv.tv_sec == 0 && tv.tv_usec == 0);
    CHECK_EQ(getsockopt(c, SOL_SOCKET, SO_SNDTIMEO, &tv, &len), 0);
    CHECK(tv.tv_sec == 0 && tv.tv_usec == 0);
    CHECK_EQ(set_timeouts(c, -5, -1), 0);
    CHECK_EQ(getsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, &len), 0);
    CHECK(tv.tv_sec == 0 && tv.tv_usec == 0);
    CHECK_EQ(set_timeouts(-1, 1, 1), -1);

    /* SO_SNDTIMEO: a peer that never reads fills both socket buffers; then
     * sendall() fails with EAGAIN instead of blocking forever. */
    {
        const size_t big = 64u * 1024u * 1024u;
        uint8_t *junk = calloc(1, big);

        CHECK_EQ(set_timeouts(c, 0, 1), 0);
        t0 = now_ns();
        errno = 0;
        CHECK_EQ(sendall(c, junk, big, 0), -1);
        el = secs_since(t0);
        CHECK(errno == EAGAIN || errno == EWOULDBLOCK);
        CHECK(el >= 0.9 && el < 5.0);
        free(junk);
    }
    close(c);
    close(s);
}

/* ---- SIGPIPE ------------------------------------------------------------------ */

static void test_no_sigpipe(int lfd, uint16_t port)
{
    struct sigaction sa;
    uint8_t b[1024], m[64];
    size_t n;
    int c, s, rc = 0;

    /* SIGPIPE must still have its default action (terminate): if sendall()
     * forgot MSG_NOSIGNAL, this test program would die right here. */
    CHECK_EQ(sigaction(SIGPIPE, NULL, &sa), 0);
    CHECK(sa.sa_handler == SIG_DFL);
    memset(b, 'z', sizeof(b));

    /* Writing after our own shutdown(SHUT_WR): EPIPE at once. */
    CHECK_EQ(tcp_pair(lfd, port, &c, &s), 0);
    CHECK_EQ(shutdown(c, SHUT_WR), 0);
    errno = 0;
    CHECK_EQ(sendall(c, b, 1, 0), -1);
    CHECK_EQ(errno, EPIPE);
    close(c);
    close(s);

    /* Peer closed: the first write is accepted locally, the peer answers it
     * with RST, and from then on writes fail with EPIPE. */
    CHECK_EQ(tcp_pair(lfd, port, &c, &s), 0);
    close(s);
    CHECK_EQ(recvall(c, b, 1, NULL), NET_EOF);    /* FIN has arrived */
    for (int i = 0; i < 200 && rc == 0; i++) {
        rc = sendall(c, b, sizeof(b), 0);
        if (rc == 0)
            sleep_ms(5);
    }
    CHECK_EQ(rc, -1);
    CHECK_EQ(errno, EPIPE);
    n = enc_file_end(m, sizeof(m), 1);
    CHECK_EQ(send_encoded(c, m, n, 1), -1);       /* with MSG_MORE as well */
    CHECK_EQ(errno, EPIPE);
    errno = 4242;
    send_error(c, FDS_ERR_IO, "ignored");          /* best effort, no crash */
    CHECK_EQ(errno, 4242);
    close(c);
}

/* ---- Options and small helpers ------------------------------------------------ */

static void test_options(int lfd, uint16_t port)
{
    uint8_t b[4];
    size_t got = 9;
    int c, s;
    int64_t t0;

    CHECK_EQ(tcp_pair(lfd, port, &c, &s), 0);
    CHECK_EQ(sockopt_int(c, IPPROTO_TCP, TCP_NODELAY), 0);
    CHECK_EQ(set_nodelay(c), 0);
    CHECK(sockopt_int(c, IPPROTO_TCP, TCP_NODELAY) > 0);

    CHECK_EQ(set_nonblocking(c, 1), 0);
    CHECK(fcntl(c, F_GETFL) & O_NONBLOCK);
    t0 = now_ns();
    errno = 0;
    CHECK_EQ(recvall(c, b, 1, &got), NET_ERR);    /* returns, does not spin */
    CHECK(errno == EAGAIN || errno == EWOULDBLOCK);
    CHECK_EQ(got, 0);
    CHECK(secs_since(t0) < 0.5);
    CHECK_EQ(set_nonblocking(c, 0), 0);
    CHECK((fcntl(c, F_GETFL) & O_NONBLOCK) == 0);
    CHECK_EQ(set_nonblocking(c, 0), 0);            /* idempotent */

    CHECK_EQ(set_nodelay(-1), -1);
    CHECK_EQ(set_nonblocking(-1, 1), -1);
    close(c);
    close(s);
}

static void test_env_bufsizes(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int snd0, rcv0, v;

    CHECK(fd >= 0);
    snd0 = sockopt_int(fd, SOL_SOCKET, SO_SNDBUF);
    rcv0 = sockopt_int(fd, SOL_SOCKET, SO_RCVBUF);

    /* Unset, zero, negative or garbage: nothing is changed. */
    unsetenv("FDS_SNDBUF");
    unsetenv("FDS_RCVBUF");
    apply_env_bufsizes(fd);
    CHECK_EQ(sockopt_int(fd, SOL_SOCKET, SO_SNDBUF), snd0);
    CHECK_EQ(sockopt_int(fd, SOL_SOCKET, SO_RCVBUF), rcv0);
    setenv("FDS_SNDBUF", "0", 1);
    setenv("FDS_RCVBUF", "-4096", 1);
    apply_env_bufsizes(fd);
    CHECK_EQ(sockopt_int(fd, SOL_SOCKET, SO_SNDBUF), snd0);
    CHECK_EQ(sockopt_int(fd, SOL_SOCKET, SO_RCVBUF), rcv0);
    setenv("FDS_SNDBUF", "64k", 1);
    setenv("FDS_RCVBUF", "", 1);
    apply_env_bufsizes(fd);
    CHECK_EQ(sockopt_int(fd, SOL_SOCKET, SO_SNDBUF), snd0);
    CHECK_EQ(sockopt_int(fd, SOL_SOCKET, SO_RCVBUF), rcv0);

    /* Set: Linux stores (and reports) twice the requested size. */
    setenv("FDS_SNDBUF", "65536", 1);
    setenv("FDS_RCVBUF", "8192", 1);
    apply_env_bufsizes(fd);
    v = sockopt_int(fd, SOL_SOCKET, SO_SNDBUF);
    CHECK(v >= 65536);
    v = sockopt_int(fd, SOL_SOCKET, SO_RCVBUF);
    CHECK(v >= 8192 && v <= 16384);
    CHECK(v != rcv0);
    close(fd);

    /* setsockopt failure: a warning line, no crash. */
    apply_env_bufsizes(-1);
    unsetenv("FDS_SNDBUF");
    unsetenv("FDS_RCVBUF");
}

static void test_shutdown_close(int lfd, uint16_t port)
{
    uint8_t b[4];
    int c, s;

    errno = 4242;
    shutdown_close(-1);                            /* no-op */
    CHECK_EQ(errno, 4242);

    CHECK_EQ(tcp_pair(lfd, port, &c, &s), 0);
    errno = 4242;
    shutdown_close(s);
    CHECK_EQ(errno, 4242);                         /* caller's errno kept */
    errno = 0;
    CHECK_EQ(fcntl(s, F_GETFD), -1);               /* really closed */
    CHECK_EQ(errno, EBADF);
    CHECK_EQ(recvall(c, b, 1, NULL), NET_EOF);    /* peer sees an orderly FIN */
    close(c);
}

static void test_peer_to_str(void)
{
    char s[64];
    int p[2], sv[2], fd;

    strcpy(s, "junk");
    peer_to_str(-1, s, sizeof(s));
    CHECK_STR(s, "?");
    CHECK_EQ(pipe(p), 0);
    peer_to_str(p[0], s, sizeof(s));               /* not a socket */
    CHECK_STR(s, "?");
    close(p[0]);
    close(p[1]);
    CHECK_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    peer_to_str(sv[0], s, sizeof(s));              /* not IPv4 */
    CHECK_STR(s, "?");
    close(sv[0]);
    close(sv[1]);
    fd = socket(AF_INET, SOCK_STREAM, 0);
    peer_to_str(fd, s, sizeof(s));                 /* not connected */
    CHECK_STR(s, "?");
    close(fd);
    peer_to_str(-1, s, 0);                         /* cap 0: nothing written */
    CHECK_STR(s, "?");
}

int main(void)
{
    uint16_t port = 0;
    int lfd = listen_random(&port);

    CHECK(lfd >= 0);
    if (lfd < 0)
        UNIT_DONE();
    test_listen_connect(lfd, port);
    test_connect_refused();
    test_connect_timeout();
    test_resolve_fail();
    test_big_transfer(lfd, port);
    test_recvall_eof(lfd, port);
    test_recv_msg();
    test_timeouts(lfd, port);
    test_no_sigpipe(lfd, port);
    test_options(lfd, port);
    test_env_bufsizes();
    test_shutdown_close(lfd, port);
    test_peer_to_str();
    close(lfd);
    UNIT_DONE();
}
