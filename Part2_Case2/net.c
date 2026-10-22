/*
 * net.c - TCP socket helpers (see net.h).
 *
 * This is the lowest layer of the project: everything that touches a socket
 * goes through these functions, so the tricky POSIX details live in exactly
 * one place:
 *
 *   - send()/recv() may transfer fewer bytes than asked ("short" I/O), and may
 *     be interrupted by a signal (EINTR). sendall()/recvall() loop until the
 *     whole buffer is done, so the callers can think in whole messages.
 *   - Writing to a socket whose peer has gone away raises SIGPIPE, whose
 *     default action kills the process. A fault-tolerant server and a retrying
 *     client must survive that, so every send uses MSG_NOSIGNAL and gets EPIPE
 *     as an ordinary error instead.
 *   - A blocking connect() to an unreachable host can hang for minutes (the
 *     kernel retries the SYN). tcp_connect() uses a non-blocking connect plus
 *     poll() so the client's retry loop gets control back after timeout_sec.
 *
 * Depends only on util (env_long, now_ns) and log (log_warn).
 */
#include "net.h"
#include "log.h"
#include "util.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

/* ---- Full-length I/O ------------------------------------------------------ */

int sendall(int fd, const void *buf, size_t n, int flags)
{
    const uint8_t *p = buf;
    size_t left = n;

    while (left > 0) {
        ssize_t r = send(fd, p, left, flags | MSG_NOSIGNAL);

        if (r < 0) {
            /* A signal arrived before anything was sent: just try again. */
            if (errno == EINTR)
                continue;
            /* Everything else is a real failure for the caller, including
             * EAGAIN/EWOULDBLOCK, which on a blocking socket means that
             * SO_SNDTIMEO expired because the peer stopped reading. */
            return -1;
        }
        if (r == 0) {
            /* send() never legitimately returns 0 for left > 0. Looping would
             * spin forever, so report it as a broken connection. */
            errno = EPIPE;
            return -1;
        }
        p += r;
        left -= (size_t)r;
    }
    return 0;
}

int recvall(int fd, void *buf, size_t n, size_t *got)
{
    uint8_t *p = buf;
    size_t done = 0;
    int rc = NET_OK;

    while (done < n) {
        ssize_t r = recv(fd, p + done, n - done, 0);

        if (r < 0) {
            if (errno == EINTR)
                continue;
            /* EAGAIN/EWOULDBLOCK here means SO_RCVTIMEO expired; the caller
             * sees it in errno and treats it as a lost connection. */
            rc = NET_ERR;
            break;
        }
        if (r == 0) {
            /* Orderly shutdown by the peer before n bytes arrived. This is
             * kept apart from NET_ERR because "closed between messages" is
             * normal, while "closed inside a message" is a failure. */
            rc = NET_EOF;
            break;
        }
        done += (size_t)r;
    }
    /* Reported on every path: callers count wire bytes and need to know how
     * much of a header arrived before the stream ended. Assigning *got does
     * not touch errno, so the error code from recv() survives. */
    if (got != NULL)
        *got = done;
    return rc;
}

/* ---- Listening and connecting ------------------------------------------- */

int tcp_listen(uint16_t port)
{
    struct sockaddr_in addr;
    int one = 1, saved;
    /* SOCK_CLOEXEC: the descriptor must not leak into any child process. */
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

    if (fd < 0)
        goto fail;
    /* SO_REUSEADDR lets the server restart at once on the same port even
     * while old connections are still in TIME_WAIT (common in experiments). */
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0)
        goto fail;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);   /* every local interface */
    addr.sin_port = htons(port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0)
        goto fail;
    /* SOMAXCONN (deviation 16): many clients start at the same moment in the
     * experiments, and a short backlog would make the kernel drop their SYNs. */
    if (listen(fd, SOMAXCONN) != 0)
        goto fail;
    return fd;

fail:
    saved = errno;                      /* close() and fprintf() may change it */
    fprintf(stderr, "[server] cannot listen on port %u: %s\n",
            (unsigned)port, strerror(saved));
    if (fd >= 0)
        close(fd);
    errno = saved;
    return -1;
}

/*
 * Wait until a non-blocking connect() started on fd has finished, for at most
 * timeout_sec seconds (<= 0: no limit). Returns 0 when connected, -1 with
 * errno set otherwise.
 */
static int wait_connected(int fd, int timeout_sec)
{
    struct pollfd pfd = { .fd = fd, .events = POLLOUT, .revents = 0 };
    int64_t deadline = now_ns() + (int64_t)timeout_sec * 1000000000LL;
    int err = 0;
    socklen_t len = sizeof(err);

    for (;;) {
        int wait_ms = -1, r;

        if (timeout_sec > 0) {
            int64_t left_ns = deadline - now_ns();

            if (left_ns <= 0) {
                errno = ETIMEDOUT;
                return -1;
            }
            /* Round up: rounding down would turn the last fraction of a
             * millisecond into poll(0) calls that spin. */
            int64_t ms = (left_ns + 999999) / 1000000;
            wait_ms = ms > INT_MAX ? INT_MAX : (int)ms;
        }
        r = poll(&pfd, 1, wait_ms);
        if (r > 0)
            break;
        if (r == 0) {
            errno = ETIMEDOUT;
            return -1;
        }
        /* EINTR: loop. The deadline is absolute (CLOCK_MONOTONIC), so the
         * remaining time is recomputed and signals cannot extend the wait. */
        if (errno != EINTR)
            return -1;
    }
    /* Writability only says the attempt is over; SO_ERROR says how it ended
     * (0 = connected, else ECONNREFUSED, EHOSTUNREACH, ...). */
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0)
        return -1;
    if (err != 0) {
        errno = err;
        return -1;
    }
    return 0;
}

/* Try one resolved address. Returns a connected blocking fd or -1 (errno set). */
static int connect_one(const struct addrinfo *ai, int timeout_sec)
{
    int saved;
    int fd = socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol);

    if (fd < 0)
        return -1;
    /* Non-blocking only for the connect, so that poll() can bound it. */
    if (set_nonblocking(fd, 1) != 0)
        goto fail;
    if (connect(fd, ai->ai_addr, ai->ai_addrlen) != 0) {
        /* EINPROGRESS is the normal answer of a non-blocking connect. If a
         * signal interrupts connect() (EINTR), POSIX says the connection still
         * proceeds asynchronously, so it is waited for in the same way. */
        if (errno != EINPROGRESS && errno != EINTR)
            goto fail;
        if (wait_connected(fd, timeout_sec) != 0)
            goto fail;
    }
    /* The rest of the code relies on blocking I/O with SO_RCVTIMEO. */
    if (set_nonblocking(fd, 0) != 0)
        goto fail;
    return fd;

fail:
    saved = errno;
    close(fd);
    errno = saved;
    return -1;
}

int tcp_connect(const char *host, uint16_t port, int timeout_sec)
{
    struct addrinfo hints, *res = NULL;
    char port_str[8];
    int rc, last_err = EHOSTUNREACH;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;          /* the project is IPv4-only */
    hints.ai_socktype = SOCK_STREAM;
    /* The port is already a number: never look it up in /etc/services. */
    hints.ai_flags = AI_NUMERICSERV;
    snprintf(port_str, sizeof(port_str), "%u", (unsigned)port);

    /* getaddrinfo (deviation 16) accepts a dotted IPv4 address or a host name. */
    rc = getaddrinfo(host, port_str, &hints, &res);
    if (rc != 0) {
        log_warn("cannot resolve host '%s': %s", host,
                 rc == EAI_SYSTEM ? strerror(errno) : gai_strerror(rc));
        /* The gai error codes are not errno values; EHOSTUNREACH is the
         * closest errno so callers can still use strerror(errno). */
        errno = EHOSTUNREACH;
        return -1;
    }
    /* A name may resolve to several addresses: use the first that works. */
    for (const struct addrinfo *ai = res; ai != NULL; ai = ai->ai_next) {
        int fd = connect_one(ai, timeout_sec);

        if (fd >= 0) {
            freeaddrinfo(res);
            return fd;
        }
        last_err = errno;
    }
    freeaddrinfo(res);
    errno = last_err;                   /* freeaddrinfo() may have changed it */
    return -1;
}

/* ---- Socket options ------------------------------------------------------- */

int set_timeouts(int fd, int rcv_sec, int snd_sec)
{
    /* {0, 0} is the kernel's "no time-out"; a negative value means the same. */
    struct timeval rcv = { .tv_sec = rcv_sec > 0 ? rcv_sec : 0, .tv_usec = 0 };
    struct timeval snd = { .tv_sec = snd_sec > 0 ? snd_sec : 0, .tv_usec = 0 };

    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rcv, sizeof(rcv)) != 0)
        return -1;
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &snd, sizeof(snd)) != 0)
        return -1;
    return 0;
}

int set_nodelay(int fd)
{
    /* Deviation 10: without it, Nagle's algorithm holds a small request
     * (FETCH, RESUME) back until the previous segment is ACKed, and delayed
     * ACK on the other side can make that wait ~40 ms per request. */
    int one = 1;

    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

int set_nonblocking(int fd, int on)
{
    /* Read-modify-write so the other file status flags are kept. */
    int fl = fcntl(fd, F_GETFL);

    if (fl < 0)
        return -1;
    fl = on ? (fl | O_NONBLOCK) : (fl & ~O_NONBLOCK);
    return fcntl(fd, F_SETFL, fl) == 0 ? 0 : -1;
}

/* Set one buffer size from an environment variable, if it holds a value > 0. */
static void apply_one_bufsize(int fd, const char *var, int opt, const char *opt_name)
{
    long v = env_long(var, 0);
    int size;

    if (v <= 0)                         /* unset, invalid or 0: keep the default */
        return;
    size = v > INT_MAX ? INT_MAX : (int)v;
    if (setsockopt(fd, SOL_SOCKET, opt, &size, sizeof(size)) != 0)
        log_warn("setsockopt(%s=%d) failed: %s", opt_name, size, strerror(errno));
}

void apply_env_bufsizes(int fd)
{
    /* Only the buffer-size experiment sets these. Setting SO_SNDBUF/SO_RCVBUF
     * also turns off the kernel's buffer auto-tuning, which is why nothing is
     * done when the variables are absent. The kernel clamps the value to
     * net.core.wmem_max / rmem_max and reports twice the request back. */
    apply_one_bufsize(fd, "FDS_SNDBUF", SO_SNDBUF, "SO_SNDBUF");
    apply_one_bufsize(fd, "FDS_RCVBUF", SO_RCVBUF, "SO_RCVBUF");
}

void shutdown_close(int fd)
{
    int saved = errno;

    if (fd < 0)
        return;
    /* shutdown() first: it sends FIN and wakes up any thread blocked in
     * recv()/send() on this socket at once, even if another descriptor still
     * refers to it (close() alone would not). Errors are irrelevant here: the
     * socket may already be dead, and closing it is all that is wanted. */
    (void)shutdown(fd, SHUT_RDWR);
    (void)close(fd);
    /* Callers usually report the error that made them close the socket. */
    errno = saved;
}

void peer_to_str(int fd, char *out, size_t cap)
{
    struct sockaddr_storage ss;
    socklen_t len = sizeof(ss);
    char ip[INET_ADDRSTRLEN];
    const struct sockaddr_in *sin = (const struct sockaddr_in *)&ss;

    if (out == NULL || cap == 0)
        return;
    /* sockaddr_storage is large enough for any family, so a non-IPv4 socket
     * cannot be truncated; it is reported as "?" by the family check. */
    if (getpeername(fd, (struct sockaddr *)&ss, &len) != 0 ||
        ss.ss_family != AF_INET ||
        inet_ntop(AF_INET, &sin->sin_addr, ip, sizeof(ip)) == NULL) {
        snprintf(out, cap, "?");
        return;
    }
    snprintf(out, cap, "%s:%u", ip, (unsigned)ntohs(sin->sin_port));
}
