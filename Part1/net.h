/*
 * net.h - TCP socket helpers: full-length I/O, listen/connect, socket options.
 *
 * Every write in the project goes through sendall() (MSG_NOSIGNAL, so a dead
 * peer gives EPIPE instead of killing the process) and every fixed-length read
 * through recvall(). Both restart on EINTR.
 */
#ifndef FDS_NET_H
#define FDS_NET_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

/* Return codes of recvall() / recv_msg() (negative = not OK). */
#define NET_OK       0
#define NET_ERR     -1   /* error; errno set (EAGAIN/EWOULDBLOCK = SO_RCVTIMEO time-out) */
#define NET_EOF     -2   /* peer closed the connection before n bytes arrived */
#define NET_PROTO   -3   /* protocol violation (bad magic/version/length/payload) */

/*
 * Send exactly n bytes. flags are OR-ed with MSG_NOSIGNAL (pass MSG_MORE to let
 * the kernel coalesce a header with the data that follows).
 * Returns 0 on success, -1 on error (errno set).
 */
int sendall(int fd, const void *buf, size_t n, int flags);

/*
 * Receive exactly n bytes. Returns NET_OK, NET_EOF or NET_ERR.
 * If got != NULL, *got is set to the number of bytes actually received
 * (useful when the stream ends in the middle).
 */
int recvall(int fd, void *buf, size_t n, size_t *got);

/*
 * Listening IPv4 TCP socket on INADDR_ANY:port with SO_REUSEADDR and
 * backlog SOMAXCONN. Returns fd or -1 (message printed to stderr).
 */
int tcp_listen(uint16_t port);

/*
 * Connect to host:port. host may be a dotted IPv4 address or a host name
 * (getaddrinfo, AF_INET). The connect itself is bounded by timeout_sec
 * (non-blocking connect + poll); the returned socket is blocking again.
 * Returns fd or -1 (errno set; ECONNREFUSED, ETIMEDOUT, ...).
 */
int tcp_connect(const char *host, uint16_t port, int timeout_sec);

/* SO_RCVTIMEO / SO_SNDTIMEO in seconds; 0 leaves that direction blocking forever. */
int set_timeouts(int fd, int rcv_sec, int snd_sec);

/* TCP_NODELAY on (deviation 10). */
int set_nodelay(int fd);

/* O_NONBLOCK on/off. */
int set_nonblocking(int fd, int on);

/*
 * Apply SO_SNDBUF / SO_RCVBUF from the environment variables FDS_SNDBUF and
 * FDS_RCVBUF (bytes) if they are set; used only by the buffer-size experiment.
 */
void apply_env_bufsizes(int fd);

/* Abortive-free close used for fault injection: shutdown(SHUT_RDWR) then close(). */
void shutdown_close(int fd);

/* "a.b.c.d:port" of the peer into out (cap >= 32). */
void peer_to_str(int fd, char *out, size_t cap);

#endif /* FDS_NET_H */
