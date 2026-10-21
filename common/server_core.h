/*
 * server_core.h - the part of the server that is identical in every case
 * (design section 8.3, deviation 14): listening socket, accept loop,
 * single-threaded vs thread-per-client scheduling, the 256-thread cap,
 * the signal thread and graceful shutdown, and the per-connection CSV row.
 *
 * Each case's server.c only parses its arguments, fills a server_cfg_t and
 * provides serve_client(conn_t *). Everything a connection needs travels in
 * its conn_t.
 *
 * Thread model:
 *   main thread:   tcp_listen() -> loop { accept() -> make conn_t ->
 *                  single mode: run it inline; multi mode: detached pthread }
 *   signal thread: sigwait(SIGINT/SIGTERM) -> stop accepting (shutdown() of
 *                  the listening socket wakes accept()) -> wait up to
 *                  FDS_SHUTDOWN_GRACE_S for active connections -> log_close()
 *                  -> exit(0)
 *   SIGPIPE is ignored; all sends use MSG_NOSIGNAL anyway.
 */
#ifndef FDS_SERVER_CORE_H
#define FDS_SERVER_CORE_H

#include <stdint.h>
#include "fds.h"
#include "topic_map.h"

struct conn;

/*
 * Fault hook, called by send_file_range() before every 64 KB chunk.
 * Returns 1 to inject a fault: the caller sends *prefix (< chunk_len) bytes of
 * the chunk, then shutdown()+close(). Returns 0 to send the chunk normally.
 * Part I leaves conn_t.fault_hook NULL (no fault code is linked).
 */
typedef int (*fault_hook_fn)(void *fault_state, size_t chunk_len, size_t *prefix);

/*
 * Progress hook, called by send_file_range() with the file index, the file
 * offset handed to the socket so far and the number of data bytes just sent:
 *   - once right after FILE_HDR is sent, with new_off = start offset and n = 0
 *     (so the session knows the current file even before its first chunk,
 *     e.g. for a 0-byte file or a fault before the first chunk);
 *   - after every chunk, and after a fault prefix (n may be 0).
 * Case 2 / Enhanced use it to update the session table. NULL = none.
 */
typedef void (*progress_hook_fn)(struct conn *c, uint32_t file_index, uint64_t new_off, uint64_t n);

typedef struct conn {
    /* set by server_core */
    int              fd;              /* -1 once closed (conn_abort) */
    uint64_t         conn_id;         /* server-wide counter, 1, 2, 3, ... */
    char             peer[48];
    topic_map_t     *map;
    const struct server_cfg *cfg;
    int64_t          start_ns, start_wall_ns;

    /* set by serve_client(); read by server_core for the CSV row */
    const char      *kind;            /* "get", "resume", "manifest", "fetch" (default "-") */
    const char      *status;          /* default "OK"; see log_conn_row_t.status */
    char             topic[FDS_TOPIC_MAX + 1];      /* canonical key once known */
    uint8_t          session_id[FDS_SESSION_ID_LEN];
    int              have_session;
    int              k;               /* Enhanced: connections announced in MANIFEST_REQ */

    /* counters, updated by transfer.c and by protocol sends in serve_client() */
    uint64_t         data_bytes;      /* file data bytes handed to the socket */
    uint64_t         wire_bytes;      /* all bytes sent (data + headers + control) */
    uint64_t         chunks;          /* complete or partial data chunks sent */
    uint32_t         files_completed; /* FILE_END messages sent */
    int              faulted;         /* 1 after an injected fault */

    /* hooks, set by serve_client() (NULL = off) */
    fault_hook_fn    fault_hook;
    void            *fault_state;
    progress_hook_fn progress_hook;
    void            *user;            /* per-case data (e.g. session pointer) */
} conn_t;

typedef struct server_cfg {
    uint16_t    port;
    const char *root;                 /* topics_root_dir as given */
    int         multi;                /* 1 = thread per client, 0 = sequential */
    double      p;                    /* failure probability; -1 in Part I */
    int         have_seed;
    long        seed;
    const char *case_name;            /* "part1", "case1", "case2", "enhanced" */
    int       (*serve_client)(conn_t *c);   /* per-case handler; return value ignored */
} server_cfg_t;

/*
 * Load the topic map, start listening, run until SIGINT/SIGTERM.
 * Calls log_init() itself (role "server"). Returns the process exit code
 * (FDS_EXIT_FATAL if the map cannot be loaded or the port cannot be bound).
 */
int server_run(const server_cfg_t *cfg);

/* shutdown(SHUT_RDWR) + close() the connection's socket once; fd becomes -1. */
void conn_abort(conn_t *c);

/*
 * Send an encoded control message on c, adding its length to c->wire_bytes.
 * Returns 0 or -1.
 */
int conn_send(conn_t *c, const uint8_t *buf, size_t len, int more);

/* Send ERROR(code, msg) on c (best effort) and set c->status to "ERR_<code>". */
void conn_send_error(conn_t *c, uint16_t code, const char *msg);

/* 1 once shutdown has started (serve loops may check it between files). */
int server_stopping(void);

/* Print usage + exit(FDS_EXIT_FATAL) helpers live in each server.c. */

#endif /* FDS_SERVER_CORE_H */
