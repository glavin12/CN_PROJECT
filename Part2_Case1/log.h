/*
 * log.h - thread-safe human-readable logging and raw CSV performance logs
 * (design sections 6.4 and 7, with deviation 8: no hard-coded log path).
 *
 * CSV files are written only when the user asks for them:
 *   FDS_LOG=<file.csv>      one row per connection (server) / per run (client)
 *   FDS_EVENTS=<file.csv>   server only: one row per FAULT / RESUME / FETCH event
 *   FDS_RUN_ID=<text>       copied into every row (default "-")
 *   FDS_CLIENT_ID=<text>    client only (default: the pid)
 * Several processes may append to the same CSV: each row is one write() on an
 * O_APPEND descriptor under flock(), and the header is written only if the
 * file is empty.
 *
 * Server connection CSV columns:
 *   run_id,case,mode,p,seed,conn_id,kind,session_id,topic,k,peer,
 *   start_ns,end_ns,start_wall_ns,data_bytes,wire_bytes,chunks,files_completed,faulted,status
 * Server event CSV columns:
 *   run_id,case,ts_ns,event,conn_id,session_id,topic,file_index,offset,chunk_index,conn_data_bytes,detail
 * Client CSV columns:
 *   run_id,case,host,client_id,pid,topic,k,io,start_ns,end_ns,start_wall_ns,end_wall_ns,
 *   useful_bytes,data_bytes_rx,wire_bytes_rx,files,attempts,reconnects,rejects,status
 *
 * *_ns are CLOCK_MONOTONIC (comparable between processes of one machine);
 * *_wall_ns are CLOCK_REALTIME.
 */
#ifndef FDS_LOG_H
#define FDS_LOG_H

#include <stdint.h>

typedef struct {
    const char *role;       /* "server" or "client" (prefix of stderr lines) */
    const char *case_name;  /* "part1", "case1", "case2", "enhanced" */
    const char *mode;       /* server: "single"/"multi"; client: "threads"/"poll"/"-" */
    double      p;          /* failure probability (-1 for Part I) */
    int         have_seed;
    long        seed;
} log_ctx_t;

/* Call once at start-up (before any thread is created). */
void log_init(const log_ctx_t *ctx);

/* Flush and close the CSV files (graceful shutdown / exit). Safe to call twice. */
void log_close(void);

/* stderr line "[role] ..." unless FDS_QUIET is set. Thread-safe. */
void log_info(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

/* stderr line "[role] WARNING: ..." always. Thread-safe. */
void log_warn(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

/* One server connection (written when the connection ends). */
typedef struct {
    uint64_t       conn_id;
    const char    *kind;          /* "get", "resume", "manifest", "fetch", "-" */
    const uint8_t *session_id;    /* 16 bytes or NULL */
    const char    *topic;         /* canonical topic key or "" */
    int            k;             /* parallel connections announced by the client (0 = n/a) */
    const char    *peer;          /* "a.b.c.d:port" */
    int64_t        start_ns, end_ns, start_wall_ns;
    uint64_t       data_bytes;    /* file payload bytes handed to the socket */
    uint64_t       wire_bytes;    /* data_bytes + every header/control byte sent */
    uint64_t       chunks;
    uint32_t       files_completed;
    int            faulted;       /* 1 if a fault was injected on this connection */
    const char    *status;        /* "OK", "FAULT", "PEER_LOST", "ERR_400", "ERR_404", "ERR_500", "BUSY", "REJECT" */
} log_conn_row_t;
void log_conn(const log_conn_row_t *r);

/* One server event (FAULT, RESUME_OK, RESUME_REJECT, FETCH_REJECT, SESSION_NEW, ...). */
void log_event(const char *event, uint64_t conn_id, const uint8_t *session_id,
               const char *topic, int64_t file_index, uint64_t offset,
               uint64_t chunk_index, uint64_t conn_data_bytes, const char *detail);

/* One client run (written once, just before the client exits). */
typedef struct {
    const char *topic;            /* topic as typed on the command line */
    int         k;                /* parallel connections (1 for non-Enhanced) */
    const char *io;               /* "threads", "poll" or "-" */
    int64_t     start_ns, end_ns, start_wall_ns, end_wall_ns;
    uint64_t    useful_bytes;     /* bytes of the topic stored (= topic size on success) */
    uint64_t    data_bytes_rx;    /* file bytes received, including re-received ones */
    uint64_t    wire_bytes_rx;    /* every byte received */
    uint32_t    files;            /* files stored */
    uint32_t    attempts;         /* connection tries, failed connects included
                                     (Enhanced: manifest + every worker try) */
    uint32_t    reconnects;       /* tries made after a failure. Case 1/2: attempts - 1.
                                     Enhanced: can be lower than the number of faults,
                                     because a re-queued file is often taken by
                                     another connection that is still open */
    uint32_t    rejects;          /* RESUME_REJECT received */
    const char *status;           /* "OK", "DNF" (gave up) or "FAIL" (fatal error) */
} log_client_row_t;
void log_client(const log_client_row_t *r);

#endif /* FDS_LOG_H */
