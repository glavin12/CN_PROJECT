/*
 * enh_client.h - state shared by the two Enhanced client engines
 * (design sections 3.8, 6.1, 6.2; deviations 11 and 13).
 *
 *   ./client <ip> <port> <topic> <output_dir> [connections=4] [max_retries=20] [io=threads|poll]
 *
 * Flow (both engines):
 *   1. enh_fetch_manifest(): one short connection, MANIFEST_REQ(topic, K) ->
 *      MANIFEST(session id, file list). Saved in output_dir/<topic>/.session
 *      (session id, topic, k). If a .session for this topic already exists, its
 *      session id is sent in later FETCHes (the server validates it); the
 *      manifest itself is always fetched again.
 *   2. enh_build_queue(): for every file i of the manifest
 *        completed <name> with the manifest size and a .session existed -> done
 *        <name>.part exists and a .session existed -> queue (i, part size)
 *        otherwise                                 -> queue (i, 0)
 *      Without a previous .session everything is fetched from 0 (stale files
 *      from an unrelated earlier run are overwritten, never trusted).
 *   3. Engine: K connections; each repeatedly takes the next item and sends
 *      FETCH(session id, i, off, topic, name), receives FILE_HDR + bytes +
 *      FILE_END into <name>.part and renames it. One FETCH outstanding per
 *      connection (never pipelined: deviation 13).
 *        threads (client.c):      K worker threads, blocking sockets, workq_pop()
 *        poll    (client_poll.c): 1 thread, K O_NONBLOCK sockets, poll(), an
 *                                 incremental parser per socket, workq_trypop()
 *   4. Failure of one connection: re-queue (i, current .part size), back off
 *      (backoff_ms(streak)), reconnect; the other K-1 connections continue.
 *      Progress (any new byte stored or file completed) resets that
 *      connection's streak; a connection whose streak reaches max_retries
 *      aborts the run (exit 2).
 *      RESUME_REJECT reason 4/5: delete the .part, re-queue from 0.
 *      RESUME_REJECT reason 1/2/3 (topic changed on the server): abort, remove
 *      partials and .session, and start over from step 1 (counts as an attempt
 *      without progress).
 *      MSG_ERROR 404 / 400 / 500: fatal (exit 1). 503: retry like a failure.
 *   5. Done when workq_outstanding() == 0: ckpt_clear(), print a summary, write
 *      the CSV row (log_client), exit 0.
 */
#ifndef FDS_ENH_CLIENT_H
#define FDS_ENH_CLIENT_H

#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include "fds.h"
#include "protocol.h"
#include "workq.h"

typedef struct enh {
    /* configuration (from argv) */
    const char *host;
    uint16_t    port;
    const char *topic;               /* as typed */
    char        dir[PATH_MAX];       /* output_dir/<topic>, created */
    int         k;                   /* 1..FDS_ENH_MAX_K */
    int         max_retries;
    int         use_poll;            /* 0 = threads, 1 = poll */

    /* manifest */
    uint8_t      session_id[FDS_SESSION_ID_LEN];
    m_manifest_t man;
    int          had_session;        /* a .session for this topic existed at start */

    /* per-file state and the queue */
    uint8_t     *done;               /* done[i] = 1 once file i is complete on disk */
    workq_t     *q;

    /* statistics and shared flags, protected by mu */
    pthread_mutex_t mu;
    uint64_t    data_bytes;          /* file bytes received */
    uint64_t    wire_bytes;          /* all bytes received */
    uint32_t    files_done;
    uint32_t    attempts;            /* connections opened (incl. manifest) */
    uint32_t    reconnects;          /* connections opened after a failure */
    uint32_t    rejects;
    int         fatal;               /* 1 = fatal error -> exit 1 */
    int         gave_up;             /* 1 = max_retries reached -> exit 2 */
    int         restart;             /* 1 = reject 1-3: start over from the manifest */
} enh_t;

/* ---- implemented in client.c ---------------------------------------------- */

/* Step 1 (with retries). Returns 0, or an exit code (FDS_EXIT_FATAL / FDS_EXIT_GAVE_UP). */
int  enh_fetch_manifest(enh_t *e);

/* Step 2. Allocates done[] and q. Returns 0 or -1. */
int  enh_build_queue(enh_t *e);

/* Open one worker connection (TCP_NODELAY, FDS_CLI_TIMEOUT_S time-outs, env
 * buffer sizes) and count it in attempts/reconnects. Returns fd or -1. */
int  enh_connect(enh_t *e, int is_reconnect);

/* Mark file idx complete (done[], files_done) under mu. */
void enh_file_completed(enh_t *e, uint32_t idx);

/* Add received byte counts under mu. */
void enh_add_bytes(enh_t *e, uint64_t data, uint64_t wire);

/* Current size of <name>.part for file idx (0 if absent): the re-queue offset. */
uint64_t enh_part_offset(enh_t *e, uint32_t idx);

/* Threads engine. Returns the process exit code. */
int  enh_run_threads(enh_t *e);

/* ---- implemented in client_poll.c ------------------------------------------ */

/* Non-blocking poll() engine. Returns the process exit code. */
int  enh_run_poll(enh_t *e);

#endif /* FDS_ENH_CLIENT_H */
