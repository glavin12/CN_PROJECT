/*
 * transfer.h - file streaming (design sections 3.5-3.8, 6.3).
 *
 * Server side streams with sendfile() (zero-copy, explicit offset; read()+
 * sendall() fallback if sendfile is unsupported), 64 KB chunks, FILE_HDR sent
 * with MSG_MORE, fault hook before every chunk, progress hook after.
 *
 * Client side receives into a 64 KB buffer and writes straight to the output
 * file. Two storage modes:
 *   RX_DIRECT : write <dir>/<name> with O_TRUNC (Part I, Case 1).
 *   RX_PART   : write <dir>/<name>.part, rename to <name> on FILE_END
 *               (Case 2, Enhanced). The .part size IS the checkpoint offset:
 *               every received byte is written before the next recv().
 */
#ifndef FDS_TRANSFER_H
#define FDS_TRANSFER_H

#include <stdint.h>
#include "fds.h"
#include "protocol.h"
#include "server_core.h"
#include "topic_map.h"

/* ---- Server side -------------------------------------------------------------- */

typedef enum {
    XFER_OK       = 0,   /* everything requested was sent */
    XFER_FAULT    = 1,   /* fault injected; connection already closed (conn_abort) */
    XFER_PEER     = 2,   /* send failed / timed out: peer gone */
    XFER_IOERR    = 3    /* file could not be opened/read (ERROR 500 already sent) */
} xfer_rc_t;

/*
 * Send file t->files[idx] from byte `off`: FILE_HDR(idx, size, off, name),
 * the bytes [off, size) in chunks, then FILE_END(idx).
 * Before each chunk: c->fault_hook (if set). c->progress_hook (if set) is
 * called after FILE_HDR (n = 0) and after each chunk / fault prefix.
 * On a fault: sends the prefix, log_event("FAULT", ...), c->faulted = 1,
 * c->status = "FAULT", conn_abort(c). Updates all conn_t counters.
 * Requires off <= size (the caller validated it).
 */
xfer_rc_t send_file_range(conn_t *c, const topic_t *t, uint32_t idx, uint64_t off);

/*
 * Stream a topic: optionally TOPIC_BEGIN(session_id, n_files, total_bytes)
 * (send_begin != 0), then send_file_range(from_idx, from_off) followed by every
 * later file from offset 0, then TOPIC_END(n_files, total_bytes).
 * Returns the first non-OK code, or XFER_OK.
 */
xfer_rc_t serve_topic(conn_t *c, const topic_t *t, const uint8_t session_id[FDS_SESSION_ID_LEN],
                      uint32_t from_idx, uint64_t from_off, int send_begin);

/* ---- Client side -------------------------------------------------------------- */

typedef enum { RX_DIRECT = 0, RX_PART = 1 } rx_mode_t;

typedef enum {
    RX_OK = 0,          /* message(s) fully received and stored */
    RX_LOST,            /* connection lost (EOF, reset, time-out): retry may help */
    RX_PROTO,           /* server sent something invalid: fatal */
    RX_IOERR,           /* local file error: fatal */
    RX_SERVER_ERR,      /* server sent MSG_ERROR (see rx_ctx_t.err) */
    RX_REJECT           /* server sent RESUME_REJECT (see rx_ctx_t.reject_reason) */
} rx_rc_t;

typedef struct rx_ctx {
    int         fd;
    const char *dir;              /* output_dir/<topic as typed> (already created) */
    rx_mode_t   mode;

    /* counters (cumulative; the caller may keep one rx_ctx across reconnects) */
    uint64_t    data_bytes;       /* file bytes received and written */
    uint64_t    wire_bytes;       /* every byte received */
    uint32_t    files_done;       /* FILE_END processed */

    /* expected next file index (for the order check); recv_topic sets/advances it */
    uint32_t    next_index;
    uint64_t    stored_bytes;     /* sum of sizes of files completed in order */

    /*
     * Called right after a FILE_HDR is accepted, before any data byte is
     * written (Case 2 saves its .session checkpoint here). NULL = none.
     * Returning non-zero aborts with RX_IOERR.
     */
    int       (*on_file_hdr)(struct rx_ctx *rx, const m_file_hdr_t *h);
    void       *user;

    /* filled on RX_SERVER_ERR / RX_REJECT */
    m_error_t   err;
    uint16_t    reject_reason;
} rx_ctx_t;

/*
 * Receive the raw bytes of one file whose FILE_HDR was already decoded, then
 * its FILE_END. RX_DIRECT: open <name> O_TRUNC (start_offset must be 0).
 * RX_PART: start_offset 0 -> create/truncate <name>.part; start_offset > 0 ->
 * <name>.part must exist and is truncated to start_offset before appending;
 * start_offset == file_size and <name> already complete with that size ->
 * nothing to write. On FILE_END: fsync is NOT done (speed), the file is closed
 * and (RX_PART) renamed to <name>. Partial data stays on disk on RX_LOST.
 */
rx_rc_t recv_file(rx_ctx_t *rx, const m_file_hdr_t *h);

/*
 * After TOPIC_BEGIN (or RESUME_OK): loop over FILE_HDR -> recv_file until
 * TOPIC_END, checking that file indices arrive in order starting at
 * rx->next_index. On RX_OK *end is filled; the caller compares it with the
 * TOPIC_BEGIN totals. MSG_ERROR -> RX_SERVER_ERR.
 */
rx_rc_t recv_topic(rx_ctx_t *rx, m_topic_end_t *end);

/*
 * Receive one message that must be of type `want` (or MSG_ERROR /
 * MSG_RESUME_REJECT, which map to RX_SERVER_ERR / RX_REJECT). The payload is
 * returned malloc'd in *payload (caller frees), its length in *len.
 */
rx_rc_t recv_expect(rx_ctx_t *rx, uint8_t want, uint8_t **payload, size_t *len);

#endif /* FDS_TRANSFER_H */
