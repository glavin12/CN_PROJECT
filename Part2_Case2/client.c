/*
 * client.c - Part II Case 2 client: resumable topic download with a
 * client-side checkpoint (session management).
 *
 * IT305 Socket Programming Project, Group 006 + 043.
 *
 *   ./client <server_ip> <server_port> <topic> <output_dir> [max_retries]
 *
 * The files of <topic> are written to output_dir/<topic exactly as typed>/
 * (deviation 4). max_retries (default 20) is the number of consecutive
 * attempts WITHOUT progress after which the client gives up with exit status
 * 2. An attempt that stores any new byte or reaches a later file resets the
 * count (design 4.4), so a transfer that keeps moving forward is never
 * abandoned, whatever the fault rate. Exit status: 0 done, 1 fatal error
 * (usage, unknown topic, protocol or local file error), 2 gave up.
 *
 * Session protocol, client side (design section 5.1):
 *   phase 1  send GET <topic>; TOPIC_BEGIN(session id, file_count,
 *            total_bytes) arrives -> save .session; then for every file
 *            FILE_HDR (-> save .session) / raw bytes / FILE_END
 *   phase 2  TOPIC_END -> verify the totals, delete .session, exit 0
 *   phase 3  connection lost (EOF, reset, 15 s time-out) -> keep .session and
 *            the .part, back off (50 ms doubling to 1 s), reconnect
 *   phase 4  send RESUME(session id, file_index, offset, topic, name);
 *            RESUME_OK(file_index, offset) echoes the resume point, then
 *            FILE_HDR(start_offset = offset), the rest of the file, the later
 *            files and TOPIC_END
 *   phase 5  RESUME_REJECT(reason) -> delete the partial data and the
 *            checkpoint and go straight back to phase 1 with GET
 *
 * Checkpoint (design 5.4-5.6, checkpoint.h). The client is authoritative,
 * because only it knows which bytes reached its disk; the server validates.
 *   <name>.part  data of the file being received (rx mode RX_PART). Every
 *                received byte is written before the next recv(), so the
 *                size of the .part IS the byte offset of the checkpoint.
 *                FILE_END renames it to <name>.
 *   .session     session id, topic, index and name of the file being
 *                received. Rewritten atomically (tmp file + rename) on
 *                TOPIC_BEGIN and on every FILE_HDR, before the file's first
 *                byte is written, so it always names the file that the .part
 *                belongs to. An in-memory copy (client_t.ck) is kept in sync.
 * The offset is never stored: it is stat(<name>.part).st_size at the moment
 * the RESUME is built. The checkpoint therefore cannot disagree with the
 * data, it costs no I/O per chunk, and because both pieces are on disk a
 * download resumes even after the client process itself was killed
 * (kill -9) and started again with the same arguments.
 *
 * Accounting: ONE rx_ctx_t is kept for the whole run, so its counters
 * (data_bytes, wire_bytes) accumulate over every connection, and
 * rx.stored_bytes accumulates the sizes of the files completed since the last
 * TOPIC_BEGIN, over all reconnections. A GET restarts the topic at file 0, so
 * TOPIC_BEGIN resets stored_bytes. See uncount_completed_file() for the one
 * resume that would otherwise count a file twice.
 */
#include "fds.h"
#include "checkpoint.h"
#include "log.h"
#include "net.h"
#include "protocol.h"
#include "transfer.h"
#include "util.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Largest request we send: RESUME = 16 header + 16 id + 4 + 8 + 1 + 63 topic
 * + 2 + 255 name = 365 bytes. */
#define REQ_BUF 512

_Static_assert(sizeof(((ckpt_t *)0)->file_name) == sizeof(((m_file_hdr_t *)0)->name),
               "checkpoint and FILE_HDR name fields must have the same size");
_Static_assert(sizeof(((ckpt_t *)0)->file_name) == sizeof(((m_resume_t *)0)->name),
               "checkpoint and RESUME name fields must have the same size");

/* Outcome of one connection attempt, as the retry loop sees it. */
typedef enum {
    ATT_DONE,       /* topic complete and verified */
    ATT_RETRY,      /* lost / refused / server busy: back off and try again */
    ATT_REJECTED,   /* checkpoint rejected and dropped: GET again at once */
    ATT_FATAL       /* unrecoverable: exit 1 */
} attempt_rc_t;

typedef struct client {
    /* command line */
    const char *host;
    uint16_t    port;
    const char *topic;              /* as typed: directory name, GET and RESUME */
    const char *out_dir;
    long        max_retries;
    char        dir[PATH_MAX];      /* output_dir/<topic> */

    /* in-memory copy of dir/.session; meaningful only when have_ckpt */
    int         have_ckpt;
    ckpt_t      ck;
    uint64_t    ck_file_size;       /* size announced by the FILE_HDR of ck.file_index */

    /* TOPIC_BEGIN of the current download, if THIS process received it
     * (not the case after a restart that went straight to RESUME) */
    int             saw_begin;
    m_topic_begin_t begin;

    rx_ctx_t      rx;               /* one receive context for the whole run */
    m_topic_end_t end;              /* TOPIC_END, valid after ATT_DONE */
    const char   *why;              /* what went wrong in the last attempt */

    /* statistics for the CSV row and the summary line */
    uint32_t    attempts;           /* connections opened */
    uint32_t    rejects;            /* RESUME_REJECT received */
    int64_t     start_ns, start_wall_ns;
} client_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "usage: %s <server_ip> <server_port> <topic> <output_dir> [max_retries]\n"
            "  max_retries  consecutive attempts without progress before giving up "
            "(1..1000000, default %d)\n",
            prog, FDS_CASE2_MAX_RETRIES);
}

/* ---- Arguments and output directory ----------------------------------------- */

static int parse_args(int argc, char **argv, client_t *cl)
{
    cl->max_retries = FDS_CASE2_MAX_RETRIES;
    if (argc < 5 || argc > 6)
        return -1;
    cl->host = argv[1];
    if (cl->host[0] == '\0') {
        fprintf(stderr, "server_ip must not be empty\n");
        return -1;
    }
    if (parse_port(argv[2], &cl->port) != 0) {
        fprintf(stderr, "invalid port '%s' (expected 1..65535)\n", argv[2]);
        return -1;
    }
    cl->topic = argv[3];
    if (!topic_is_valid(cl->topic)) {
        fprintf(stderr, "invalid topic '%s' (1..%u bytes, no '/', not '.' or '..')\n",
                cl->topic, FDS_TOPIC_MAX);
        return -1;
    }
    cl->out_dir = argv[4];
    if (cl->out_dir[0] == '\0') {
        fprintf(stderr, "output_dir must not be empty\n");
        return -1;
    }
    if (argc == 6 && parse_long(argv[5], 1, 1000000, &cl->max_retries) != 0) {
        fprintf(stderr, "invalid max_retries '%s' (expected 1..1000000)\n", argv[5]);
        return -1;
    }
    return 0;
}

/* output_dir/<topic as typed>, created with mkdir -p (deviation 4). */
static int prepare_dir(client_t *cl)
{
    if (path_join(cl->dir, sizeof(cl->dir), cl->out_dir, cl->topic) != 0) {
        log_warn("output path too long: %s/%s", cl->out_dir, cl->topic);
        return -1;
    }
    if (mkdir_p(cl->dir, 0755) != 0) {
        log_warn("cannot create %s: %s", cl->dir, strerror(errno));
        return -1;
    }
    return 0;
}

/* ---- Checkpoint --------------------------------------------------------------- */

/* Write .session atomically and mirror it in memory. */
static int save_checkpoint(client_t *cl, const ckpt_t *ck)
{
    if (ckpt_save(cl->dir, ck) != 0) {
        log_warn("cannot write checkpoint in %s: %s", cl->dir, strerror(errno));
        return -1;
    }
    cl->ck = *ck;
    cl->have_ckpt = 1;
    return 0;
}

/*
 * Forget the current download: delete every .part and the checkpoint files,
 * and reset what was counted for it. Used at a fresh start and after
 * RESUME_REJECT (design 5.1 phase 5). Completed files stay; the new download
 * overwrites them. A failure is not fatal: the next TOPIC_BEGIN replaces
 * .session anyway, and recv_file() truncates a .part before reusing it.
 */
static void discard_download(client_t *cl)
{
    if (remove_partials(cl->dir) != 0)
        log_warn("cannot remove partial files in %s: %s", cl->dir, strerror(errno));
    cl->have_ckpt       = 0;
    cl->saw_begin       = 0;
    cl->rx.next_index   = 0;
    cl->rx.stored_bytes = 0;
}

/*
 * At start-up: resume from dir/.session if it describes a Case 2 download of
 * this very topic (also after the previous client process was killed).
 * Otherwise start fresh. A corrupt record, one for another topic, or one
 * written by the Enhanced client (k != 0) is not ours to trust, and any
 * .part next to it has no checkpoint that vouches for it.
 */
static void load_checkpoint(client_t *cl)
{
    ckpt_t ck;
    int    rc = ckpt_load(cl->dir, &ck);

    if (rc == 0 && strcmp(ck.topic, cl->topic) == 0 && ck.k == 0) {
        cl->ck = ck;
        cl->have_ckpt = 1;
        if (ck.file_index >= 0)
            log_info("found checkpoint: file %" PRId64 " '%s'; resuming", ck.file_index,
                     ck.file_name);
        return;
    }
    if (rc < 0)
        log_warn("ignoring unreadable checkpoint in %s; starting fresh", cl->dir);
    else if (rc == 0)
        log_info("checkpoint in %s belongs to another download; starting fresh", cl->dir);
    discard_download(cl);
}

/* RESUME needs a file to resume: a record saved on TOPIC_BEGIN alone
 * (file_index -1) only has a session id, so that case starts with GET. */
static int can_resume(const client_t *cl)
{
    return cl->have_ckpt && cl->ck.file_index >= 0 && cl->ck.file_name[0] != '\0';
}

/*
 * Byte offset of the checkpoint, i.e. how much of file ck.file_index is stored:
 *   <name>.part exists -> its size (the normal case);
 *   else <name> exists -> its size: the file was completed and renamed, but
 *                         the next FILE_HDR never arrived (*complete = 1);
 *   else               -> 0: stopped between saving .session and creating
 *                         the .part, so nothing of the file is stored.
 */
static uint64_t checkpoint_offset(const client_t *cl, int *complete)
{
    uint64_t size;

    *complete = 0;
    if (part_size(cl->dir, cl->ck.file_name, &size) == 0)
        return size;
    if (final_size(cl->dir, cl->ck.file_name, &size) == 0) {
        *complete = 1;
        return size;
    }
    return 0;
}

/*
 * rx.on_file_hdr: called for every accepted FILE_HDR before the first byte of
 * the file is written. The record must name this file before its .part gets
 * any data, otherwise a crash could leave a .part that no checkpoint covers.
 * When resuming the file the record already names, nothing changes and the
 * write is skipped.
 */
static int on_file_hdr(rx_ctx_t *rx, const m_file_hdr_t *h)
{
    client_t *cl = rx->user;
    ckpt_t    ck;

    if (!cl->have_ckpt)
        return -1;      /* cannot happen: TOPIC_BEGIN or a loaded record comes first */
    cl->ck_file_size = h->file_size;
    if (cl->ck.file_index == (int64_t)h->file_index && strcmp(cl->ck.file_name, h->name) == 0)
        return 0;
    ck = cl->ck;
    ck.file_index = h->file_index;
    memcpy(ck.file_name, h->name, sizeof(ck.file_name));
    return save_checkpoint(cl, &ck);
}

/* ---- One connection ------------------------------------------------------------- */

static void close_socket(int fd)
{
    if (close(fd) != 0 && errno != EINTR)
        log_warn("close: %s", strerror(errno));
}

/* Connect and set the socket up as every Part II client does: 15 s time-outs
 * (a silent server counts as a lost connection), TCP_NODELAY (deviation 10)
 * and the optional buffer sizes of the buffer experiment. Returns fd or -1. */
static int open_connection(client_t *cl)
{
    int fd = tcp_connect(cl->host, cl->port, FDS_CONNECT_TIMEOUT_S);

    if (fd < 0) {
        log_info("cannot connect to %s:%u: %s", cl->host, (unsigned)cl->port, strerror(errno));
        cl->why = "connect failed";
        return -1;
    }
    cl->attempts++;
    if (set_timeouts(fd, FDS_CLI_TIMEOUT_S, FDS_CLI_TIMEOUT_S) != 0) {
        log_warn("cannot set socket time-outs: %s", strerror(errno));
        close_socket(fd);
        cl->why = "socket set-up failed";
        return -1;
    }
    if (set_nodelay(fd) != 0)       /* only a performance option: carry on */
        log_warn("cannot set TCP_NODELAY: %s", strerror(errno));
    apply_env_bufsizes(fd);
    return fd;
}

/* Phase 1: GET <topic>, then TOPIC_BEGIN. The new session id is saved before
 * any file arrives, so even a cut inside the first file can be resumed. */
static rx_rc_t start_with_get(client_t *cl, int fd)
{
    uint8_t         buf[REQ_BUF];
    uint8_t        *payload = NULL;
    size_t          n, len = 0;
    m_topic_begin_t b;
    ckpt_t          ck;
    rx_rc_t         rc;

    n = enc_get(buf, sizeof(buf), cl->topic);
    if (n == 0)
        return RX_PROTO;            /* cannot happen: the topic was validated */
    if (send_encoded(fd, buf, n, 0) != 0)
        return RX_LOST;
    rc = recv_expect(&cl->rx, MSG_TOPIC_BEGIN, &payload, &len);
    if (rc == RX_OK && dec_topic_begin(payload, len, &b) != 0)
        rc = RX_PROTO;
    free(payload);
    if (rc != RX_OK)
        return rc;

    memset(&ck, 0, sizeof(ck));
    memcpy(ck.session_id, b.session_id, sizeof(ck.session_id));
    memcpy(ck.topic, cl->topic, strlen(cl->topic) + 1);
    ck.file_index = -1;             /* no FILE_HDR yet */
    if (save_checkpoint(cl, &ck) != 0)
        return RX_IOERR;

    /* The topic (re)starts at file 0: count completed files from scratch. */
    cl->saw_begin       = 1;
    cl->begin           = b;
    cl->rx.next_index   = 0;
    cl->rx.stored_bytes = 0;
    log_info("session started: %" PRIu32 " files, %" PRIu64 " bytes", b.file_count,
             b.total_bytes);
    return RX_OK;
}

/*
 * rx.stored_bytes grows by a file's size on its FILE_END. When the checkpoint
 * file was already completed by this process and the next FILE_HDR never
 * arrived, the resume asks for it at offset == size, and the server answers
 * with FILE_HDR + FILE_END and no data. That second FILE_END would count the
 * file twice, so its size is taken off here first.
 * Only needed when this process saw TOPIC_BEGIN (the only case in which the
 * total is checked): then every FILE_HDR since then, including the one that
 * named the checkpoint file, was received here and ck_file_size is its size.
 */
static void uncount_completed_file(client_t *cl)
{
    if (cl->saw_begin && cl->rx.stored_bytes >= cl->ck_file_size)
        cl->rx.stored_bytes -= cl->ck_file_size;
}

/* Phase 4: RESUME from the checkpoint and check that RESUME_OK echoes it.
 * RX_REJECT is passed up to the caller (phase 5). */
static rx_rc_t start_with_resume(client_t *cl, int fd)
{
    uint8_t       buf[REQ_BUF];
    uint8_t      *payload = NULL;
    size_t        n, len = 0;
    m_resume_t    m;
    m_resume_ok_t ok;
    int           complete;
    rx_rc_t       rc;

    memset(&m, 0, sizeof(m));
    memcpy(m.session_id, cl->ck.session_id, sizeof(m.session_id));
    m.file_index = (uint32_t)cl->ck.file_index;
    m.offset     = checkpoint_offset(cl, &complete);
    memcpy(m.topic, cl->topic, strlen(cl->topic) + 1);
    memcpy(m.name, cl->ck.file_name, sizeof(m.name));

    n = enc_resume(buf, sizeof(buf), MSG_RESUME, &m);
    if (n == 0)
        return RX_PROTO;            /* cannot happen: the record was validated */
    if (send_encoded(fd, buf, n, 0) != 0)
        return RX_LOST;
    rc = recv_expect(&cl->rx, MSG_RESUME_OK, &payload, &len);
    if (rc == RX_OK && dec_resume_ok(payload, len, &ok) != 0)
        rc = RX_PROTO;
    free(payload);
    if (rc != RX_OK)
        return rc;
    if (ok.file_index != m.file_index || ok.offset != m.offset) {
        log_warn("RESUME_OK names file %" PRIu32 " offset %" PRIu64
                 ", but the checkpoint is file %" PRIu32 " offset %" PRIu64,
                 ok.file_index, ok.offset, m.file_index, m.offset);
        return RX_PROTO;
    }

    if (complete)
        uncount_completed_file(cl);
    cl->rx.next_index = m.file_index;   /* recv_topic checks the order from here */
    return RX_OK;
}

/* Phase 2: TOPIC_END arrived. Check it against what was received, then
 * remove .session so the directory holds only the topic's files. */
static attempt_rc_t finish_topic(client_t *cl, const m_topic_end_t *end)
{
    if (end->file_count != cl->rx.next_index) {
        log_warn("TOPIC_END announces %" PRIu32 " files, but %" PRIu32 " were received",
                 end->file_count, cl->rx.next_index);
        return ATT_FATAL;
    }
    /* After a restart we never saw TOPIC_BEGIN and the files before the
     * checkpoint were counted by the previous process, so the byte total
     * can only be checked when this process received the whole topic. */
    if (cl->saw_begin &&
        (end->file_count != cl->begin.file_count || end->total_bytes != cl->begin.total_bytes ||
         cl->rx.stored_bytes != end->total_bytes)) {
        log_warn("totals do not match: TOPIC_BEGIN %" PRIu32 " files/%" PRIu64
                 " bytes, TOPIC_END %" PRIu32 "/%" PRIu64 ", stored %" PRIu64 " bytes",
                 cl->begin.file_count, cl->begin.total_bytes, end->file_count,
                 end->total_bytes, cl->rx.stored_bytes);
        return ATT_FATAL;
    }
    if (ckpt_clear(cl->dir) != 0) {
        log_warn("cannot remove the checkpoint in %s: %s", cl->dir, strerror(errno));
        return ATT_FATAL;
    }
    cl->have_ckpt = 0;
    cl->end = *end;
    return ATT_DONE;
}

/* Phase 5: the server cannot resume from our checkpoint (design 5.5). */
static void handle_reject(client_t *cl)
{
    log_info("server rejected the checkpoint (file %" PRId64 " '%s'): %s; starting over",
             cl->ck.file_index, cl->ck.file_name, reject_reason_str(cl->rx.reject_reason));
    cl->rejects++;
    discard_download(cl);
}

/* Map a failed receive to what the retry loop should do. */
static attempt_rc_t failure_outcome(client_t *cl, rx_rc_t rc)
{
    switch (rc) {
    case RX_LOST:
        cl->why = "connection lost";
        return ATT_RETRY;
    case RX_SERVER_ERR:
        if (cl->rx.err.code == FDS_ERR_BUSY) {
            log_info("server busy (%u %s)", (unsigned)cl->rx.err.code, cl->rx.err.msg);
            cl->why = "server busy";
            return ATT_RETRY;
        }
        log_warn("server error %u: %s", (unsigned)cl->rx.err.code, cl->rx.err.msg);
        return ATT_FATAL;
    case RX_PROTO:
        log_warn("protocol error: unexpected or malformed message from the server");
        return ATT_FATAL;
    case RX_IOERR:
        log_warn("local file error while writing into %s", cl->dir);
        return ATT_FATAL;
    case RX_REJECT:
        log_warn("protocol error: RESUME_REJECT outside a resume");
        return ATT_FATAL;
    case RX_OK:
    default:
        break;
    }
    log_warn("internal error: unexpected receive status %d", (int)rc);
    return ATT_FATAL;
}

/* One connection: RESUME if there is a file to resume, GET otherwise, then
 * receive files until TOPIC_END or until the connection fails. */
static attempt_rc_t run_attempt(client_t *cl)
{
    m_topic_end_t end;
    attempt_rc_t  out;
    rx_rc_t       rc;
    int           fd, resuming;

    fd = open_connection(cl);
    if (fd < 0)
        return ATT_RETRY;
    cl->rx.fd = fd;
    resuming  = can_resume(cl);
    rc = resuming ? start_with_resume(cl, fd) : start_with_get(cl, fd);
    if (rc == RX_REJECT && resuming) {
        handle_reject(cl);
        out = ATT_REJECTED;
    } else if (rc != RX_OK) {
        out = failure_outcome(cl, rc);
    } else {
        rc  = recv_topic(&cl->rx, &end);
        out = rc == RX_OK ? finish_topic(cl, &end) : failure_outcome(cl, rc);
    }
    close_socket(fd);
    cl->rx.fd = -1;
    return out;
}

/* ---- Retry loop ----------------------------------------------------------------- */

/*
 * Progress = the checkpoint moved forward: new file bytes were stored, or a
 * FILE_HDR of a later file arrived (which also covers 0-byte files). A resume
 * that only re-confirms a file that was already complete is not progress, so
 * the loop stays bounded even if every connection died at that point.
 */
typedef struct {
    int64_t  file_index;            /* -1 = no file yet */
    uint64_t data_bytes;
} mark_t;

static mark_t progress_mark(const client_t *cl)
{
    mark_t m;

    m.file_index = cl->have_ckpt ? cl->ck.file_index : -1;
    m.data_bytes = cl->rx.data_bytes;
    return m;
}

static int made_progress(const client_t *cl, const mark_t *before)
{
    mark_t now = progress_mark(cl);

    return now.data_bytes > before->data_bytes || now.file_index > before->file_index;
}

/* "connection lost at file 57 offset 65536; resuming in 50 ms" */
static void report_retry(const client_t *cl, long wait_ms)
{
    uint64_t off;
    int      complete;

    if (fds_quiet())
        return;
    if (!can_resume(cl)) {
        log_info("%s before the first file; retrying in %ld ms", cl->why, wait_ms);
        return;
    }
    off = checkpoint_offset(cl, &complete);
    log_info("%s at file %" PRId64 " offset %" PRIu64 "; resuming in %ld ms", cl->why,
             cl->ck.file_index, off, wait_ms);
}

/*
 * Design 4.4: max_retries counts only CONSECUTIVE attempts without progress.
 * Any progress resets the streak, so a long transfer under a high fault rate
 * still finishes, while a dead server or a hopeless fault rate stops after a
 * bounded number of attempts. A reject also counts towards the streak (it
 * stores nothing), but the GET that follows it is sent at once, without
 * back-off: the server answered, it is not overloaded.
 */
static int run_transfer(client_t *cl)
{
    int streak = 0;

    for (;;) {
        mark_t       before = progress_mark(cl);
        attempt_rc_t rc     = run_attempt(cl);
        long         wait_ms;

        if (rc == ATT_DONE)
            return FDS_EXIT_OK;
        if (rc == ATT_FATAL)
            return FDS_EXIT_FATAL;
        if (rc == ATT_RETRY && made_progress(cl, &before))
            streak = 0;
        else
            streak++;
        if (streak >= cl->max_retries) {
            log_warn("giving up after %d attempts without progress", streak);
            return FDS_EXIT_GAVE_UP;
        }
        if (rc == ATT_REJECTED)
            continue;               /* design 5.1 phase 5: straight back to GET */
        wait_ms = backoff_ms(streak > 0 ? streak : 1);
        report_retry(cl, wait_ms);
        sleep_ms(wait_ms);
    }
}

/* ---- Reporting -------------------------------------------------------------------- */

/* Files of the topic known to be complete on disk: every file before the
 * checkpoint file, or what this process received in order. */
static uint32_t files_stored(const client_t *cl)
{
    uint32_t n = cl->rx.next_index;

    if (cl->have_ckpt && cl->ck.file_index > (int64_t)n)
        n = (uint32_t)cl->ck.file_index;
    return n;
}

/* One CSV row (only if FDS_LOG is set) and, on success, one summary line on
 * stdout. */
static void report(const client_t *cl, int code)
{
    log_client_row_t row;
    int64_t          end_ns = now_ns();
    int              ok = code == FDS_EXIT_OK;

    memset(&row, 0, sizeof(row));
    row.topic         = cl->topic;
    row.k             = 1;
    row.io            = "-";
    row.start_ns      = cl->start_ns;
    row.end_ns        = end_ns;
    row.start_wall_ns = cl->start_wall_ns;
    row.end_wall_ns   = wall_ns();
    row.useful_bytes  = ok ? cl->end.total_bytes : 0;
    row.data_bytes_rx = cl->rx.data_bytes;
    row.wire_bytes_rx = cl->rx.wire_bytes;
    row.files         = ok ? cl->end.file_count : files_stored(cl);
    row.attempts      = cl->attempts;
    row.reconnects    = cl->attempts > 0 ? cl->attempts - 1 : 0;
    row.rejects       = cl->rejects;
    row.status        = ok ? "OK" : code == FDS_EXIT_GAVE_UP ? "DNF" : "FAIL";
    log_client(&row);

    if (ok) {
        double secs = (double)(end_ns - cl->start_ns) / 1e9;
        double mbps = secs > 0 ? (double)cl->end.total_bytes / secs / 1e6 : 0.0;

        printf("OK topic=%s files=%" PRIu32 " bytes=%" PRIu64 " time=%.3fs rate=%.2fMB/s"
               " attempts=%" PRIu32 " reconnects=%" PRIu32 " rejects=%" PRIu32 "\n",
               cl->topic, cl->end.file_count, cl->end.total_bytes, secs, mbps,
               row.attempts, row.reconnects, row.rejects);
        fflush(stdout);
    }
}

int main(int argc, char **argv)
{
    client_t    cl;                 /* the whole run's state; single-threaded */
    const char *prog = argc > 0 ? argv[0] : "client";
    log_ctx_t   ctx = { "client", "case2", "-", -1.0, 0, 0 };
    int         code;

    memset(&cl, 0, sizeof(cl));
    if (parse_args(argc, argv, &cl) != 0) {
        usage(prog);
        return FDS_EXIT_FATAL;
    }
    log_init(&ctx);
    cl.start_ns      = now_ns();
    cl.start_wall_ns = wall_ns();
    cl.why           = "connection lost";
    cl.rx.fd          = -1;
    cl.rx.dir         = cl.dir;
    cl.rx.mode        = RX_PART;
    cl.rx.on_file_hdr = on_file_hdr;
    cl.rx.user        = &cl;

    if (prepare_dir(&cl) != 0) {
        code = FDS_EXIT_FATAL;
    } else {
        load_checkpoint(&cl);
        code = run_transfer(&cl);
    }
    report(&cl, code);
    log_close();
    return code;
}
