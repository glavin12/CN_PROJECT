/*
 * server.c - Part II Case 2 Enhanced: multi-threaded topic server with fault
 * injection, session management and PARALLEL TCP connections per client.
 *
 * IT305 Socket Programming Project, Group 006 + 043.
 *
 * Usage: ./server <port> <topics_root_dir> <failure_probability> [seed]
 *
 * Message exchange (design section 3.8; deviations 11 and 13):
 *
 *   client                                       server (one thread per connection)
 *   -- manifest connection (short) --
 *     MANIFEST_REQ(topic, K)   ------------->    session_create(topic, enhanced)
 *                              <-------------    MANIFEST(session id, n x (size, name))
 *     (both sides close)
 *   -- K worker connections, each one a loop --
 *     FETCH(id, #i, off, topic, name) ------>    session_validate_resume (design 5.5,
 *                                                check 5 per file: deviation 11)
 *                              <-------------    FILE_HDR #i (start_offset = off)
 *                              <-------------    bytes [off, size)  (cut here if this is the
 *                                                connection's first FETCH and it was drawn)
 *                              <-------------    FILE_END #i
 *     FETCH(id, #j, ...)       ------------->    ... the next file on the same connection
 *     (client closes when its queue is empty: EOF here is the normal end)
 *
 * A connection's first message decides its kind: MANIFEST_REQ (answered, then
 * the connection ends) or FETCH (served in a loop). Anything else is ERROR 400.
 *
 * Why each FETCH is self-contained: it repeats the session id, the topic and
 * the file name, so ANY connection (a brand-new one after a fault included)
 * can serve ANY file of the session without per-connection server state. The
 * server checks every FETCH against the session table exactly like a Case 2
 * RESUME, so a FETCH after a fault IS the resume: it starts at the client's
 * .part size and only the missing bytes are sent again.
 *
 * Why there is only ONE FETCH outstanding per connection (never pipelined,
 * deviation 13): if the client sent FETCH #j while #i was still streaming and
 * a fault then cut the connection during #i, the server would close() a
 * socket whose receive buffer still holds the unread FETCH #j. Linux answers
 * close() with unread data by sending RST instead of FIN, and an RST makes the
 * client's kernel throw away every byte it has queued but the client has not
 * read yet. The prefix of #i that this server counted as "sent" would then be
 * lost on the way, and the byte accounting (server data_bytes == bytes the
 * client stored) that the overhead measurements rely on would break. With one
 * request at a time the server has always read everything the client sent
 * before it closes, so a fault gives a clean FIN after the last byte sent.
 * The cost is one round trip per file, which the K parallel connections hide.
 *
 * Why the worker connections stay open between files: thousands of small
 * images would otherwise each pay a TCP handshake (and slow start).
 *
 * Fault injection (design section 4, deviations 9 and 28): the same rule as
 * Case 1 and Case 2, one decision per connection. Each connection has its own
 * drand48_r() generator (seeded from a hash of (seed, conn_id) with [seed],
 * deviation 20). When its first FETCH is accepted, fault_plan() decides: with
 * probability p the connection is cut at a uniformly random byte of the data
 * that FETCH will send. Only that connection ends; the client's other K-1
 * connections and the session are unaffected. A connection that is not cut
 * serves the rest of its FETCHes without faults. The server cannot know which
 * later FETCHes will arrive on the same connection, so the cut is placed in
 * the one request it does know. Each of the K connections is cut with
 * probability p, so a parallel transfer sees more cuts than a Case 2 one at
 * the same p; each costs only the bytes of one file that were in flight.
 *
 * The accept loop, thread cap (503), signal thread and CSV rows are in
 * server_core.c (deviation 14); this file only adds main() + serve_client().
 */
#include "fault.h"
#include "fds.h"
#include "log.h"
#include "net.h"
#include "protocol.h"
#include "server_core.h"
#include "session.h"
#include "topic_map.h"
#include "transfer.h"
#include "util.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* conn_t.topic receives a copy of topic_t.key: both must have the same size. */
_Static_assert(sizeof(((conn_t *)0)->topic) == sizeof(((topic_t *)0)->key),
               "conn_t.topic and topic_t.key must have the same size");

/* What the FETCH loop does after one request. */
typedef enum {
    FETCH_CONTINUE,                  /* read the next FETCH on this connection */
    FETCH_STOP                       /* the connection is finished (or broken) */
} fetch_next_t;

/* Print why the arguments were rejected, then the usage line; exit 1. */
static void usage(const char *prog, const char *why)
{
    if (why != NULL)
        fprintf(stderr, "server: %s\n", why);
    fprintf(stderr, "usage: %s <port> <topics_root_dir> <failure_probability> [seed]\n", prog);
    exit(FDS_EXIT_FATAL);
}

/*
 * progress_hook of every connection: transfer.c calls it right after each
 * FILE_HDR (n = 0) and after every chunk or fault prefix. It moves the
 * session's sent_off[file] forward, which is what check 5 of a later FETCH
 * for that file is validated against. A connection that has not accepted a
 * FETCH yet has no session to update.
 */
static void enh_progress(conn_t *c, uint32_t file_index, uint64_t new_off, uint64_t n)
{
    if (c->have_session)
        session_progress(c->session_id, file_index, new_off, n);
}

/* ERROR 404 with the key the client asked for. */
static void reply_unknown_topic(conn_t *c, const char *key)
{
    char msg[128];

    snprintf(msg, sizeof(msg), "unknown topic '%s'", key);
    conn_send_error(c, FDS_ERR_NOT_FOUND, msg);
}

/* ---- MANIFEST_REQ ------------------------------------------------------------ */

/*
 * Answer MANIFEST_REQ(topic, K) with MANIFEST(session id, file list) and end
 * the connection. The session is created here, so that every FETCH of the run
 * (on any of the K connections) is checked against one record.
 */
static void serve_manifest(conn_t *c, const uint8_t *payload, size_t len)
{
    m_manifest_req_t req;
    uint8_t sid[FDS_SESSION_ID_LEN];
    uint8_t *buf;
    size_t n = 0;
    const topic_t *t;
    char detail[32];

    if (dec_manifest_req(payload, len, &req) != 0) {
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "malformed MANIFEST_REQ");
        return;
    }
    c->kind = "manifest";
    /* The decoder leaves the range of K to us: it is policy, not syntax. */
    if (req.connections < 1 || req.connections > FDS_ENH_MAX_K) {
        char msg[64];

        snprintf(msg, sizeof(msg), "connections must be 1..%d", FDS_ENH_MAX_K);
        conn_send_error(c, FDS_ERR_BAD_REQUEST, msg);
        return;
    }
    c->k = req.connections;

    /* Exact, then case-insensitive match, then one re-scan for new folders. */
    t = topic_find_or_rescan(c->map, req.topic);
    if (t == NULL) {
        reply_unknown_topic(c, req.topic);
        return;
    }
    memcpy(c->topic, t->key, sizeof(c->topic));

    if (session_create(t, 1, sid) != 0) {
        /* Every slot holds a live transfer (deviation 12): try again later. */
        conn_send_error(c, FDS_ERR_BUSY, "session table full");
        return;
    }
    memcpy(c->session_id, sid, sizeof(c->session_id));
    c->have_session = 1;
    snprintf(detail, sizeof(detail), "k=%d", c->k);
    log_event("SESSION_NEW", c->conn_id, sid, t->key, -1, 0, 0, 0, detail);

    /* The manifest can be large (cane: about 250 KB, deviation 2), so it is
     * built in a heap buffer of the exact size. */
    buf = enc_manifest_alloc(sid, t->files, t->n_files, t->total_bytes, &n);
    if (buf == NULL) {
        conn_send_error(c, FDS_ERR_IO, "cannot build the manifest");
        session_set_state(sid, SESS_FREE);     /* nobody can ever use this id */
        return;
    }
    if (conn_send(c, buf, n, 0) != 0) {
        /*
         * Not every byte reached the socket, so the client cannot hold the
         * id; it will ask again and get a new session. Releasing this slot
         * keeps a burst of failed manifests from filling the table.
         */
        c->status = "PEER_LOST";
        session_set_state(sid, SESS_FREE);
    } else {
        /*
         * No connection is using the session until the first FETCH arrives,
         * which makes it ACTIVE again (session_validate_resume). Marking it
         * INTERRUPTED meanwhile lets the table evict it if no FETCH ever
         * comes. A client that resumes after a restart keeps its OLD id, so
         * this fresh one would otherwise sit ACTIVE, unevictable, for 300 s.
         */
        session_set_state(sid, SESS_INTERRUPTED);
    }
    free(buf);
    /* Returning ends the connection: the client opens its K workers next. */
}

/* ---- FETCH --------------------------------------------------------------------- */

/* Send RESUME_REJECT(reason). Returns 0, or -1 if the peer is gone. */
static int send_reject(conn_t *c, uint16_t reason)
{
    uint8_t buf[FDS_HDR_LEN + 2];
    size_t n = enc_resume_reject(buf, sizeof(buf), reason);

    if (n == 0 || conn_send(c, buf, n, 0) != 0) {
        c->status = "PEER_LOST";
        return -1;
    }
    return 0;
}

/*
 * The FETCH was refused by the session checks (reason 1..5). The connection
 * itself is fine, so it stays open: after reason 4/5 the client deletes its
 * .part and FETCHes the same file from 0 on this connection; after 1/2/3 it
 * closes and starts over with a new MANIFEST_REQ.
 */
static fetch_next_t reject_fetch(conn_t *c, const m_resume_t *m, const topic_t *t, int reason)
{
    char detail[64];

    snprintf(detail, sizeof(detail), "reason=%d (%s)", reason,
             reject_reason_str((uint16_t)reason));
    log_event("FETCH_REJECT", c->conn_id, m->session_id, t != NULL ? t->key : m->topic,
              m->file_index, m->offset, 0, c->data_bytes, detail);
    return send_reject(c, (uint16_t)reason) == 0 ? FETCH_CONTINUE : FETCH_STOP;
}

/*
 * Turn the result of send_file_range() into the next step. On XFER_FAULT
 * transfer.c has already logged the FAULT event, set c->faulted and
 * c->status = "FAULT" and closed the socket.
 */
static fetch_next_t after_send(conn_t *c, xfer_rc_t rc)
{
    switch (rc) {
    case XFER_OK:
        /* The last missing byte of the topic may just have gone out on this
         * connection; DONE makes the slot reusable at once (deviation 12). */
        if (session_all_sent(c->session_id))
            session_set_state(c->session_id, SESS_DONE);
        return FETCH_CONTINUE;
    case XFER_FAULT:
        break;
    case XFER_PEER:
        c->status = "PEER_LOST";        /* the client stopped reading */
        break;
    case XFER_IOERR:
        c->status = "ERR_500";          /* transfer.c already sent ERROR 500 */
        break;
    }
    /* The client will re-FETCH the file on another connection; until then
     * the slot waits as INTERRUPTED (evictable if the client never returns). */
    session_set_state(c->session_id, SESS_INTERRUPTED);
    return FETCH_STOP;
}

/*
 * Serve one FETCH(session id, file index, offset, topic, name):
 * validate it (design 5.5 with the per-file check 5 of deviation 11), then
 * send FILE_HDR + bytes [offset, size) + FILE_END.
 */
static fetch_next_t serve_one_fetch(conn_t *c, const uint8_t *payload, size_t len)
{
    m_resume_t m;
    const topic_t *t;
    int rc;

    if (dec_resume(payload, len, &m) != 0) {
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "malformed FETCH");
        return FETCH_STOP;
    }
    /* We never hand out the all-zero id; zero-filled memory looks like it. */
    if (bytes_all_zero(m.session_id, sizeof(m.session_id))) {
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "FETCH without a session id");
        return FETCH_STOP;
    }
    /*
     * A connection belongs to one session: its progress hook updates that
     * session only. Checked before validation so that a stray id cannot
     * create (adopt) a session as a side effect.
     */
    if (c->have_session && memcmp(m.session_id, c->session_id, sizeof(m.session_id)) != 0) {
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "FETCH for another session on this connection");
        return FETCH_STOP;
    }

    /* t == NULL is handled by the validation: reject reason 1. */
    t = topic_find_or_rescan(c->map, m.topic);
    rc = session_validate_resume(m.session_id, t, m.file_index, m.name, m.offset, 1);
    if (rc > 0)
        return reject_fetch(c, &m, t, rc);
    if (rc < 0) {
        /* Unknown id to adopt, but every slot is busy (deviation 12). */
        conn_send_error(c, FDS_ERR_BUSY, "session table full");
        return FETCH_STOP;
    }

    if (!c->have_session) {
        memcpy(c->session_id, m.session_id, sizeof(c->session_id));
        memcpy(c->topic, t->key, sizeof(c->topic));
        c->have_session = 1;
    }
    if (m.offset > 0) {
        /* A FETCH from the middle of a file is a resume after a fault. */
        log_event("FETCH_RESUME", c->conn_id, m.session_id, t->key, m.file_index,
                  m.offset, 0, c->data_bytes, NULL);
    }
    /* The connection's one fault decision, on its first FETCH that carries
     * data (a no-op afterwards). Checks 2 and 4 passed: index and offset are
     * in range. */
    fault_plan(c->fault_state, t->files[m.file_index].size - m.offset);
    return after_send(c, send_file_range(c, t, m.file_index, m.offset));
}

/*
 * The FETCH loop of a worker connection. `payload` is the first FETCH (read
 * by serve_first()); every later one is read here. Exactly one request is
 * outstanding at a time (see the top of this file). Every payload is freed.
 */
static void serve_fetches(conn_t *c, uint8_t *payload, size_t len)
{
    c->kind = "fetch";
    for (;;) {
        msg_hdr_t h;
        fetch_next_t next = serve_one_fetch(c, payload, len);
        int rc;

        free(payload);
        payload = NULL;
        /* Graceful shutdown: finish the file in flight, take no new one. */
        if (next == FETCH_STOP || server_stopping())
            return;

        rc = conn_recv_request(c, &h, &payload, NULL);
        if (rc == NET_EOF || rc == NET_ERR) {
            /*
             * EOF: this worker's queue is empty, the normal end (status stays
             * "OK"). NET_ERR: reset, or idle longer than the server time-out.
             * If the topic is not fully sent yet, the session may now have no
             * connection at all (a killed client, a DNF run, an adopted session
             * whose client already had some files): INTERRUPTED keeps such a
             * slot evictable instead of pinning it ACTIVE for 300 s. Another
             * live connection's next chunk makes it ACTIVE again.
             */
            if (rc == NET_ERR)
                c->status = "PEER_LOST";
            if (c->have_session && !session_all_sent(c->session_id))
                session_set_state(c->session_id, SESS_INTERRUPTED);
            return;
        }
        if (rc == NET_PROTO || h.type != MSG_FETCH) {
            free(payload);
            conn_send_error(c, FDS_ERR_BAD_REQUEST, "expected FETCH");
            return;
        }
        len = (size_t)h.payload_len;
    }
}

/* ---- serve_client ----------------------------------------------------------- */

/* Read the first message and dispatch on its type. */
static void serve_first(conn_t *c)
{
    msg_hdr_t h;
    uint8_t *payload = NULL;
    int rc = conn_recv_request(c, &h, &payload, NULL);

    if (rc == NET_EOF || rc == NET_ERR) {
        c->status = "PEER_LOST";        /* closed / reset / timed out before a request */
        return;
    }
    if (rc == NET_PROTO) {
        /* Bad magic, version, type or length (design 3.1). */
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "malformed header");
        return;
    }
    switch (h.type) {
    case MSG_MANIFEST_REQ:
        serve_manifest(c, payload, (size_t)h.payload_len);
        free(payload);
        break;
    case MSG_FETCH:
        serve_fetches(c, payload, (size_t)h.payload_len);   /* frees payload */
        break;
    default:
        free(payload);
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "expected MANIFEST_REQ or FETCH");
        break;
    }
}

/*
 * serve_client() of Case 2 Enhanced, run on the connection's own thread.
 *
 * The fault state lives on this thread's stack: it belongs to exactly one
 * connection and one thread, so its generator needs no lock, and it is gone
 * when the connection ends. The hook pointers are cleared before returning
 * so nothing can reach the dead stack frame afterwards.
 */
static int enhanced_serve(conn_t *c)
{
    fault_state_t f;

    fault_init(&f, c->cfg->p, c->cfg->have_seed, c->cfg->seed, c->conn_id);
    c->fault_hook = fault_should_fail;
    c->fault_state = &f;
    c->progress_hook = enh_progress;

    serve_first(c);

    c->fault_hook = NULL;
    c->fault_state = NULL;
    c->progress_hook = NULL;
    return 0;
}

int main(int argc, char **argv)
{
    server_cfg_t cfg;

    if (argc < 4 || argc > 5)
        usage(argv[0], NULL);

    memset(&cfg, 0, sizeof(cfg));
    if (parse_port(argv[1], &cfg.port) != 0)
        usage(argv[0], "port must be an integer in 1..65535");
    cfg.root = argv[2];
    if (parse_prob(argv[3], &cfg.p) != 0)
        usage(argv[0], "failure_probability must be a real number in [0, 1]");
    if (argc == 5) {
        /* Any long: fault_init() seeds each connection from hash(seed, conn_id). */
        if (parse_long(argv[4], LONG_MIN, LONG_MAX, &cfg.seed) != 0)
            usage(argv[0], "seed must be an integer");
        cfg.have_seed = 1;
    }

    cfg.multi = 1;                       /* Part II: always one thread per connection */
    cfg.case_name = "enhanced";
    cfg.serve_client = enhanced_serve;

    /* Empty session table before the first connection can arrive. */
    session_table_init();

    /* Loads the topic map (exit 1 if root is not a readable directory),
     * listens, and runs until SIGINT/SIGTERM. */
    return server_run(&cfg);
}
