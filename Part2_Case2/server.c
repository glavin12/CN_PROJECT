/*
 * server.c - Part II Case 2 server: fault injection + session management.
 *
 * IT305 Socket Programming Project, Group 006 + 043.
 *
 *   ./server <port> <topics_root_dir> <failure_probability> [seed]
 *
 * What lives where (deviation 14): everything that is identical in every case
 * (listening socket, one detached thread per client, the 256-thread cap,
 * graceful shutdown on SIGINT/SIGTERM, the per-connection CSV row) is in
 * server_core.c. This file only parses the arguments and provides
 * serve_client(), which runs in the worker thread of one connection. Part II
 * is always thread-per-client, so multi = 1.
 *
 * Session protocol, server side (design section 5.1):
 *   phase 1  GET <topic>        create a session with a random 16-byte id, then
 *                               TOPIC_BEGIN(id, file_count, total_bytes) and
 *                               FILE_HDR / data / FILE_END for every file
 *   phase 2  TOPIC_END sent     the session is marked DONE
 *   phase 3  fault or peer lost the session is marked INTERRUPTED; its record
 *                               (next_file, file_off_sent) stays in the table
 *   phase 4  RESUME(id, file_index, offset, topic, name)
 *                               validate it (design 5.5), RESUME_OK(index,
 *                               offset), then FILE_HDR(start_offset = offset),
 *                               only the bytes [offset, size), the later files
 *                               and TOPIC_END. No TOPIC_BEGIN: the client got it
 *                               on its first connection (design figure 5).
 *   phase 5  invalid checkpoint RESUME_REJECT(reason 1..5); the client drops its
 *                               partial data and starts again with GET.
 *
 * Who is authoritative (design 5.5 and 5.6): the CLIENT. Only the client knows
 * which bytes reached its disk (the size of its .part file), so it reports the
 * checkpoint and the server resumes exactly there. The server's record is
 * still useful: it is updated after every chunk (progress hook below), so it
 * is an upper bound on what the client can hold. A checkpoint beyond it cannot
 * be genuine and is rejected (check 5). Checks 1-4 compare the checkpoint with
 * the topic itself, so a server that lost the record (restart, eviction) can
 * still resume safely: session_validate_resume() then adopts the client's id.
 *
 * Fault injection (design section 4, deviations 9 and 28): every connection
 * has its own drand48_r() generator, seeded from a hash of (seed, conn_id)
 * (or of time, thread id and conn_id without a seed; deviation 20). Once the
 * GET or RESUME is accepted, fault_plan() takes the connection's one
 * decision: with probability p it is cut at a uniformly random byte of the
 * data still to send (the whole topic after a GET, the rest from the
 * checkpoint after a RESUME). transfer.c stops at that byte and then
 * shutdown()+close()s the connection. Control messages never get a
 * decision. The session record keeps what was really handed to the socket,
 * the bytes before the cut included, so the client's RESUME after the cut is
 * accepted.
 */
#include "fds.h"
#include "fault.h"
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

/* RESUME_OK / RESUME_REJECT: 16-byte header + at most 12 payload bytes. */
#define SMALL_MSG_BUF (FDS_HDR_LEN + 16u)

static void usage(const char *prog)
{
    fprintf(stderr,
            "usage: %s <port> <topics_root_dir> <failure_probability> [seed]\n"
            "  failure_probability  real number in [0, 1]: chance that a connection is cut\n"
            "  seed                 optional integer; makes the fault positions repeatable\n",
            prog);
    exit(FDS_EXIT_FATAL);
}

/*
 * The progress_hook of every Case 2 connection. transfer.c calls it right
 * after each FILE_HDR (n = 0) and after each chunk or fault prefix, so the
 * session record always says how far the stream has got. That record is what
 * check 5 compares a later RESUME against. Before a session exists (or after
 * a rejected RESUME) there is nothing to update.
 */
static void on_progress(conn_t *c, uint32_t file_index, uint64_t new_off, uint64_t n)
{
    if (c->have_session)
        session_progress(c->session_id, file_index, new_off, n);
}

/* Remember the canonical topic key for the CSV row (the client may have typed
 * "Cane" for the folder "cane"). */
static void set_topic(conn_t *c, const topic_t *t)
{
    snprintf(c->topic, sizeof(c->topic), "%s", t->key);
}

/* From now on this connection streams for session `id`: the progress hook
 * starts updating it and the CSV row carries its id. */
static void bind_session(conn_t *c, const uint8_t id[FDS_SESSION_ID_LEN])
{
    memcpy(c->session_id, id, FDS_SESSION_ID_LEN);
    c->have_session = 1;
}

/*
 * After serve_topic(): DONE when TOPIC_END went out (phase 2). Anything else
 * leaves the session INTERRUPTED (phase 3), which keeps the record for the
 * client's RESUME and also makes the slot evictable (deviation 12), so a
 * client that never returns cannot hold a slot for ever.
 */
static void end_session(conn_t *c, xfer_rc_t rc)
{
    if (rc == XFER_OK) {
        session_set_state(c->session_id, SESS_DONE);
        return;
    }
    if (rc == XFER_PEER)
        c->status = "PEER_LOST";
    session_set_state(c->session_id, SESS_INTERRUPTED);
}

/* File data bytes from (file_index, offset) to the end of the topic: what a
 * RESUME to that checkpoint will send. The checkpoint has passed checks 2 and
 * 4, so file_index < n_files and offset <= that file's size. */
static uint64_t bytes_from(const topic_t *t, uint32_t file_index, uint64_t offset)
{
    uint64_t n = t->files[file_index].size - offset;

    for (uint32_t i = file_index + 1; i < t->n_files; i++)
        n += t->files[i].size;
    return n;
}

/*
 * Phase 1: GET <topic>. A new session is created for every GET, even when the
 * same client had one before: a GET means the client has no usable checkpoint
 * (first run, or its checkpoint was rejected), so the old record is of no use.
 */
static void handle_get(conn_t *c, const uint8_t *payload, size_t len)
{
    char           key[FDS_TOPIC_MAX + 1];
    char           msg[FDS_TOPIC_MAX + 32];
    uint8_t        id[FDS_SESSION_ID_LEN];
    const topic_t *t;

    c->kind = "get";
    if (dec_get(payload, len, key) != 0) {
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "malformed GET request");
        return;
    }
    t = topic_find_or_rescan(c->map, key);
    if (t == NULL) {
        snprintf(msg, sizeof(msg), "unknown topic '%s'", key);
        conn_send_error(c, FDS_ERR_NOT_FOUND, msg);
        return;
    }
    set_topic(c, t);
    if (session_create(t, 0, id) != 0) {
        /* Every slot holds an ACTIVE session (deviation 12). The client
         * backs off and tries again. */
        conn_send_error(c, FDS_ERR_BUSY, "session table full");
        return;
    }
    bind_session(c, id);
    log_event("SESSION_NEW", c->conn_id, id, t->key, -1, 0, 0, 0, NULL);
    fault_plan(c->fault_state, t->total_bytes);        /* the whole topic */
    end_session(c, serve_topic(c, t, id, 0, 0, 1));
}

/* Phase 5: tell the client why its checkpoint is unusable. No session is
 * bound, so nothing on the server changes. */
static void reject_resume(conn_t *c, const m_resume_t *m, const topic_t *t, int reason)
{
    uint8_t buf[SMALL_MSG_BUF];
    size_t  n = enc_resume_reject(buf, sizeof(buf), (uint16_t)reason);

    /* Best effort: the connection ends here whether or not it arrives. */
    (void)conn_send(c, buf, n, 0);
    c->status = "REJECT";
    log_event("RESUME_REJECT", c->conn_id, m->session_id, t != NULL ? t->key : m->topic,
              (int64_t)m->file_index, m->offset, 0, 0, reject_reason_str((uint16_t)reason));
}

/*
 * Phase 4: RESUME(session_id, file_index, offset, topic, name).
 * session_validate_resume() runs the five checks of design 5.5 and, if they
 * pass, moves the session's record to the checkpoint and marks it ACTIVE.
 * Files before file_index are complete on the client and are not sent again;
 * file_index itself is sent from `offset`, so no stored byte is retransmitted.
 */
static void handle_resume(conn_t *c, const uint8_t *payload, size_t len)
{
    uint8_t        buf[SMALL_MSG_BUF];
    m_resume_t     m;
    m_resume_ok_t  ok;
    const topic_t *t;
    size_t         n;
    int            rc;

    c->kind = "resume";
    if (dec_resume(payload, len, &m) != 0) {
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "malformed RESUME request");
        return;
    }
    /* We never hand out the all-zero id (session_create skips it), but zeroed
     * memory looks like it: two clients with a zeroed .session must not end up
     * sharing one adopted record. */
    if (bytes_all_zero(m.session_id, sizeof(m.session_id))) {
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "RESUME without a session id");
        return;
    }
    /* The topic as the client typed it; NULL is answered by check 1. */
    t = topic_find_or_rescan(c->map, m.topic);
    if (t != NULL)
        set_topic(c, t);

    rc = session_validate_resume(m.session_id, t, m.file_index, m.name, m.offset, 0);
    if (rc < 0) {
        /* Unknown id, checkpoint valid, but no slot to adopt it into. */
        conn_send_error(c, FDS_ERR_BUSY, "session table full");
        return;
    }
    if (rc > 0) {
        reject_resume(c, &m, t, rc);
        return;
    }

    bind_session(c, m.session_id);
    ok.file_index = m.file_index;
    ok.offset     = m.offset;
    n = enc_resume_ok(buf, sizeof(buf), &ok);
    if (conn_send(c, buf, n, 0) != 0) {
        c->status = "PEER_LOST";
        session_set_state(c->session_id, SESS_INTERRUPTED);
        return;
    }
    log_event("RESUME_OK", c->conn_id, m.session_id, t->key, (int64_t)m.file_index,
              m.offset, 0, 0, NULL);
    fault_plan(c->fault_state, bytes_from(t, m.file_index, m.offset));
    end_session(c, serve_topic(c, t, m.session_id, m.file_index, m.offset, 0));
}

/*
 * One connection, in its own worker thread. The first message decides the
 * phase: GET starts a session, RESUME continues one. Anything else is a
 * malformed request.
 */
static int serve_client(conn_t *c)
{
    fault_state_t fs;           /* owned by this thread only: no lock needed */
    msg_hdr_t     h;
    uint8_t      *payload = NULL;
    int           rc;

    fault_init(&fs, c->cfg->p, c->cfg->have_seed, c->cfg->seed, c->conn_id);
    c->fault_state   = &fs;
    c->fault_hook    = fault_should_fail;
    c->progress_hook = on_progress;

    rc = conn_recv_request(c, &h, &payload, NULL);
    if (rc == NET_PROTO) {
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "malformed message header");
    } else if (rc != NET_OK) {
        c->status = "PEER_LOST";          /* closed or timed out before asking */
    } else if (h.type == MSG_GET) {
        handle_get(c, payload, (size_t)h.payload_len);
    } else if (h.type == MSG_RESUME) {
        handle_resume(c, payload, (size_t)h.payload_len);
    } else {
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "expected GET or RESUME");
    }
    free(payload);

    /* fs lives in this stack frame: make sure nothing can use it later. */
    c->fault_hook  = NULL;
    c->fault_state = NULL;
    return 0;
}

int main(int argc, char **argv)
{
    const char  *prog = argc > 0 ? argv[0] : "server";
    server_cfg_t cfg;

    memset(&cfg, 0, sizeof(cfg));
    if (argc < 4 || argc > 5)
        usage(prog);
    if (parse_port(argv[1], &cfg.port) != 0) {
        fprintf(stderr, "invalid port '%s' (expected 1..65535)\n", argv[1]);
        usage(prog);
    }
    if (argv[2][0] == '\0') {
        fprintf(stderr, "topics_root_dir must not be empty\n");
        usage(prog);
    }
    cfg.root = argv[2];
    if (parse_prob(argv[3], &cfg.p) != 0) {
        fprintf(stderr, "invalid failure probability '%s' (expected a number in [0, 1])\n",
                argv[3]);
        usage(prog);
    }
    if (argc == 5) {
        if (parse_long(argv[4], LONG_MIN, LONG_MAX, &cfg.seed) != 0) {
            fprintf(stderr, "invalid seed '%s' (expected an integer)\n", argv[4]);
            usage(prog);
        }
        cfg.have_seed = 1;
    }
    cfg.multi        = 1;               /* Part II: always one thread per client */
    cfg.case_name    = "case2";
    cfg.serve_client = serve_client;

    session_table_init();
    return server_run(&cfg);
}
