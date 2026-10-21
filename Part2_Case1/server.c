/*
 * server.c - Part II Case 1: multi-threaded topic server with fault
 * injection and NO session management.
 *
 * IT305 Socket Programming Project, Group 006 + 043.
 *
 * Usage: ./server <port> <topics_root_dir> <failure_probability> [seed]
 *
 * Message exchange (design section 3.6): exactly the Part I messages.
 *
 *   client                                     server
 *     GET <topic>            ------------->
 *                            <-------------    TOPIC_BEGIN(session 0, file_count, total_bytes)
 *                            <-------------    FILE_HDR #0 + data + FILE_END ... FILE_HDR #57 + part of its data
 *                                              fault drawn for a chunk: send a random prefix of
 *                                              that chunk, then shutdown() + close()
 *     (recv() returns 0 before TOPIC_END: failure detected; back-off; reconnect)
 *     GET <topic>            ------------->
 *                            <-------------    TOPIC_BEGIN, ALL files again from #0, TOPIC_END
 *
 * Neither side keeps any transfer state: the session id is all zeros and
 * every connection serves the topic from file 0, offset 0. A reconnecting
 * client is indistinguishable from a new one.
 *
 * WHEN and HOW the failure decision is made (design section 4, deviation 9;
 * the full rationale is the header comment of fault.c):
 *   - WHEN: inside the file-send loop of transfer.c (send_file_range()),
 *     immediately before every 64 KB chunk of file data, through the
 *     connection's fault hook. Headers and control messages never get a
 *     decision, so every fault interrupts file data; a 0-byte file has no
 *     chunk and no decision.
 *   - HOW: each connection owns a fault_state_t (on this thread's stack, see
 *     case1_serve()) with its own drand48_r() generator, seeded with
 *     seed ^ conn_id when [seed] is given (repeatable runs), otherwise from
 *     time, thread id and conn_id. One uniform draw r in [0,1) per chunk; the
 *     chunk faults iff r < p. A second draw picks how many bytes of that chunk
 *     still go out (0 .. len-1), then the server logs a FAULT event and calls
 *     shutdown(SHUT_RDWR) + close(). Only that connection's thread ends; the
 *     server keeps accepting clients.
 *   - p is therefore a per-64-KB-chunk probability: with N chunks in the
 *     topic, an attempt succeeds with probability (1 - p)^N (design 4.4).
 *     p = 0 behaves exactly like Part I; p = 1 cuts every chunk, so Case 1
 *     can never finish and the client gives up after max_retries.
 *
 * Part II always runs thread-per-client (the assignment requires it), so
 * there is no single/multi argument here. The accept loop, thread cap, signal
 * thread and CSV rows are in server_core.c (deviation 14).
 */
#include "fault.h"
#include "fds.h"
#include "net.h"
#include "protocol.h"
#include "server_core.h"
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

/* Print why the arguments were rejected, then the usage line; exit 1. */
static void usage(const char *prog, const char *why)
{
    if (why != NULL)
        fprintf(stderr, "server: %s\n", why);
    fprintf(stderr, "usage: %s <port> <topics_root_dir> <failure_probability> [seed]\n", prog);
    exit(FDS_EXIT_FATAL);
}

/*
 * Read the client's request and decode "GET <topic>" into key.
 * Returns 0 on success. On failure the error reply (if any) has been sent and
 * c->status says why; the caller just returns. The payload is freed on every
 * path. Received bytes are counted in a local only: conn_t.wire_bytes is
 * defined as bytes SENT, and that is what the CSV reports.
 */
static int read_get_request(conn_t *c, char key[FDS_TOPIC_MAX + 1])
{
    msg_hdr_t h;
    uint8_t *payload = NULL;
    uint64_t rx_wire = 0;
    int rc = recv_msg(c->fd, &h, &payload, &rx_wire);
    int ok = rc == NET_OK && h.type == MSG_GET &&
             dec_get(payload, (size_t)h.payload_len, key) == 0;

    free(payload);                       /* NULL on the error paths: fine */
    if (ok)
        return 0;
    if (rc == NET_OK || rc == NET_PROTO) {
        /* A bad header (magic, version, type, length; design 3.1), another
         * message type, or a GET whose text is not "GET <valid topic>". */
        conn_send_error(c, FDS_ERR_BAD_REQUEST, "expected \"GET <topic>\"");
    } else {
        /* Closed, reset or timed out before a whole request arrived. */
        c->status = "PEER_LOST";
    }
    return -1;
}

/* ERROR 404 with the key the client asked for. */
static void reply_unknown_topic(conn_t *c, const char *key)
{
    char msg[128];

    snprintf(msg, sizeof(msg), "unknown topic '%s'", key);
    conn_send_error(c, FDS_ERR_NOT_FOUND, msg);
}

/*
 * Turn the result of serve_topic() into the connection's CSV status.
 * On XFER_FAULT transfer.c has already logged the FAULT event, set
 * c->faulted and c->status = "FAULT" and closed the socket.
 */
static void record_result(conn_t *c, xfer_rc_t rc)
{
    switch (rc) {
    case XFER_OK:
    case XFER_FAULT:
        break;
    case XFER_PEER:
        c->status = "PEER_LOST";        /* the client stopped reading */
        break;
    case XFER_IOERR:
        c->status = "ERR_500";          /* transfer.c already sent ERROR 500 */
        break;
    }
}

/* The GET exchange itself: identical to Part I. */
static void serve_get(conn_t *c)
{
    /* Case 1 keeps no session: TOPIC_BEGIN carries an all-zero session id. */
    static const uint8_t zero_sid[FDS_SESSION_ID_LEN];
    char key[FDS_TOPIC_MAX + 1];
    const topic_t *t;

    if (read_get_request(c, key) != 0)
        return;
    c->kind = "get";

    /* Exact match, then case-insensitive ("Cane" -> "cane"), then one re-scan
     * of topics_root_dir in case the folder was created after start-up. */
    t = topic_find_or_rescan(c->map, key);
    if (t == NULL) {
        reply_unknown_topic(c, key);
        return;
    }
    memcpy(c->topic, t->key, sizeof(c->topic));

    /* Always the whole topic from file 0, offset 0: nothing is remembered
     * from an earlier, interrupted connection of the same client. */
    record_result(c, serve_topic(c, t, zero_sid, 0, 0, 1));
}

/*
 * serve_client() of Case 1, run on the connection's own thread: install the
 * per-connection fault injector, then serve the GET.
 *
 * The fault state lives on this thread's stack: it belongs to exactly one
 * connection and one thread, so its generator needs no lock, and it is gone
 * when the connection ends. The hook pointers are cleared before returning
 * so nothing can reach the dead stack frame afterwards.
 */
static int case1_serve(conn_t *c)
{
    fault_state_t f;

    fault_init(&f, c->cfg->p, c->cfg->have_seed, c->cfg->seed, c->conn_id);
    c->fault_hook = fault_should_fail;
    c->fault_state = &f;

    serve_get(c);

    c->fault_hook = NULL;
    c->fault_state = NULL;
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
        /* Any long: fault_init() seeds each connection with seed ^ conn_id. */
        if (parse_long(argv[4], LONG_MIN, LONG_MAX, &cfg.seed) != 0)
            usage(argv[0], "seed must be an integer");
        cfg.have_seed = 1;
    }

    cfg.multi = 1;                       /* Part II: always one thread per client */
    cfg.case_name = "case1";
    cfg.serve_client = case1_serve;

    /* Loads the topic map (exit 1 if root is not a readable directory),
     * listens, and runs until SIGINT/SIGTERM. */
    return server_run(&cfg);
}
