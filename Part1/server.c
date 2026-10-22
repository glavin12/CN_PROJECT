/*
 * server.c - Part I: topic-based file server, single- or multi-threaded.
 *
 * IT305 Socket Programming Project, Group 006 + 043.
 *
 * Usage: ./server <port> <topics_root_dir> [single|multi]     (default multi)
 *
 * Every sub-folder of topics_root_dir is a topic; the folder name is the key
 * and its realpath() is the value (design section 2, topic_map.c). A client
 * asks for one topic and receives every file of it.
 *
 * Message exchange on one connection (design section 3.5):
 *
 *   client                                     server
 *     GET <topic>            ------------->    look the topic up
 *                            <-------------    TOPIC_BEGIN(session 0, file_count, total_bytes)
 *                            <-------------    FILE_HDR(#i, size, offset 0, name)
 *                            <-------------    size raw bytes of file #i (sendfile)
 *                            <-------------    FILE_END(#i)
 *                                  ... repeated for every file, in index order ...
 *                            <-------------    TOPIC_END(file_count, total_bytes)
 *                                              close
 *
 *   Unknown topic -> ERROR 404; malformed request -> ERROR 400; then close.
 *
 * This file only parses the arguments and provides serve_client(). The accept
 * loop, the thread per client, the 256-thread cap, the signal thread and the
 * per-connection CSV row are in server_core.c (deviation 14); the streaming
 * itself (FILE_HDR + sendfile + FILE_END) is serve_topic() in transfer.c.
 *
 * Why single vs multi is a runtime argument and not two programs or a
 * compile-time flag (design section 9): the grader requires exactly one
 * executable called "server" in this folder, and `make` must produce only
 * server and client. With an optional trailing argument, one build serves
 * both experiments, and the required invocation `./server <port> <root>`
 * still works unchanged (it runs the multi-threaded server). The two modes
 * use identical messages; they differ only in how server_core schedules
 * accepted connections: inline on the main thread (single) or one detached
 * pthread per client (multi).
 */
#include "fds.h"
#include "net.h"
#include "protocol.h"
#include "server_core.h"
#include "topic_map.h"
#include "transfer.h"
#include "util.h"

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
    fprintf(stderr, "usage: %s <port> <topics_root_dir> [single|multi]\n", prog);
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
    int rc = conn_recv_request(c, &h, &payload, &rx_wire);
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
 * XFER_FAULT cannot happen here (Part I installs no fault hook), but would
 * already carry status "FAULT" from transfer.c.
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

/*
 * serve_client() of Part I: one GET, the whole topic, done. server_core calls
 * it on the main thread (single) or on the connection's own thread (multi),
 * and closes the socket and writes the CSV row after it returns.
 */
static int part1_serve(conn_t *c)
{
    /* Part I keeps no session: TOPIC_BEGIN carries an all-zero session id. */
    static const uint8_t zero_sid[FDS_SESSION_ID_LEN];
    char key[FDS_TOPIC_MAX + 1];
    const topic_t *t;

    if (read_get_request(c, key) != 0)
        return 0;
    c->kind = "get";

    /* Exact match, then case-insensitive ("Cane" -> "cane"), then one re-scan
     * of topics_root_dir in case the folder was created after start-up. */
    t = topic_find_or_rescan(c->map, key);
    if (t == NULL) {
        reply_unknown_topic(c, key);
        return 0;
    }
    memcpy(c->topic, t->key, sizeof(c->topic));

    /* All files from index 0, offset 0, with TOPIC_BEGIN in front. */
    record_result(c, serve_topic(c, t, zero_sid, 0, 0, 1));
    return 0;
}

/* Optional third argument: "single" -> 0, "multi" -> 1, anything else -> -1. */
static int parse_mode(const char *s)
{
    if (strcmp(s, "single") == 0)
        return 0;
    if (strcmp(s, "multi") == 0)
        return 1;
    return -1;
}

int main(int argc, char **argv)
{
    server_cfg_t cfg;

    if (argc < 3 || argc > 4)
        usage(argv[0], NULL);

    memset(&cfg, 0, sizeof(cfg));
    if (parse_port(argv[1], &cfg.port) != 0)
        usage(argv[0], "port must be an integer in 1..65535");
    cfg.root = argv[2];
    cfg.multi = 1;                       /* default: thread per client */
    if (argc == 4 && (cfg.multi = parse_mode(argv[3])) < 0)
        usage(argv[0], "mode must be 'single' or 'multi'");

    cfg.p = -1.0;                        /* Part I: no fault injection at all */
    cfg.have_seed = 0;
    cfg.seed = 0;
    cfg.case_name = "part1";
    cfg.serve_client = part1_serve;

    /* Loads the topic map (exit 1 if root is not a readable directory),
     * listens, and runs until SIGINT/SIGTERM. */
    return server_run(&cfg);
}
