/*
 * fds.h - constants and small types shared by every module of the
 * Fault-tolerant topic-based File Distribution Service (FDS).
 *
 * IT305 Socket Programming Project, Group 006 + 043.
 * See docs/design_document.txt for the design and docs/DEVIATIONS.md for
 * the changes made during implementation.
 */
#ifndef FDS_H
#define FDS_H

#include <stddef.h>
#include <stdint.h>
#include <limits.h>

/* ---- Wire protocol constants (design section 3) ------------------------ */
#define FDS_MAGIC            0x5446u   /* "TF" */
#define FDS_VERSION          1u
#define FDS_HDR_LEN          16u       /* magic u16, ver u8, type u8, reserved u32, payload_len u64 */
#define FDS_SESSION_ID_LEN   16u
#define FDS_TOPIC_MAX        63u       /* topic key length 1..63 bytes */
#define FDS_NAME_MAX         255u      /* file name length 1..255 bytes */

/* Size caps per message type (deviation 2: MANIFEST may exceed 64 KB). */
#define FDS_MAX_CTRL_PAYLOAD      (64u * 1024u)
#define FDS_MAX_MANIFEST_PAYLOAD  (64u * 1024u * 1024u)
/* Largest payload a server accepts from a client. The biggest request is
 * RESUME/FETCH: 16+4+8+1+63+2+255 = 349 bytes. A server never buffers more
 * than this for an unauthenticated peer. */
#define FDS_MAX_REQUEST_PAYLOAD   512u

/* ---- Transfer constants ------------------------------------------------ */
#define FDS_CHUNK_SIZE       65536u    /* file data is sent in 64 KB chunks; one fault draw per chunk */
#define FDS_IO_BUF           65536u    /* client receive / write buffer */

/* ---- Server limits and timeouts ---------------------------------------- */
#define FDS_MAX_THREADS      256       /* concurrent worker threads; beyond -> ERROR 503 */
#define FDS_MAX_SESSIONS     256       /* server session table slots (Case 2 / Enhanced) */
#define FDS_SESSION_EXPIRY_S 300       /* idle session expiry */
#define FDS_SRV_TIMEOUT_S    30        /* server SO_SNDTIMEO / SO_RCVTIMEO */
#define FDS_CLI_TIMEOUT_S    15        /* Part II client SO_RCVTIMEO (Part I client: none, deviation 1) */
#define FDS_CONNECT_TIMEOUT_S 10       /* client connect() time-out */
#define FDS_SHUTDOWN_GRACE_S 5         /* graceful shutdown: wait this long for active transfers */
#define FDS_RESCAN_MIN_MS    1000      /* topic re-scan at most once per second */

/* ---- Client retry policy ----------------------------------------------- */
#define FDS_BACKOFF_START_MS 50
#define FDS_BACKOFF_MAX_MS   1000
#define FDS_CASE1_MAX_RETRIES 100      /* Case 1 counts every attempt */
#define FDS_CASE2_MAX_RETRIES 20       /* Case 2 / Enhanced count consecutive attempts without progress */
#define FDS_ENH_DEFAULT_K    4
#define FDS_ENH_MAX_K        16

/* ---- Process exit codes (design section 1.3) ---------------------------- */
#define FDS_EXIT_OK          0         /* success */
#define FDS_EXIT_FATAL       1         /* usage error, unknown topic, unrecoverable error */
#define FDS_EXIT_GAVE_UP     2         /* client reached max_retries */

/* ---- Error codes carried in MSG_ERROR ----------------------------------- */
#define FDS_ERR_BAD_REQUEST  400
#define FDS_ERR_NOT_FOUND    404
#define FDS_ERR_IO           500
#define FDS_ERR_BUSY         503

/* ---- RESUME_REJECT reasons (design section 5.5) -------------------------- */
#define FDS_REJ_UNKNOWN_TOPIC   1
#define FDS_REJ_INDEX_RANGE     2
#define FDS_REJ_NAME_MISMATCH   3
#define FDS_REJ_OFFSET_TOO_BIG  4
#define FDS_REJ_AHEAD_OF_SENT   5

/* One file of a topic, as cached by the server and as listed in a MANIFEST. */
typedef struct {
    char     name[FDS_NAME_MAX + 1];   /* plain file name, no directory part */
    uint64_t size;                     /* bytes, from stat() at load time */
} file_entry_t;

/* Number of 64 KB chunks needed for a file of `size` bytes (0 for an empty file). */
static inline uint64_t fds_chunks_of(uint64_t size)
{
    return (size + FDS_CHUNK_SIZE - 1) / FDS_CHUNK_SIZE;
}

#define FDS_MIN(a, b) ((a) < (b) ? (a) : (b))
#define FDS_MAX(a, b) ((a) > (b) ? (a) : (b))

#endif /* FDS_H */
