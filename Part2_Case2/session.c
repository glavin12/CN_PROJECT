/*
 * session.c - server-side session table (design sections 5.3 and 5.5,
 * deviations 11 and 12). The contract is in session.h.
 *
 * Why a fixed table behind one mutex:
 *   - A session must outlive the connection that created it (the whole point
 *     of Case 2 is that a *new* connection resumes it), so it cannot live on a
 *     worker thread's stack. A static array of FDS_MAX_SESSIONS slots needs no
 *     allocation per GET and bounds the server's memory.
 *   - Every operation is a handful of field updates, so one mutex is cheaper
 *     and far easier to defend than per-slot locks. The functions take the
 *     id, find the slot and finish their work before unlocking; no pointer to
 *     a slot is ever returned, so no caller can touch a slot without the lock
 *     (and a slot can be evicted and reused safely at any time).
 *
 * Topics: a slot keeps `const topic_t *`. Deviation 3 guarantees that a topic_t
 * is never moved or freed while the server runs, so the pointer stays valid
 * without a lock, and pointer equality is an exact "same topic" test. This
 * file only reads topic_t fields; it never calls the topic_map functions.
 */
#include "session.h"
#include "util.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

/* Idle time after which a slot is considered dead, in now_ns() units. */
#define EXPIRY_NS ((int64_t)FDS_SESSION_EXPIRY_S * 1000000000LL)

typedef struct {
    uint8_t        id[FDS_SESSION_ID_LEN];
    const topic_t *topic;
    sess_state_t   state;          /* SESS_FREE (= 0) means the slot is unused */
    int            enhanced;       /* 0 = Case 2 (one stream), 1 = Enhanced (K streams) */
    int            adopted;        /* 1 = created from a client's RESUME/FETCH, not by GET */

    /* Case 2 progress: where the single stream of this session is now. */
    uint32_t       next_file;
    uint64_t       file_off_sent;

    uint64_t       total_sent;     /* statistics: data bytes handed to sockets */
    uint32_t       reconnects;     /* statistics: Case 2 RESUMEs accepted */
    int64_t        last_seen_ns;   /* now_ns() of the last activity (eviction order) */

    /* Enhanced progress: highest offset of file i handed to any socket.
     * n_files entries when enhanced and n_files > 0, else NULL. */
    uint64_t      *sent_off;
} slot_t;

static slot_t          slot[FDS_MAX_SESSIONS];
/* Statically initialised so session_table_init() can safely run twice. */
static pthread_mutex_t table_mu = PTHREAD_MUTEX_INITIALIZER;

/*
 * Index of the live slot holding `id`, or -1. FREE slots are skipped, so a
 * zero-filled free slot can never match an id. Caller holds table_mu.
 */
static int find_locked(const uint8_t id[FDS_SESSION_ID_LEN])
{
    for (int i = 0; i < FDS_MAX_SESSIONS; i++)
        if (slot[i].state != SESS_FREE && memcmp(slot[i].id, id, FDS_SESSION_ID_LEN) == 0)
            return i;
    return -1;
}

/*
 * Choose a slot for a new session (deviation 12), in this order:
 *   1. a FREE slot;
 *   2. a DONE slot: its transfer is finished, so nobody needs it. The oldest
 *      one is taken, which keeps a just-finished record around a little
 *      longer for a client that lost the final TOPIC_END;
 *   3. a slot idle for more than FDS_SESSION_EXPIRY_S, whatever its state:
 *      a live connection updates last_seen after every chunk and the server
 *      times a silent peer out after FDS_SRV_TIMEOUT_S, so even an ACTIVE
 *      slot that has been quiet this long belongs to a dead thread;
 *   4. the INTERRUPTED slot that has waited longest for its client.
 * A fresh ACTIVE slot is never taken: a thread is streaming for it right now.
 * Returns -1 if only fresh ACTIVE slots exist (the caller replies ERROR 503).
 * Caller holds table_mu.
 */
static int pick_slot_locked(int64_t now)
{
    int done = -1, expired = -1, interrupted = -1;

    for (int i = 0; i < FDS_MAX_SESSIONS; i++) {
        const slot_t *s = &slot[i];

        if (s->state == SESS_FREE)
            return i;
        if (s->state == SESS_DONE) {
            if (done < 0 || s->last_seen_ns < slot[done].last_seen_ns)
                done = i;
        } else if (now - s->last_seen_ns > EXPIRY_NS) {
            if (expired < 0 || s->last_seen_ns < slot[expired].last_seen_ns)
                expired = i;
        } else if (s->state == SESS_INTERRUPTED) {
            if (interrupted < 0 || s->last_seen_ns < slot[interrupted].last_seen_ns)
                interrupted = i;
        }
    }
    if (done >= 0)
        return done;
    if (expired >= 0)
        return expired;
    return interrupted;
}

/*
 * Turn slot i into a new ACTIVE session. Returns the slot's previous sent_off
 * so the caller can free() it after unlocking (no reason to hold the lock
 * while the allocator works). Caller holds table_mu.
 */
static uint64_t *fill_slot_locked(int i, const uint8_t id[FDS_SESSION_ID_LEN],
                                  const topic_t *t, int enhanced, int adopted,
                                  uint64_t *sent_off, int64_t now)
{
    uint64_t *old = slot[i].sent_off;

    memset(&slot[i], 0, sizeof(slot[i]));
    memcpy(slot[i].id, id, FDS_SESSION_ID_LEN);
    slot[i].topic        = t;
    slot[i].state        = SESS_ACTIVE;
    slot[i].enhanced     = enhanced;
    slot[i].adopted      = adopted;
    slot[i].last_seen_ns = now;
    slot[i].sent_off     = sent_off;
    return old;
}

/*
 * The per-file record of an Enhanced session (zeroed: nothing sent yet).
 * Returns 0 with *out set (NULL when no array is needed), or -1 if out of
 * memory.
 */
static int alloc_sent_off(const topic_t *t, int enhanced, uint64_t **out)
{
    *out = NULL;
    if (!enhanced || t->n_files == 0)
        return 0;
    *out = calloc(t->n_files, sizeof(**out));
    return *out != NULL ? 0 : -1;
}

void session_table_init(void)
{
    pthread_mutex_lock(&table_mu);
    /* Free first so that a second call (tests, or a restart of the server
     * loop) does not leak the arrays of the sessions it wipes. */
    for (int i = 0; i < FDS_MAX_SESSIONS; i++)
        free(slot[i].sent_off);
    memset(slot, 0, sizeof(slot));     /* SESS_FREE == 0 */
    pthread_mutex_unlock(&table_mu);
}

int session_create(const topic_t *t, int enhanced, uint8_t id_out[FDS_SESSION_ID_LEN])
{
    uint8_t   id[FDS_SESSION_ID_LEN];
    uint64_t *sent_off, *old;
    int64_t   now;
    int       i;

    if (t == NULL)
        return -1;
    enhanced = enhanced != 0;
    /* Allocate before locking: a new Enhanced session always needs the
     * array, and calloc of a few thousand entries should not stall the other
     * threads. */
    if (alloc_sent_off(t, enhanced, &sent_off) != 0)
        return -1;

    pthread_mutex_lock(&table_mu);
    now = now_ns();
    i = pick_slot_locked(now);
    if (i < 0) {
        pthread_mutex_unlock(&table_mu);
        free(sent_off);
        return -1;
    }
    /*
     * 128 random bits make a collision practically impossible, but the check
     * costs one table scan, and two clients sharing a record would be a real
     * bug, so it is cheap insurance. An all-zero id is never handed out
     * because zero-filled memory (a FREE slot, an uninitialised id) looks
     * exactly like it.
     */
    do {
        random_bytes(id, sizeof(id));
    } while (bytes_all_zero(id, sizeof(id)) || find_locked(id) >= 0);
    old = fill_slot_locked(i, id, t, enhanced, 0, sent_off, now);
    pthread_mutex_unlock(&table_mu);

    free(old);
    memcpy(id_out, id, FDS_SESSION_ID_LEN);
    return 0;
}

/*
 * Checks 1-4 of design section 5.5 for slot i (-1 = unknown id). They only
 * compare the checkpoint with the topic, so they are safe whether or not the
 * server remembers the session: a checkpoint that passes them names a real
 * byte position of a real file of this topic. Caller holds table_mu.
 */
static int check_static_locked(int i, const topic_t *t, uint32_t file_index,
                               const char *name, uint64_t offset, int enhanced)
{
    if (t == NULL)
        return FDS_REJ_UNKNOWN_TOPIC;
    /*
     * A known id must belong to this topic. It must also be the same kind of
     * session: a Case 2 record has no sent_off[] to check an Enhanced FETCH
     * against (and the reverse), so the record cannot vouch for it. That can
     * only happen through a programming error, and rejecting it leads to a
     * fresh start, never to wrong data.
     */
    if (i >= 0 && (slot[i].topic != t || slot[i].enhanced != enhanced))
        return FDS_REJ_UNKNOWN_TOPIC;
    if (file_index >= t->n_files)
        return FDS_REJ_INDEX_RANGE;
    /* Same index but another name: the topic folder changed since the
     * client's run started, so its data may belong to a different file. */
    if (name == NULL || strcmp(name, t->files[file_index].name) != 0)
        return FDS_REJ_NAME_MISMATCH;
    if (offset > t->files[file_index].size)
        return FDS_REJ_OFFSET_TOO_BIG;
    return 0;
}

/*
 * Check 5: the checkpoint is not ahead of what this session handed to its
 * socket(s). The server's record is an upper bound on what was handed to the
 * socket (it is updated after every send, and a byte cannot reach the client
 * before it is sent), so a client checkpoint can never legitimately exceed
 * it. One that does comes from a corrupted or unrelated .session; rejecting
 * it makes the client start over instead of keeping bytes of unknown origin.
 *
 * Enhanced: offset 0 means "send the whole file", which is always safe.
 *
 * sent_off is non-NULL here: the slot is enhanced and check 2 passed, so
 * n_files > 0 and alloc_sent_off() created the array.
 */
static int ahead_of_sent(const slot_t *s, uint32_t file_index, uint64_t offset)
{
    if (s->enhanced)
        return !(offset == 0 || offset <= s->sent_off[file_index]);
    return !(file_index < s->next_file ||
             (file_index == s->next_file && offset <= s->file_off_sent));
}

int session_validate_resume(const uint8_t id[FDS_SESSION_ID_LEN], const topic_t *t,
                            uint32_t file_index, const char *name, uint64_t offset,
                            int enhanced)
{
    uint64_t *old = NULL;
    slot_t   *s;
    int64_t   now;
    int       i, rc;

    enhanced = enhanced != 0;
    pthread_mutex_lock(&table_mu);
    i = find_locked(id);
    rc = check_static_locked(i, t, file_index, name, offset, enhanced);
    /*
     * Check 5 needs history, and an adopted session has none: its record was
     * started from a client checkpoint after a server restart or an eviction,
     * so it knows nothing about the bytes the earlier record had sent. A
     * client may legitimately hold data from before the adoption that is
     * ahead of the adopted record (Enhanced: a .part of another file, whose
     * sent_off is still 0), and check 5 would wrongly throw that data away.
     * Checks 1-4 alone already make the resume safe (design section 5.5),
     * so an adopted session skips check 5 for the rest of its life.
     */
    if (rc == 0 && i >= 0 && !slot[i].adopted && ahead_of_sent(&slot[i], file_index, offset))
        rc = FDS_REJ_AHEAD_OF_SENT;
    if (rc != 0) {
        pthread_mutex_unlock(&table_mu);
        return rc;
    }

    now = now_ns();
    if (i < 0) {
        /*
         * Unknown id (server restarted, or the slot was evicted/expired):
         * adopt it under the client's own id, so that the client keeps
         * using the id it already stored and later FETCHes of the same
         * Enhanced run find this record. The array is allocated under the
         * lock here, unlike in session_create(), because only now do we know
         * it is needed; allocating it for every FETCH "just in case" would
         * cost far more, and adoption is rare.
         */
        uint64_t *sent_off;

        i = pick_slot_locked(now);
        if (i < 0 || alloc_sent_off(t, enhanced, &sent_off) != 0) {
            pthread_mutex_unlock(&table_mu);
            return -1;
        }
        old = fill_slot_locked(i, id, t, enhanced, 1, sent_off, now);
    }

    s = &slot[i];
    s->state = SESS_ACTIVE;
    if (s->enhanced) {
        /*
         * The server is about to send this file from `offset`, and the client
         * already holds [0, offset). For a known session check 5 makes the
         * max a no-op; for an adopted one it raises the empty record to what
         * the client proved it has. Each FETCH is one file of a run, not a
         * reconnection, so reconnects is not counted for Enhanced.
         */
        if (offset > s->sent_off[file_index])
            s->sent_off[file_index] = offset;
    } else {
        /*
         * Case 2 has one stream, and from now on it restarts at the
         * checkpoint; the record follows the stream, even if that moves it
         * backwards (the bytes after the checkpoint will be sent again).
         */
        s->next_file     = file_index;
        s->file_off_sent = offset;
        s->reconnects++;
    }
    s->last_seen_ns = now;
    pthread_mutex_unlock(&table_mu);

    free(old);
    return 0;
}

void session_progress(const uint8_t id[FDS_SESSION_ID_LEN], uint32_t file_index,
                      uint64_t new_off, uint64_t n)
{
    int64_t now = now_ns();     /* read before locking: keeps the section short */
    slot_t *s;
    int     i;

    pthread_mutex_lock(&table_mu);
    i = find_locked(id);
    if (i < 0) {
        /* Evicted while the transfer ran: nothing to record. The next RESUME
         * or FETCH will adopt the id again. */
        pthread_mutex_unlock(&table_mu);
        return;
    }
    s = &slot[i];
    if (s->enhanced) {
        /*
         * max(): K connections may serve the same file one after another
         * (a re-queued FETCH starts again at the client's offset), and the
         * record must keep the highest offset ever handed to a socket.
         * The bound check guards the array against a caller bug.
         */
        if (file_index < s->topic->n_files && new_off > s->sent_off[file_index])
            s->sent_off[file_index] = new_off;
    } else {
        /*
         * Case 2 streams files strictly in order on one connection, so the
         * latest call is the stream position. The server calls this with
         * n = 0 right after each FILE_HDR, which moves next_file to the new
         * file before its first chunk: a fault just before that chunk then
         * leaves a record that accepts the client's (new file, offset 0)
         * checkpoint written on FILE_HDR.
         */
        s->next_file     = file_index;
        s->file_off_sent = new_off;
    }
    s->total_sent  += n;
    s->last_seen_ns = now;
    /* Bytes are flowing again, so the session is alive (Enhanced: another of
     * its K connections faulted earlier). A DONE session stays DONE. */
    if (s->state == SESS_INTERRUPTED)
        s->state = SESS_ACTIVE;
    pthread_mutex_unlock(&table_mu);
}

void session_set_state(const uint8_t id[FDS_SESSION_ID_LEN], sess_state_t st)
{
    uint64_t *old = NULL;
    int64_t   now = now_ns();
    int       i;

    pthread_mutex_lock(&table_mu);
    i = find_locked(id);
    if (i >= 0) {
        switch (st) {
        case SESS_FREE:
            /* Releasing a slot must also release its array. */
            old = slot[i].sent_off;
            memset(&slot[i], 0, sizeof(slot[i]));
            break;
        case SESS_ACTIVE:
        case SESS_INTERRUPTED:
        case SESS_DONE:
            slot[i].state        = st;
            slot[i].last_seen_ns = now;   /* INTERRUPTED: the wait for the client starts now */
            break;
        default:
            break;                        /* not a state: ignore */
        }
    }
    pthread_mutex_unlock(&table_mu);
    free(old);
}

int session_all_sent(const uint8_t id[FDS_SESSION_ID_LEN])
{
    int all = 0, i;

    pthread_mutex_lock(&table_mu);
    i = find_locked(id);
    /* Only an Enhanced session has per-file progress; Case 2 learns that it
     * is finished from serve_topic() reaching TOPIC_END. */
    if (i >= 0 && slot[i].enhanced) {
        const topic_t *t = slot[i].topic;

        all = 1;
        for (uint32_t f = 0; f < t->n_files; f++) {
            if (slot[i].sent_off[f] < t->files[f].size) {
                all = 0;
                break;
            }
        }
    }
    pthread_mutex_unlock(&table_mu);
    return all;
}

int session_get_info(const uint8_t id[FDS_SESSION_ID_LEN], session_info_t *out)
{
    int i;

    pthread_mutex_lock(&table_mu);
    i = find_locked(id);
    if (i >= 0) {
        /* Copy the values out: the slot itself must not leave the lock. */
        out->state         = slot[i].state;
        out->next_file     = slot[i].next_file;
        out->file_off_sent = slot[i].file_off_sent;
        out->total_sent    = slot[i].total_sent;
        out->reconnects    = slot[i].reconnects;
    }
    pthread_mutex_unlock(&table_mu);
    return i >= 0 ? 0 : -1;
}

int session_count(void)
{
    int n = 0;

    pthread_mutex_lock(&table_mu);
    for (int i = 0; i < FDS_MAX_SESSIONS; i++)
        if (slot[i].state != SESS_FREE)
            n++;
    pthread_mutex_unlock(&table_mu);
    return n;
}
