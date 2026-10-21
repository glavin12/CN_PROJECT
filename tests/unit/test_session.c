/*
 * test_session.c - unit tests for session.c (design sections 5.3 and 5.5,
 * deviations 11 and 12).
 *
 * Topics are built by hand (topic_map.c is not linked): session.c only reads
 * the key, files[], n_files and total_bytes fields.
 *
 * The CHECK macros of unit.h use plain counters, so the stress threads never
 * call them; they count their own errors and main() checks the totals.
 */
#include "unit.h"
#include "fds.h"
#include "session.h"
#include "util.h"

#include <pthread.h>
#include <stdlib.h>

/* Topic A: a 100 000-byte file (two chunks), an empty file whose name holds a
 * space and '=', and a small file. */
static file_entry_t files_a[3] = {
    { "a.jpg",     100000 },
    { "b c=d.jpg", 0 },
    { "z.png",     200 },
};
/* Topic B has the same file names as A: only the topic pointer differs. */
static file_entry_t files_b[3] = {
    { "a.jpg",     100000 },
    { "b c=d.jpg", 0 },
    { "z.png",     200 },
};
static topic_t topic_a, topic_b, topic_empty;

static void make_topic(topic_t *t, const char *key, file_entry_t *files, uint32_t n)
{
    memset(t, 0, sizeof(*t));
    snprintf(t->key, sizeof(t->key), "%s", key);
    t->files = files;
    t->n_files = n;
    for (uint32_t i = 0; i < n; i++) {
        t->total_bytes  += files[i].size;
        t->total_chunks += fds_chunks_of(files[i].size);
    }
}

/* A deterministic id that session_create() would never produce by chance. */
static void fixed_id(uint8_t id[FDS_SESSION_ID_LEN], uint8_t tag)
{
    memset(id, 0xA5, FDS_SESSION_ID_LEN);
    id[0] = tag;
}

/* State of a session, or -1 if the id is unknown (evicted / never existed). */
static int state_of(const uint8_t id[FDS_SESSION_ID_LEN])
{
    session_info_t info;

    return session_get_info(id, &info) == 0 ? (int)info.state : -1;
}

static void test_create_and_info(void)
{
    uint8_t id[FDS_SESSION_ID_LEN], id2[FDS_SESSION_ID_LEN];
    session_info_t info;

    session_table_init();
    CHECK_EQ(session_count(), 0);
    CHECK_EQ(session_create(&topic_a, 0, id), 0);
    CHECK(!bytes_all_zero(id, sizeof(id)));
    CHECK_EQ(session_count(), 1);
    CHECK_EQ(session_get_info(id, &info), 0);
    CHECK_EQ(info.state, SESS_ACTIVE);
    CHECK_EQ(info.next_file, 0);
    CHECK_EQ(info.file_off_sent, 0);
    CHECK_EQ(info.total_sent, 0);
    CHECK_EQ(info.reconnects, 0);

    CHECK_EQ(session_create(&topic_a, 0, id2), 0);
    CHECK(memcmp(id, id2, sizeof(id)) != 0);
    CHECK_EQ(session_count(), 2);

    CHECK_EQ(session_create(NULL, 0, id2), -1);       /* no topic: refused */
    fixed_id(id2, 1);
    CHECK_EQ(session_get_info(id2, &info), -1);       /* unknown id */
    CHECK_EQ(session_all_sent(id2), 0);
    session_progress(id2, 0, 10, 10);                 /* unknown: ignored */
    session_set_state(id2, SESS_DONE);                /* unknown: ignored */
    CHECK_EQ(session_count(), 2);

    /* Setting FREE releases the slot. */
    session_set_state(id, SESS_FREE);
    CHECK_EQ(session_get_info(id, &info), -1);
    CHECK_EQ(session_count(), 1);
}

static void test_reject_reasons(void)
{
    uint8_t id[FDS_SESSION_ID_LEN], eid[FDS_SESSION_ID_LEN];
    session_info_t info;

    session_table_init();
    CHECK_EQ(session_create(&topic_a, 0, id), 0);

    /* 1: no topic, or a known id of another topic */
    CHECK_EQ(session_validate_resume(id, NULL, 0, "a.jpg", 0, 0), FDS_REJ_UNKNOWN_TOPIC);
    CHECK_EQ(session_validate_resume(id, &topic_b, 0, "a.jpg", 0, 0), FDS_REJ_UNKNOWN_TOPIC);
    /* 2: index out of range */
    CHECK_EQ(session_validate_resume(id, &topic_a, 3, "a.jpg", 0, 0), FDS_REJ_INDEX_RANGE);
    CHECK_EQ(session_validate_resume(id, &topic_a, UINT32_MAX, "a.jpg", 0, 0), FDS_REJ_INDEX_RANGE);
    /* 3: name mismatch */
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "z.png", 0, 0), FDS_REJ_NAME_MISMATCH);
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "a.jpg ", 0, 0), FDS_REJ_NAME_MISMATCH);
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, NULL, 0, 0), FDS_REJ_NAME_MISMATCH);
    /* 4: offset beyond the file */
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "a.jpg", 100001, 0), FDS_REJ_OFFSET_TOO_BIG);
    CHECK_EQ(session_validate_resume(id, &topic_a, 1, "b c=d.jpg", 1, 0), FDS_REJ_OFFSET_TOO_BIG);
    /* 5 (Case 2): nothing sent yet, so any byte or later file is ahead */
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "a.jpg", 1, 0), FDS_REJ_AHEAD_OF_SENT);
    CHECK_EQ(session_validate_resume(id, &topic_a, 1, "b c=d.jpg", 0, 0), FDS_REJ_AHEAD_OF_SENT);

    /* The checks run in the documented order: the first failing one wins. */
    CHECK_EQ(session_validate_resume(id, &topic_b, 9, "x", 1u << 30, 0), FDS_REJ_UNKNOWN_TOPIC);
    CHECK_EQ(session_validate_resume(id, &topic_a, 9, "x", 1u << 30, 0), FDS_REJ_INDEX_RANGE);
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "x", 1u << 30, 0), FDS_REJ_NAME_MISMATCH);
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "a.jpg", 1u << 30, 0), FDS_REJ_OFFSET_TOO_BIG);

    /* A rejected checkpoint changes nothing. */
    CHECK_EQ(session_get_info(id, &info), 0);
    CHECK_EQ(info.next_file, 0);
    CHECK_EQ(info.file_off_sent, 0);
    CHECK_EQ(info.reconnects, 0);

    /* (0, 0) is never ahead. */
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "a.jpg", 0, 0), 0);
    CHECK_EQ(session_get_info(id, &info), 0);
    CHECK_EQ(info.reconnects, 1);

    /* 5 (Enhanced): ahead of sent_off[i] */
    CHECK_EQ(session_create(&topic_a, 1, eid), 0);
    CHECK_EQ(session_validate_resume(eid, &topic_a, 0, "a.jpg", 1, 1), FDS_REJ_AHEAD_OF_SENT);
    CHECK_EQ(session_validate_resume(eid, &topic_a, 2, "z.png", 200, 1), FDS_REJ_AHEAD_OF_SENT);
    CHECK_EQ(session_validate_resume(eid, &topic_b, 0, "a.jpg", 0, 1), FDS_REJ_UNKNOWN_TOPIC);
    CHECK_EQ(session_validate_resume(eid, &topic_a, 0, "a.jpg", 100001, 1), FDS_REJ_OFFSET_TOO_BIG);

    /* A known id used for the other kind of session: the record cannot
     * vouch for it (reported as reason 1). */
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "a.jpg", 0, 1), FDS_REJ_UNKNOWN_TOPIC);
    CHECK_EQ(session_validate_resume(eid, &topic_a, 0, "a.jpg", 0, 0), FDS_REJ_UNKNOWN_TOPIC);
}

static void test_case2_progress(void)
{
    uint8_t id[FDS_SESSION_ID_LEN];
    session_info_t info;

    session_table_init();
    CHECK_EQ(session_create(&topic_a, 0, id), 0);

    /* FILE_HDR(0) then one chunk */
    session_progress(id, 0, 0, 0);
    session_progress(id, 0, 65536, 65536);
    CHECK_EQ(session_get_info(id, &info), 0);
    CHECK_EQ(info.next_file, 0);
    CHECK_EQ(info.file_off_sent, 65536);
    CHECK_EQ(info.total_sent, 65536);

    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "a.jpg", 65537, 0), FDS_REJ_AHEAD_OF_SENT);
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "a.jpg", 65536, 0), 0);
    CHECK_EQ(session_get_info(id, &info), 0);
    CHECK_EQ(info.state, SESS_ACTIVE);
    CHECK_EQ(info.file_off_sent, 65536);
    CHECK_EQ(info.reconnects, 1);

    /* A checkpoint behind the record is accepted; the stream restarts there,
     * so the record follows it backwards. */
    session_set_state(id, SESS_INTERRUPTED);
    CHECK_EQ(state_of(id), SESS_INTERRUPTED);
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "a.jpg", 30000, 0), 0);
    CHECK_EQ(session_get_info(id, &info), 0);
    CHECK_EQ(info.state, SESS_ACTIVE);
    CHECK_EQ(info.file_off_sent, 30000);
    CHECK_EQ(info.reconnects, 2);
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "a.jpg", 30001, 0), FDS_REJ_AHEAD_OF_SENT);

    /* The rest of file 0 is sent. Without the n = 0 call after FILE_HDR(1)
     * the record is still at file 0, so (1, 0) is ahead of it ... */
    session_progress(id, 0, 95536, 65536);
    session_progress(id, 0, 100000, 4464);
    CHECK_EQ(session_validate_resume(id, &topic_a, 1, "b c=d.jpg", 0, 0), FDS_REJ_AHEAD_OF_SENT);
    /* ... the hook after FILE_HDR(i+1) moves next_file before the first
     * chunk, so the client's checkpoint (written on FILE_HDR) is accepted
     * even if the fault comes right before that chunk. */
    session_progress(id, 1, 0, 0);
    CHECK_EQ(session_get_info(id, &info), 0);
    CHECK_EQ(info.next_file, 1);
    CHECK_EQ(info.file_off_sent, 0);
    CHECK_EQ(session_validate_resume(id, &topic_a, 1, "b c=d.jpg", 0, 0), 0);
    /* An earlier file is never ahead (the client re-receives from there). */
    session_progress(id, 2, 0, 0);
    CHECK_EQ(session_validate_resume(id, &topic_a, 0, "a.jpg", 100000, 0), 0);
    CHECK_EQ(session_get_info(id, &info), 0);
    CHECK_EQ(info.next_file, 0);
    CHECK_EQ(info.file_off_sent, 100000);
    CHECK_EQ(info.total_sent, 65536 + 65536 + 4464);

    /* progress revives an INTERRUPTED session; a DONE one stays DONE */
    session_set_state(id, SESS_INTERRUPTED);
    session_progress(id, 0, 100000, 0);
    CHECK_EQ(state_of(id), SESS_ACTIVE);
    session_set_state(id, SESS_DONE);
    session_progress(id, 2, 200, 200);
    CHECK_EQ(state_of(id), SESS_DONE);
    /* A client that lost TOPIC_END may resume a DONE session. */
    CHECK_EQ(session_validate_resume(id, &topic_a, 2, "z.png", 200, 0), 0);
    CHECK_EQ(state_of(id), SESS_ACTIVE);
}

static void test_adoption(void)
{
    uint8_t u[FDS_SESSION_ID_LEN], u2[FDS_SESSION_ID_LEN], ue[FDS_SESSION_ID_LEN];
    session_info_t info;

    session_table_init();
    fixed_id(u, 10);
    fixed_id(u2, 11);
    fixed_id(ue, 12);

    /* An unknown id that fails checks 1-4 is rejected and not adopted. */
    CHECK_EQ(session_validate_resume(u2, NULL, 0, "a.jpg", 0, 0), FDS_REJ_UNKNOWN_TOPIC);
    CHECK_EQ(session_validate_resume(u2, &topic_a, 3, "a.jpg", 0, 0), FDS_REJ_INDEX_RANGE);
    CHECK_EQ(session_validate_resume(u2, &topic_a, 2, "zz.png", 0, 0), FDS_REJ_NAME_MISMATCH);
    CHECK_EQ(session_validate_resume(u2, &topic_a, 2, "z.png", 201, 0), FDS_REJ_OFFSET_TOO_BIG);
    CHECK_EQ(session_count(), 0);

    /* Case 2: checks 1-4 pass -> adopted under the client's id. */
    CHECK_EQ(session_validate_resume(u, &topic_a, 2, "z.png", 100, 0), 0);
    CHECK_EQ(session_count(), 1);
    CHECK_EQ(session_get_info(u, &info), 0);
    CHECK_EQ(info.state, SESS_ACTIVE);
    CHECK_EQ(info.next_file, 2);
    CHECK_EQ(info.file_off_sent, 100);
    CHECK_EQ(info.reconnects, 1);

    /* Ahead of the adopted record: still accepted (no history to check). */
    CHECK_EQ(session_validate_resume(u, &topic_a, 2, "z.png", 200, 0), 0);
    CHECK_EQ(session_get_info(u, &info), 0);
    CHECK_EQ(info.file_off_sent, 200);
    CHECK_EQ(info.reconnects, 2);
    session_set_state(u, SESS_INTERRUPTED);
    CHECK_EQ(session_validate_resume(u, &topic_a, 0, "a.jpg", 100000, 0), 0);
    CHECK_EQ(session_validate_resume(u, &topic_a, 2, "z.png", 150, 0), 0);
    /* Checks 1-4 still apply to an adopted session. */
    CHECK_EQ(session_validate_resume(u, &topic_b, 2, "z.png", 0, 0), FDS_REJ_UNKNOWN_TOPIC);
    CHECK_EQ(session_validate_resume(u, &topic_a, 2, "z.png", 201, 0), FDS_REJ_OFFSET_TOO_BIG);
    /* ... and it still records progress. */
    session_progress(u, 2, 180, 30);
    CHECK_EQ(session_get_info(u, &info), 0);
    CHECK_EQ(info.next_file, 2);
    CHECK_EQ(info.file_off_sent, 180);
    CHECK_EQ(info.total_sent, 30);
    CHECK_EQ(session_count(), 1);

    /* Enhanced adoption: a .part of another file from before the restart
     * is ahead of the empty record and must still be accepted. */
    CHECK_EQ(session_validate_resume(ue, &topic_a, 0, "a.jpg", 500, 1), 0);
    CHECK_EQ(session_count(), 2);
    CHECK_EQ(session_validate_resume(ue, &topic_a, 2, "z.png", 150, 1), 0);
    CHECK_EQ(session_validate_resume(ue, &topic_a, 0, "a.jpg", 90000, 1), 0);
    CHECK_EQ(session_get_info(ue, &info), 0);
    CHECK_EQ(info.reconnects, 0);           /* FETCHes are not reconnections */
    CHECK_EQ(session_all_sent(ue), 0);
    session_progress(ue, 0, 100000, 10000);
    session_progress(ue, 2, 200, 50);
    CHECK_EQ(session_all_sent(ue), 1);      /* file 1 is empty: sent_off 0 >= 0 */
}

static void test_enhanced(void)
{
    uint8_t e[FDS_SESSION_ID_LEN], c2[FDS_SESSION_ID_LEN], ez[FDS_SESSION_ID_LEN];
    session_info_t info;

    session_table_init();
    CHECK_EQ(session_create(&topic_a, 1, e), 0);
    CHECK_EQ(session_all_sent(e), 0);

    /* offset 0 is always accepted, for any file */
    CHECK_EQ(session_validate_resume(e, &topic_a, 0, "a.jpg", 0, 1), 0);
    CHECK_EQ(session_validate_resume(e, &topic_a, 2, "z.png", 0, 1), 0);
    CHECK_EQ(session_validate_resume(e, &topic_a, 0, "a.jpg", 10, 1), FDS_REJ_AHEAD_OF_SENT);

    session_progress(e, 0, 0, 0);
    session_progress(e, 0, 65536, 65536);
    CHECK_EQ(session_validate_resume(e, &topic_a, 0, "a.jpg", 65536, 1), 0);
    CHECK_EQ(session_validate_resume(e, &topic_a, 0, "a.jpg", 65537, 1), FDS_REJ_AHEAD_OF_SENT);
    /* Other files are independent of file 0's progress. */
    CHECK_EQ(session_validate_resume(e, &topic_a, 2, "z.png", 1, 1), FDS_REJ_AHEAD_OF_SENT);

    /* A lower offset is accepted, but sent_off keeps the maximum: a second
     * connection serving the file from 30000 does not lower it either. */
    CHECK_EQ(session_validate_resume(e, &topic_a, 0, "a.jpg", 30000, 1), 0);
    session_progress(e, 0, 40000, 10000);
    CHECK_EQ(session_validate_resume(e, &topic_a, 0, "a.jpg", 65536, 1), 0);
    CHECK_EQ(session_get_info(e, &info), 0);
    CHECK_EQ(info.reconnects, 0);
    CHECK_EQ(info.total_sent, 75536);

    /* A fault on one connection marks INTERRUPTED; progress on another
     * revives it. */
    session_set_state(e, SESS_INTERRUPTED);
    session_progress(e, 2, 200, 200);
    CHECK_EQ(state_of(e), SESS_ACTIVE);
    CHECK_EQ(session_all_sent(e), 0);       /* file 0 not complete */
    session_progress(e, 0, 100000, 34464);
    CHECK_EQ(session_all_sent(e), 1);

    /* An out-of-range index in progress is ignored (no write past the array). */
    session_progress(e, 3, 5, 5);
    session_progress(e, UINT32_MAX, 5, 5);
    CHECK_EQ(session_all_sent(e), 1);

    /* all_sent is Enhanced-only. */
    CHECK_EQ(session_create(&topic_a, 0, c2), 0);
    CHECK_EQ(session_all_sent(c2), 0);

    /* An empty topic has no array; every check 2 fails, all_sent is true. */
    CHECK_EQ(session_create(&topic_empty, 1, ez), 0);
    CHECK_EQ(session_validate_resume(ez, &topic_empty, 0, "a.jpg", 0, 1), FDS_REJ_INDEX_RANGE);
    session_progress(ez, 0, 1, 1);
    CHECK_EQ(session_all_sent(ez), 1);
}

static void test_table_full_and_eviction(void)
{
    static uint8_t ids[FDS_MAX_SESSIONS][FDS_SESSION_ID_LEN];
    uint8_t extra[FDS_SESSION_ID_LEN], extra2[FDS_SESSION_ID_LEN], u[FDS_SESSION_ID_LEN];
    int distinct = 1;

    /* FREE is preferred over DONE. */
    session_table_init();
    CHECK_EQ(session_create(&topic_a, 0, ids[0]), 0);
    session_set_state(ids[0], SESS_DONE);
    CHECK_EQ(session_create(&topic_a, 0, ids[1]), 0);
    CHECK_EQ(session_count(), 2);
    CHECK_EQ(state_of(ids[0]), SESS_DONE);

    /* Fill the table with ACTIVE sessions (half of them Enhanced). */
    session_table_init();
    for (int i = 0; i < FDS_MAX_SESSIONS; i++)
        CHECK_EQ(session_create(&topic_a, i & 1, ids[i]), 0);
    CHECK_EQ(session_count(), FDS_MAX_SESSIONS);
    for (int i = 0; i < FDS_MAX_SESSIONS && distinct; i++)
        for (int j = i + 1; j < FDS_MAX_SESSIONS; j++)
            if (memcmp(ids[i], ids[j], FDS_SESSION_ID_LEN) == 0)
                distinct = 0;
    CHECK(distinct);

    /* Every slot is a fresh ACTIVE session: never evicted. */
    CHECK_EQ(session_create(&topic_a, 0, extra), -1);
    fixed_id(u, 20);
    CHECK_EQ(session_validate_resume(u, &topic_a, 0, "a.jpg", 0, 0), -1);
    CHECK_EQ(session_count(), FDS_MAX_SESSIONS);
    for (int i = 0; i < FDS_MAX_SESSIONS; i++)
        CHECK_EQ(state_of(ids[i]), SESS_ACTIVE);

    /* A DONE slot is reusable at once. */
    session_set_state(ids[17], SESS_DONE);
    CHECK_EQ(session_create(&topic_a, 1, extra), 0);
    CHECK_EQ(state_of(ids[17]), -1);
    CHECK_EQ(state_of(extra), SESS_ACTIVE);
    CHECK_EQ(session_count(), FDS_MAX_SESSIONS);
    CHECK_EQ(session_create(&topic_a, 0, extra2), -1);
    memcpy(ids[17], extra, FDS_SESSION_ID_LEN);

    /* Oldest INTERRUPTED first. The sleeps make the last_seen times
     * distinct even on a coarse clock. */
    session_set_state(ids[5], SESS_INTERRUPTED);
    sleep_ms(2);
    session_set_state(ids[3], SESS_INTERRUPTED);
    sleep_ms(2);
    session_set_state(ids[9], SESS_INTERRUPTED);
    sleep_ms(2);
    CHECK_EQ(session_create(&topic_a, 0, extra), 0);
    CHECK_EQ(state_of(ids[5]), -1);
    CHECK_EQ(state_of(ids[3]), SESS_INTERRUPTED);
    CHECK_EQ(state_of(ids[9]), SESS_INTERRUPTED);
    memcpy(ids[5], extra, FDS_SESSION_ID_LEN);

    /* DONE beats an older INTERRUPTED one. */
    session_set_state(ids[21], SESS_DONE);
    CHECK_EQ(session_create(&topic_a, 0, extra), 0);
    CHECK_EQ(state_of(ids[21]), -1);
    CHECK_EQ(state_of(ids[3]), SESS_INTERRUPTED);
    memcpy(ids[21], extra, FDS_SESSION_ID_LEN);

    /* Adoption evicts the same way. */
    CHECK_EQ(session_validate_resume(u, &topic_a, 1, "b c=d.jpg", 0, 1), 0);
    CHECK_EQ(state_of(ids[3]), -1);
    CHECK_EQ(state_of(u), SESS_ACTIVE);
    CHECK_EQ(session_create(&topic_a, 0, extra), 0);   /* takes ids[9] */
    CHECK_EQ(state_of(ids[9]), -1);
    CHECK_EQ(session_create(&topic_a, 0, extra), -1);  /* all ACTIVE again */
    CHECK_EQ(session_count(), FDS_MAX_SESSIONS);

    /* A resumed INTERRUPTED session is ACTIVE again and safe from eviction. */
    session_set_state(ids[40], SESS_INTERRUPTED);
    CHECK_EQ(session_validate_resume(ids[40], &topic_a, 0, "a.jpg", 0, 0), 0);
    CHECK_EQ(session_create(&topic_a, 0, extra), -1);

    /*
     * Expiry (any state idle > FDS_SESSION_EXPIRY_S) would need a 300 s
     * wait or a fake clock, which the frozen API does not offer; the scan
     * above runs that comparison on every slot, and it must not fire for
     * fresh sessions, which the -1 results show.
     */

    /* Re-initialising frees every sent_off (LeakSanitizer checks it). */
    session_table_init();
    CHECK_EQ(session_count(), 0);
}

/* ---- Thread stress (run under TSan as well) ---------------------------- */

#define STRESS_THREADS 8
#define STRESS_ITERS   10000

static void *stress_worker(void *arg)
{
    long tid = (long)arg, errors = 0;
    uint8_t id[FDS_SESSION_ID_LEN];
    session_info_t info;

    for (int it = 0; it < STRESS_ITERS; it++) {
        int enh = (int)((tid + it) & 1);

        if (session_create(&topic_a, enh, id) != 0) {
            errors++;
            continue;
        }
        session_progress(id, 0, 0, 0);
        session_progress(id, 0, 65536, 65536);
        if (session_validate_resume(id, &topic_a, 0, "a.jpg", 65536, enh) != 0)
            errors++;
        if (session_validate_resume(id, &topic_a, 0, "a.jpg", 65537, enh) != FDS_REJ_AHEAD_OF_SENT)
            errors++;
        session_set_state(id, SESS_INTERRUPTED);
        if (session_get_info(id, &info) != 0 || info.state != SESS_INTERRUPTED)
            errors++;
        if (session_validate_resume(id, &topic_a, 0, "a.jpg", 1000, enh) != 0)
            errors++;
        if (enh) {
            session_progress(id, 0, 100000, 99000);
            session_progress(id, 2, 200, 200);
            if (session_all_sent(id) != 1)
                errors++;
        } else if (session_get_info(id, &info) != 0 || info.reconnects != 2) {
            errors++;
        }
        if (session_count() > FDS_MAX_SESSIONS)
            errors++;
        session_set_state(id, SESS_DONE);
    }
    return (void *)errors;
}

static void test_thread_stress(void)
{
    pthread_t th[STRESS_THREADS];
    long total_errors = 0;

    /*
     * Each thread has at most one non-DONE session at a time, so at least
     * 248 slots are DONE and no thread's INTERRUPTED session can be evicted:
     * every result above is deterministic despite the concurrency.
     */
    session_table_init();
    for (long t = 0; t < STRESS_THREADS; t++)
        CHECK_EQ(pthread_create(&th[t], NULL, stress_worker, (void *)t), 0);
    for (int t = 0; t < STRESS_THREADS; t++) {
        void *ret;
        CHECK_EQ(pthread_join(th[t], &ret), 0);
        total_errors += (long)ret;
    }
    CHECK_EQ(total_errors, 0);
    CHECK_EQ(session_count(), FDS_MAX_SESSIONS);   /* all DONE, none freed */
    session_table_init();
}

int main(void)
{
    make_topic(&topic_a, "gatto", files_a, 3);
    make_topic(&topic_b, "cane", files_b, 3);
    make_topic(&topic_empty, "vuoto", NULL, 0);

    test_create_and_info();
    test_reject_reasons();
    test_case2_progress();
    test_adoption();
    test_enhanced();
    test_table_full_and_eviction();
    test_thread_stress();
    session_table_init();          /* calling it twice is safe */
    UNIT_DONE();
}
