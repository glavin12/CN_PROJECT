/*
 * session.h - server-side session table (design sections 5.3 and 5.5,
 * deviations 11 and 12). Used by Case 2 and Case 2 Enhanced.
 *
 * One global table of FDS_MAX_SESSIONS slots protected by one mutex. All
 * access goes through the functions below, which look a session up by its id
 * and do their work while holding the mutex, so no session pointer ever
 * escapes the lock.
 *
 * States: FREE -> ACTIVE (created / resumed) -> INTERRUPTED (fault injected or
 * peer lost) -> ACTIVE (resumed) ... -> DONE (all files sent).
 * A DONE slot may be reused immediately. A slot idle for more than
 * FDS_SESSION_EXPIRY_S may be reused. When the table is full, the oldest
 * non-ACTIVE slot is evicted; if every slot is ACTIVE, create fails (-> 503).
 *
 * Case 2 progress: (next_file, file_off_sent) = the position up to which data
 * has been handed to the socket on the session's single connection.
 * Enhanced progress (enhanced=1): sent_off[i] = highest offset of file i
 * handed to any socket of the session (K connections in parallel).
 */
#ifndef FDS_SESSION_H
#define FDS_SESSION_H

#include <stdint.h>
#include "fds.h"
#include "topic_map.h"

typedef enum { SESS_FREE = 0, SESS_ACTIVE, SESS_INTERRUPTED, SESS_DONE } sess_state_t;

/* Initialise the empty table (call once at server start-up). */
void session_table_init(void);

/*
 * Create a new session for topic t with a fresh random id (written to id_out).
 * enhanced != 0 allocates the per-file sent_off[] array.
 * Returns 0, or -1 if the table is full (caller replies ERROR 503).
 */
int session_create(const topic_t *t, int enhanced, uint8_t id_out[FDS_SESSION_ID_LEN]);

/*
 * Validate a checkpoint from RESUME (enhanced=0) or FETCH (enhanced=1),
 * design section 5.5:
 *   1 t != NULL (topic exists)                          else FDS_REJ_UNKNOWN_TOPIC
 *     and, if the session is known, it belongs to t     else FDS_REJ_UNKNOWN_TOPIC
 *   2 file_index < t->n_files                           else FDS_REJ_INDEX_RANGE
 *   3 name == t->files[file_index].name                 else FDS_REJ_NAME_MISMATCH
 *   4 offset <= t->files[file_index].size               else FDS_REJ_OFFSET_TOO_BIG
 *   5 known session only: checkpoint not ahead of the data sent:
 *       Case 2:   file_index < next_file, or file_index == next_file and offset <= file_off_sent
 *       Enhanced: offset == 0 or offset <= sent_off[file_index]
 *                                                       else FDS_REJ_AHEAD_OF_SENT
 * If the id is unknown (server restarted, entry expired) checks 1-4 suffice:
 * a new ACTIVE entry is created under the client's id and marked "adopted".
 * An adopted session skips check 5 for the rest of its life: the server has
 * no record of what was sent before it adopted the id, so it has no upper
 * bound to check against (deviation 22). Checks 1-4 still apply.
 * On success the session is ACTIVE, its progress is set to the checkpoint
 * (Case 2: next_file = file_index, file_off_sent = offset), reconnects++ and
 * 0 is returned; otherwise the reject reason (1..5).
 * If the table is full when adopting an unknown id, returns -1 (-> ERROR 503).
 */
int session_validate_resume(const uint8_t id[FDS_SESSION_ID_LEN], const topic_t *t,
                            uint32_t file_index, const char *name, uint64_t offset,
                            int enhanced);

/*
 * Progress after every chunk sent (the progress_hook of a Case 2 / Enhanced
 * connection calls this): file `file_index` has been handed to the socket up to
 * byte `new_off`; `n` data bytes were just sent. Updates next_file /
 * file_off_sent (Case 2) or sent_off[] (Enhanced), total_sent and last_seen.
 * Unknown id: ignored.
 */
void session_progress(const uint8_t id[FDS_SESSION_ID_LEN], uint32_t file_index,
                      uint64_t new_off, uint64_t n);

/* Set the state (SESS_INTERRUPTED on fault / peer lost, SESS_DONE on TOPIC_END). */
void session_set_state(const uint8_t id[FDS_SESSION_ID_LEN], sess_state_t st);

/*
 * Enhanced: 1 when every file of the session's topic has sent_off == size
 * (the server then marks it DONE), 0 otherwise or if unknown.
 */
int session_all_sent(const uint8_t id[FDS_SESSION_ID_LEN]);

/* Statistics snapshot for logs/tests. Returns 0, or -1 if the id is unknown. */
typedef struct {
    sess_state_t state;
    uint32_t     next_file;
    uint64_t     file_off_sent;
    uint64_t     total_sent;
    uint32_t     reconnects;
} session_info_t;
int session_get_info(const uint8_t id[FDS_SESSION_ID_LEN], session_info_t *out);

/* Number of non-FREE slots (tests). */
int session_count(void);

#endif /* FDS_SESSION_H */
