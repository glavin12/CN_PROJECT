/*
 * checkpoint.h - client-side checkpoint (design section 5.4, deviation 13).
 *
 * The client is authoritative for its checkpoint because only it knows which
 * bytes reached its disk:
 *   output_dir/<topic>/<name>.part   partial data; its size is the byte offset
 *   output_dir/<topic>/<name>        completed file (renamed on FILE_END)
 *   output_dir/<topic>/.session      text key=value record, written atomically
 *                                    (.session.tmp + rename) on TOPIC_BEGIN /
 *                                    MANIFEST and on every FILE_HDR (Case 2):
 *
 *     session=<32 hex digits>
 *     topic=<topic as typed>
 *     file_index=<n>        (Case 2: file currently being received; -1 = none yet)
 *     file_name=<name>      (Case 2: its name; empty if none)
 *     k=<connections>       (Enhanced only)
 *
 * The offset is never stored: it is always stat(<name>.part).st_size, so the
 * checkpoint can never disagree with the data on disk, and saving it costs no
 * extra I/O per chunk. Because it is on disk, a transfer can also resume after
 * the client program itself was restarted.
 *
 * On success the client removes .session (ckpt_clear) so output_dir/<topic>/
 * contains only the topic files.
 */
#ifndef FDS_CHECKPOINT_H
#define FDS_CHECKPOINT_H

#include <stdint.h>
#include "fds.h"

#define CKPT_FILE      ".session"
#define CKPT_TMP_FILE  ".session.tmp"
#define PART_SUFFIX    ".part"

typedef struct {
    uint8_t  session_id[FDS_SESSION_ID_LEN];
    char     topic[FDS_TOPIC_MAX + 1];
    int64_t  file_index;                 /* -1 = no FILE_HDR received yet */
    char     file_name[FDS_NAME_MAX + 1];
    int      k;                          /* 0 if absent */
} ckpt_t;

/* Load dir/.session. Returns 0 if loaded, 1 if there is none, -1 if corrupt
 * (callers treat -1 like "none" after ckpt_clear()). */
int ckpt_load(const char *dir, ckpt_t *ck);

/* Write dir/.session atomically (tmp file + rename). Returns 0 or -1. */
int ckpt_save(const char *dir, const ckpt_t *ck);

/* Remove dir/.session and dir/.session.tmp (missing files are fine). */
int ckpt_clear(const char *dir);

/* Size of dir/<name>.part into *size. Returns 0, or -1 if it does not exist. */
int part_size(const char *dir, const char *name, uint64_t *size);

/* Size of the completed dir/<name> into *size. Returns 0, or -1 if absent. */
int final_size(const char *dir, const char *name, uint64_t *size);

/* Remove every *.part file and the checkpoint files in dir (fresh restart
 * after RESUME_REJECT). Completed files are left; they will be overwritten. */
int remove_partials(const char *dir);

#endif /* FDS_CHECKPOINT_H */
