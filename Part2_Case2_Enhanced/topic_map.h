/*
 * topic_map.h - server map: topic key -> absolute directory -> sorted file list
 * (design section 2, with deviation 3).
 *
 * Built once at start-up from topics_root_dir:
 *   - every sub-folder (stat + S_ISDIR, name passes topic_is_valid(), not
 *     starting with '.') becomes one topic; key = folder name, abs_path = realpath();
 *   - every regular file directly inside it whose name passes name_is_safe()
 *     becomes a file_entry_t; files are sorted by name with strcmp(), so a file
 *     index always identifies the same file (the Case 2 checkpoints rely on it);
 *   - the topic pointer array is sorted by key (strcmp) for bsearch().
 *
 * Lifetime rule (deviation 3): a topic_t is heap-allocated once and is never
 * moved, modified or freed until topic_map_free() at process exit. Worker
 * threads and sessions may therefore keep `const topic_t *` without holding
 * any lock. A re-scan only ADDS topics (new sub-folders); it builds a new
 * sorted pointer array and swaps it in under the write lock. Files added to an
 * existing topic folder after load are not picked up (immutable snapshot).
 */
#ifndef FDS_TOPIC_MAP_H
#define FDS_TOPIC_MAP_H

#include <pthread.h>
#include <stdint.h>
#include <limits.h>
#include "fds.h"

typedef struct topic {
    char          key[FDS_TOPIC_MAX + 1];  /* folder name, e.g. "gatto" */
    char          abs_path[PATH_MAX];      /* realpath() of the folder (the map value) */
    file_entry_t *files;                   /* sorted by name */
    uint32_t      n_files;
    uint64_t      total_bytes;             /* sum of all file sizes */
    uint64_t      total_chunks;            /* sum of fds_chunks_of(size) */
} topic_t;

typedef struct {
    topic_t        **topics;               /* sorted by key; array replaced on re-scan */
    size_t           n_topics;
    char             root[PATH_MAX];       /* realpath() of topics_root_dir */
    pthread_rwlock_t lock;                 /* read: lookups; write: swapping in a re-scan */
    pthread_mutex_t  rescan_mu;            /* serialises re-scans */
    int64_t          last_rescan_ns;       /* now_ns() of the last re-scan (rate limit) */
} topic_map_t;

/*
 * Load the map from root. Returns 0, or -1 with a message on stderr if root
 * is not a readable directory. A root with zero topics is allowed (warning).
 */
int topic_map_load(topic_map_t *m, const char *root);

/*
 * Look a topic up: exact case-sensitive bsearch first, then a case-insensitive
 * linear match (the assignment writes "Cane", the folder is "cane").
 * Takes the read lock only for the lookup itself. NULL if not found.
 */
const topic_t *topic_find(topic_map_t *m, const char *key);

/*
 * topic_find(); if that fails, re-scan root for new topic folders (at most
 * once per FDS_RESCAN_MIN_MS) and try again. Used by every serve_client().
 */
const topic_t *topic_find_or_rescan(topic_map_t *m, const char *key);

/* Add topic folders created since load. Returns the number of topics added, or -1. */
int topic_map_rescan(topic_map_t *m);

/* Index of the file called `name` in t (bsearch), or -1. */
int64_t topic_file_index(const topic_t *t, const char *name);

/* Free everything (only at process exit). */
void topic_map_free(topic_map_t *m);

#endif /* FDS_TOPIC_MAP_H */
