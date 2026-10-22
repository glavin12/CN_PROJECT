/*
 * topic_map.c - the server's topic map: topic key -> absolute folder ->
 * sorted file list (design sections 2, 2.1 and 7, deviation 3).
 * The contract is in topic_map.h; this comment explains how it is met.
 *
 * Data layout
 *   m->topics is an array of POINTERS to topic_t, sorted by key with strcmp()
 *   so that topic_find() can bsearch() it. Each topic_t, together with its
 *   files[] array, is malloc'd once and filled completely while it is still
 *   private to the thread that builds it. After that it is never written,
 *   moved or freed until topic_map_free() at process exit. A worker thread can
 *   therefore use the const topic_t * returned by topic_find() for a whole
 *   transfer without holding any lock (deviation 3). Only the small pointer
 *   array is ever replaced.
 *
 * Locking
 *   lock (rwlock)  guards exactly two fields: m->topics and m->n_topics.
 *                  A reader holds it for one bsearch (plus one strcasecmp pass
 *                  on a miss); the only writer, a re-scan, holds it for two
 *                  stores. Nobody holds it across disk or network I/O, so
 *                  neither side makes the other wait noticeably, and glibc's
 *                  default reader preference cannot starve the writer in
 *                  practice.
 *   rescan_mu      serialises re-scans, so two threads never both decide that
 *                  folder "x" is new and add it twice. It also guards
 *                  last_rescan_ns, the rate limiter.
 *   The slow part of a re-scan (readdir, stat, malloc, qsort) runs with only
 *   rescan_mu held, so lookups of existing topics never wait for the disk.
 *
 * What counts as a topic and as a file. Load and re-scan apply the same rules,
 * because both go through scan_root() and load_topic().
 *   topic: an entry of root that does not start with '.', is a directory
 *          after following symlinks, and whose name passes topic_is_valid()
 *          (1..63 bytes, no '/', no control characters). key = folder name,
 *          abs_path = realpath() of the folder.
 *   file:  an entry of a topic folder whose name passes name_is_safe() (so the
 *          client can create it safely: no leading '.', no control
 *          characters) and that is a regular file after following symlinks.
 *          Nested folders, FIFOs, sockets and devices are ignored: a client
 *          can only store plain files, and opening a FIFO would block a
 *          worker thread.
 *
 * Error policy
 *   A problem with ONE folder or file (unreadable, vanished, bad name) skips
 *   that item with a warning, so one bad entry cannot take the service down.
 *   Only a problem with root itself, or running out of memory, fails the
 *   load, or fails the re-scan, which then changes nothing. A topic whose
 *   folder cannot be read completely is skipped as a whole: serving a partial
 *   file list would let a client "succeed" with files missing. Because it is
 *   not added, the next re-scan tries it again.
 *
 * Adding a topic while the server runs: a folder is snapshotted the moment a
 * re-scan first sees it (files added to it later are not picked up). So
 * prepare it under a name starting with '.', which is ignored, and rename()
 * it into place, which is atomic.
 *
 * strerror() is only used on error paths. glibc >= 2.32 documents it as
 * MT-Safe, and load runs before any worker thread exists.
 */
#include "topic_map.h"
#include "fds.h"
#include "log.h"
#include "util.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>

/* Rate limit of topic_find_or_rescan(), in the unit of now_ns(). */
#define RESCAN_MIN_NS ((int64_t)FDS_RESCAN_MIN_MS * 1000000)

/* First capacity of the growing arrays; doubled whenever they are full. */
#define INITIAL_CAP 64

/* Result of load_topic(). */
enum load_status { LOAD_NOMEM = -1, LOAD_SKIPPED = 0, LOAD_OK = 1 };

/*
 * The pthread lock calls fail only on programming errors: an uninitialised or
 * destroyed lock, or a thread locking what it already holds. Carrying on would
 * risk reading the topic array while it is being swapped, so stop loudly.
 */
static void lock_failed(const char *what, int err)
{
    fprintf(stderr, "topic_map: %s failed: %s\n", what, strerror(err));
    abort();
}

#define LOCK_OK(call) do {                                                     \
        int lock_rc = (call);                                                  \
        if (lock_rc != 0)                                                      \
            lock_failed(#call, lock_rc);                                       \
    } while (0)

/*
 * Copy s into buf for a log line, with every control byte replaced by '?'.
 * Folder and file names come from the disk, and a name containing ESC or a
 * newline must not be able to inject terminal escape codes or fake log lines.
 */
static const char *printable(const char *s, char *buf, size_t cap)
{
    size_t i;

    for (i = 0; s[i] != '\0' && i + 1 < cap; i++) {
        unsigned char c = (unsigned char)s[i];
        buf[i] = (c < 0x20 || c == 0x7f) ? '?' : (char)c;
    }
    buf[i] = '\0';
    return buf;
}

/* ---- comparators: strcmp() order everywhere, so the order never depends on the locale ---- */

static int cmp_file_entry(const void *a, const void *b)
{
    return strcmp(((const file_entry_t *)a)->name, ((const file_entry_t *)b)->name);
}

static int cmp_name_file(const void *key, const void *elem)
{
    return strcmp((const char *)key, ((const file_entry_t *)elem)->name);
}

static int cmp_topic_ptr(const void *a, const void *b)
{
    return strcmp((*(topic_t *const *)a)->key, (*(topic_t *const *)b)->key);
}

static int cmp_key_topic_ptr(const void *key, const void *elem)
{
    return strcmp((const char *)key, (*(topic_t *const *)elem)->key);
}

/* Exact, case-sensitive bsearch in a sorted pointer array. NULL if absent. */
static topic_t *find_exact(topic_t *const *arr, size_t n, const char *key)
{
    topic_t *const *hit;

    if (n == 0)                 /* arr may then be NULL, which bsearch() must not get */
        return NULL;
    hit = bsearch(key, arr, n, sizeof(*arr), cmp_key_topic_ptr);
    return hit != NULL ? *hit : NULL;
}

/*
 * 1 if a topic with exactly this key is already in the map. Called by
 * scan_root(), whose caller holds rescan_mu, so the array cannot change under
 * us anyway. The read lock is still taken so this function would stay correct
 * on its own.
 */
static int key_present(topic_map_t *m, const char *key)
{
    int found;

    LOCK_OK(pthread_rwlock_rdlock(&m->lock));
    found = find_exact(m->topics, m->n_topics, key) != NULL;
    LOCK_OK(pthread_rwlock_unlock(&m->lock));
    return found;
}

/* Free a topic that was never published, or every topic in topic_map_free(). */
static void free_topic(topic_t *t)
{
    free(t->files);
    free(t);
}

/*
 * Build one topic_t from the folder root/key. topic_map_load() and
 * topic_map_rescan() both use it (through scan_root()), so a topic is defined
 * the same way whenever it appears.
 *
 * Returns LOAD_OK and sets *out (the topic is still private to the caller),
 * LOAD_SKIPPED after a warning (the folder vanished, cannot be read, ...), or
 * LOAD_NOMEM (the caller aborts the whole load or re-scan).
 */
static enum load_status load_topic(const char *root, const char *key, topic_t **out)
{
    char path[PATH_MAX];
    enum load_status status = LOAD_OK;
    unsigned skipped_unsafe = 0;
    size_t n = 0, cap = 0, key_len = strlen(key);
    struct dirent *de;
    topic_t *t;
    DIR *d;
    int dfd;

    *out = NULL;
    /* topic_is_valid() already limits the key to FDS_TOPIC_MAX bytes; re-check
     * here so the copy below is visibly in bounds. */
    if (key_len >= sizeof(t->key) || path_join(path, sizeof(path), root, key) != 0) {
        log_warn("topic '%s': name or path too long, skipped", key);
        return LOAD_SKIPPED;
    }
    t = calloc(1, sizeof(*t));
    if (t == NULL)
        return LOAD_NOMEM;
    memcpy(t->key, key, key_len + 1);

    /* The map value is the canonical absolute path. Every later open() of a
     * file of this topic is abs_path + "/" + a name from files[]. */
    if (realpath(path, t->abs_path) == NULL) {
        log_warn("topic '%s': cannot resolve %s: %s; skipped", key, path, strerror(errno));
        free(t);
        return LOAD_SKIPPED;
    }
    d = opendir(t->abs_path);
    if (d == NULL) {
        log_warn("topic '%s': cannot open folder %s: %s; skipped",
                 key, t->abs_path, strerror(errno));
        free(t);
        return LOAD_SKIPPED;
    }
    dfd = dirfd(d);
    if (dfd < 0) {
        log_warn("topic '%s': dirfd: %s; skipped", key, strerror(errno));
        status = LOAD_SKIPPED;
        goto done;
    }

    for (;;) {
        const char *name;
        struct stat st;
        file_entry_t *fe;
        size_t name_len;

        errno = 0;                      /* readdir() returns NULL both at the end and on error */
        de = readdir(d);
        if (de == NULL) {
            if (errno != 0) {
                log_warn("topic '%s': error while reading %s: %s; skipped",
                         key, t->abs_path, strerror(errno));
                status = LOAD_SKIPPED;
            }
            break;
        }
        name = de->d_name;
        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
            continue;
        /* Checked before stat(): an unsafe name is never touched, and never
         * printed, because it may contain control characters. */
        if (!name_is_safe(name)) {
            skipped_unsafe++;
            continue;
        }
        /* fstatat(..., 0) follows symlinks: a link to a regular file is served
         * as that file (with the target's size), and a dangling link fails here. */
        if (fstatat(dfd, name, &st, 0) != 0) {
            log_warn("topic '%s': cannot stat '%s': %s; file skipped", key, name, strerror(errno));
            continue;
        }
        if (!S_ISREG(st.st_mode))
            continue;                   /* nested folder, FIFO, socket, device */
        name_len = strlen(name);
        if (name_len >= sizeof(fe->name))
            continue;                   /* cannot happen: name_is_safe() limits it to FDS_NAME_MAX */
        if (n == UINT32_MAX) {          /* n_files is a uint32_t */
            log_warn("topic '%s': more than %u files; skipped", key, (unsigned)UINT32_MAX);
            status = LOAD_SKIPPED;
            break;
        }
        if (n == cap) {
            size_t ncap = cap != 0 ? 2 * cap : INITIAL_CAP;
            file_entry_t *nf = realloc(t->files, ncap * sizeof(*nf));
            if (nf == NULL) {
                status = LOAD_NOMEM;
                break;
            }
            t->files = nf;
            cap = ncap;
        }
        fe = &t->files[n++];
        memcpy(fe->name, name, name_len + 1);
        fe->size = (uint64_t)st.st_size;
    }

done:
    if (closedir(d) != 0)
        log_warn("topic '%s': closedir: %s", key, strerror(errno));
    if (status != LOAD_OK) {
        free_topic(t);
        return status;
    }
    if (skipped_unsafe > 0)
        log_warn("topic '%s': %u entr%s skipped because the name is unsafe to create on a "
                 "client (leading '.', control character or longer than %u bytes)",
                 key, skipped_unsafe, skipped_unsafe == 1 ? "y" : "ies", FDS_NAME_MAX);

    /* Sorting by name is what makes "file index i" mean the same file in every
     * MANIFEST, checkpoint and FETCH. readdir() order is arbitrary. */
    if (n > 1)
        qsort(t->files, n, sizeof(*t->files), cmp_file_entry);
    t->n_files = (uint32_t)n;
    for (size_t i = 0; i < n; i++) {
        t->total_bytes += t->files[i].size;
        t->total_chunks += fds_chunks_of(t->files[i].size);
    }
    log_info("topic '%s': %u files, %llu bytes, %llu chunks (%s)",
             t->key, t->n_files, (unsigned long long)t->total_bytes,
             (unsigned long long)t->total_chunks, t->abs_path);
    *out = t;
    return LOAD_OK;
}

/* Free an array of topics that were never published, and the array itself. */
static void free_private_topics(topic_t **arr, size_t n)
{
    for (size_t i = 0; i < n; i++)
        free_topic(arr[i]);
    free(arr);
}

/*
 * Read m->root once and load every topic folder whose exact key is not in the
 * map yet. At load time the map is still empty, so this loads everything;
 * during a re-scan it loads only the folders created since. The new topics
 * are returned in *out, sorted by key and still private to the caller (*out
 * may be NULL when *out_n is 0).
 *
 * Returns 0, or -1 with errno set (root unreadable, out of memory). On -1
 * nothing is left allocated.
 */
static int scan_root(topic_map_t *m, topic_t ***out, size_t *out_n)
{
    char shown[FDS_NAME_MAX + 1];
    topic_t **arr = NULL;
    size_t n = 0, cap = 0;
    struct dirent *de;
    int err = 0, dfd;
    DIR *d;

    *out = NULL;
    *out_n = 0;
    d = opendir(m->root);
    if (d == NULL)
        return -1;
    dfd = dirfd(d);
    if (dfd < 0) {
        err = errno;
        goto fail;
    }

    for (;;) {
        const char *name;
        struct stat st;
        topic_t *t;
        enum load_status rc;

        errno = 0;
        de = readdir(d);
        if (de == NULL) {
            err = errno;                /* 0 = end of the directory */
            break;
        }
        name = de->d_name;
        if (name[0] == '.')
            continue;                   /* ".", "..", hidden or staging folders: silently */
        if (fstatat(dfd, name, &st, 0) != 0) {
            log_warn("topics root: cannot stat '%s': %s; skipped",
                     printable(name, shown, sizeof(shown)), strerror(errno));
            continue;
        }
        if (!S_ISDIR(st.st_mode))
            continue;                   /* a plain file next to the topics is not a topic */
        if (!topic_is_valid(name)) {
            log_warn("topics root: folder '%s' (%zu bytes) is not a valid topic name "
                     "(1..%u bytes, no control characters); skipped",
                     printable(name, shown, sizeof(shown)), strlen(name), FDS_TOPIC_MAX);
            continue;
        }
        if (key_present(m, name))
            continue;                   /* published already: never reloaded (deviation 3) */

        rc = load_topic(m->root, name, &t);
        if (rc == LOAD_NOMEM) {
            err = ENOMEM;
            break;
        }
        if (rc == LOAD_SKIPPED)
            continue;
        if (n == cap) {
            size_t ncap = cap != 0 ? 2 * cap : INITIAL_CAP;
            topic_t **na = realloc(arr, ncap * sizeof(*na));
            if (na == NULL) {
                free_topic(t);
                err = ENOMEM;
                break;
            }
            arr = na;
            cap = ncap;
        }
        arr[n++] = t;
    }
    if (closedir(d) != 0)
        log_warn("topics root: closedir: %s", strerror(errno));
    if (err != 0) {
        free_private_topics(arr, n);
        errno = err;
        return -1;
    }

    if (n > 1) {
        /* Sort, then drop duplicate keys. A duplicate can only appear if
         * readdir() returned one name twice while the folder was being
         * changed. The copies were never published, so they can be freed. */
        size_t w = 1;

        qsort(arr, n, sizeof(*arr), cmp_topic_ptr);
        for (size_t i = 1; i < n; i++) {
            if (strcmp(arr[i]->key, arr[w - 1]->key) == 0)
                free_topic(arr[i]);
            else
                arr[w++] = arr[i];
        }
        n = w;
    }
    *out = arr;
    *out_n = n;
    return 0;

fail:
    if (closedir(d) != 0)
        log_warn("topics root: closedir: %s", strerror(errno));
    errno = err;
    return -1;
}

int topic_map_load(topic_map_t *m, const char *root)
{
    struct stat st;
    topic_t **arr;
    size_t n;
    int rc;

    /* Zeroing first gives root[0] == '\0', which marks "nothing to free" for
     * topic_map_free() on every failure path below. */
    memset(m, 0, sizeof(*m));
    if (root == NULL) {
        fprintf(stderr, "error: no topics_root_dir given\n");
        return -1;
    }
    if (realpath(root, m->root) == NULL) {
        fprintf(stderr, "error: topics_root_dir '%s': %s\n", root, strerror(errno));
        m->root[0] = '\0';
        return -1;
    }
    if (stat(m->root, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "error: topics_root_dir '%s' is not a directory\n", root);
        m->root[0] = '\0';
        return -1;
    }
    rc = pthread_rwlock_init(&m->lock, NULL);
    if (rc != 0) {
        fprintf(stderr, "error: pthread_rwlock_init: %s\n", strerror(rc));
        m->root[0] = '\0';
        return -1;
    }
    rc = pthread_mutex_init(&m->rescan_mu, NULL);
    if (rc != 0) {
        fprintf(stderr, "error: pthread_mutex_init: %s\n", strerror(rc));
        pthread_rwlock_destroy(&m->lock);
        m->root[0] = '\0';
        return -1;
    }

    if (scan_root(m, &arr, &n) != 0) {
        fprintf(stderr, "error: cannot read topics_root_dir '%s': %s\n", root, strerror(errno));
        pthread_mutex_destroy(&m->rescan_mu);
        pthread_rwlock_destroy(&m->lock);
        m->root[0] = '\0';
        return -1;
    }
    m->topics = arr;
    m->n_topics = n;
    /* Back-date the rate limiter. The limit exists to stop a flood of 404
     * requests from re-reading the disk many times per second, not to delay
     * the first request for a topic created just after start-up. */
    m->last_rescan_ns = now_ns() - RESCAN_MIN_NS;

    if (n == 0)
        log_warn("no topic folders in %s (every request will get 404 until one is added)", m->root);
    else
        log_info("%zu topic(s) loaded from %s", n, m->root);
    return 0;
}

/*
 * Case-insensitive fallback: the assignment writes "Cane" while the folder is
 * "cane". If several keys differ only in case (e.g. "Mix" and "mix") and none
 * matches exactly, the first in strcmp() order wins, which is the same answer
 * every time ("MIX" -> "Mix", because 'M' < 'm'). strcasecmp() folds only
 * ASCII letters here (the server never calls setlocale()). Bytes >= 0x80, as
 * in UTF-8 names, must match exactly.
 */
const topic_t *topic_find(topic_map_t *m, const char *key)
{
    const topic_t *t;

    if (m == NULL || key == NULL)
        return NULL;
    LOCK_OK(pthread_rwlock_rdlock(&m->lock));
    t = find_exact(m->topics, m->n_topics, key);
    for (size_t i = 0; t == NULL && i < m->n_topics; i++)
        if (strcasecmp(m->topics[i]->key, key) == 0)
            t = m->topics[i];
    LOCK_OK(pthread_rwlock_unlock(&m->lock));
    /* Safe to use without the lock: a published topic_t is never changed or freed. */
    return t;
}

/*
 * The body of a re-scan. The caller holds rescan_mu, which makes this thread
 * the only one that can change m->topics. topic_map_rescan() and
 * topic_find_or_rescan() both call it. The second must hold the mutex across
 * its rate-limit check AND the scan, so it cannot simply call the locking
 * public function.
 */
static int rescan_locked(topic_map_t *m)
{
    topic_t **fresh, **merged, **old;
    size_t n_fresh, n_old;
    int rc;

    if (scan_root(m, &fresh, &n_fresh) != 0) {
        log_warn("re-scan of %s failed: %s; topic map unchanged", m->root, strerror(errno));
        rc = -1;
        goto out;
    }
    if (n_fresh == 0) {
        free(fresh);
        rc = 0;
        goto out;
    }

    /* Only a re-scan changes these two fields, and we are the only re-scan,
     * so the snapshot stays valid until our own swap below. */
    LOCK_OK(pthread_rwlock_rdlock(&m->lock));
    old = m->topics;
    n_old = m->n_topics;
    LOCK_OK(pthread_rwlock_unlock(&m->lock));

    /* Build the new array before taking the write lock, so readers are
     * blocked for two stores only, never for malloc or qsort. */
    merged = malloc((n_old + n_fresh) * sizeof(*merged));
    if (merged == NULL) {
        log_warn("re-scan of %s: out of memory; topic map unchanged", m->root);
        free_private_topics(fresh, n_fresh);
        rc = -1;
        goto out;
    }
    if (n_old > 0)
        memcpy(merged, old, n_old * sizeof(*merged));
    memcpy(merged + n_old, fresh, n_fresh * sizeof(*merged));
    qsort(merged, n_old + n_fresh, sizeof(*merged), cmp_topic_ptr);

    LOCK_OK(pthread_rwlock_wrlock(&m->lock));
    m->topics = merged;
    m->n_topics = n_old + n_fresh;
    LOCK_OK(pthread_rwlock_unlock(&m->lock));

    /* Free the OLD POINTER ARRAY only, never the topic_t objects it pointed
     * to: they are in the new array too, and threads may be using them
     * (deviation 3). No reader can still be looking at the old array. Readers
     * touch m->topics only under the read lock, and our write lock waited
     * for all of them to leave. */
    free(old);
    for (size_t i = 0; i < n_fresh; i++)
        log_info("re-scan: new topic '%s' (%u files)", fresh[i]->key, fresh[i]->n_files);
    free(fresh);                        /* the temporary array, not the topics */
    rc = (int)n_fresh;

out:
    /* A failed re-scan counts as well, so a broken root under a 404 flood is
     * still read at most once per FDS_RESCAN_MIN_MS. */
    m->last_rescan_ns = now_ns();
    return rc;
}

const topic_t *topic_find_or_rescan(topic_map_t *m, const char *key)
{
    const topic_t *t = topic_find(m, key);

    if (t != NULL || m == NULL || key == NULL)
        return t;
    /* A key that no folder could have (too long, '/', control characters)
     * can never appear by re-scanning, so it costs no disk access. */
    if (!topic_is_valid(key))
        return NULL;

    LOCK_OK(pthread_mutex_lock(&m->rescan_mu));
    /* Rate limit: under a flood of requests for an unknown topic, at most one
     * re-scan per FDS_RESCAN_MIN_MS. The others wait here for the running
     * re-scan (whose result they want anyway) and then look again. */
    if (now_ns() - m->last_rescan_ns >= RESCAN_MIN_NS)
        (void)rescan_locked(m);
    LOCK_OK(pthread_mutex_unlock(&m->rescan_mu));
    return topic_find(m, key);
}

/* Direct calls are not rate limited. They still reset the limiter. */
int topic_map_rescan(topic_map_t *m)
{
    int rc;

    if (m == NULL || m->root[0] == '\0')
        return -1;
    LOCK_OK(pthread_mutex_lock(&m->rescan_mu));
    rc = rescan_locked(m);
    LOCK_OK(pthread_mutex_unlock(&m->rescan_mu));
    return rc;
}

/* Exact, case-sensitive: the name always comes from our own MANIFEST or checkpoint. */
int64_t topic_file_index(const topic_t *t, const char *name)
{
    const file_entry_t *fe;

    if (t == NULL || name == NULL || t->n_files == 0)
        return -1;
    fe = bsearch(name, t->files, t->n_files, sizeof(*t->files), cmp_name_file);
    return fe != NULL ? (int64_t)(fe - t->files) : -1;
}

/*
 * Only at process exit, when no other thread uses the map any more. It is
 * safe after a failed topic_map_load(), and a second call does nothing.
 * root[0] == '\0' means "nothing loaded".
 */
void topic_map_free(topic_map_t *m)
{
    int rc;

    if (m == NULL || m->root[0] == '\0')
        return;
    for (size_t i = 0; i < m->n_topics; i++)
        free_topic(m->topics[i]);
    free(m->topics);
    m->topics = NULL;
    m->n_topics = 0;
    if ((rc = pthread_rwlock_destroy(&m->lock)) != 0)
        log_warn("topic map: pthread_rwlock_destroy: %s", strerror(rc));
    if ((rc = pthread_mutex_destroy(&m->rescan_mu)) != 0)
        log_warn("topic map: pthread_mutex_destroy: %s", strerror(rc));
    m->root[0] = '\0';
}
