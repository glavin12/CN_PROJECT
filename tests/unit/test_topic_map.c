/*
 * test_topic_map.c - unit tests for topic_map.c (design sections 2, 2.1 and 7,
 * deviation 3).
 *
 * Builds a throw-away topics root under /tmp with real topics plus every kind
 * of noise the loader must ignore. It then checks loading, sorting, sizes and
 * chunk counts, lookups, re-scans (rate limit, pointer stability) and lookups
 * running concurrently with re-scans. Run the concurrency part under TSan:
 *   make -C tests/unit run SAN=thread BUILD=build-tsan
 *
 * Expected warnings on stderr: an unsafe-name count and a dangling symlink
 * for "cane", and the 64-character folder name at every scan of the root.
 */
#include "unit.h"
#include "fds.h"
#include "util.h"
#include "topic_map.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static char g_tmp[] = "/tmp/fds_tm_XXXXXX";    /* created by mkdtemp() */
static char g_root[PATH_MAX];                  /* its realpath(): what the map must report */
static char g_long_name[FDS_TOPIC_MAX + 2];    /* 64 characters: one too long for a topic */

/* ---- building the tree; any setup failure ends the test at once ---- */

static void die(const char *what, const char *path)
{
    fprintf(stderr, "test setup failed: %s %s: %s\n", what, path, strerror(errno));
    exit(1);
}

static void at(char *out, const char *rel)
{
    if (path_join(out, PATH_MAX, g_root, rel) != 0) {
        fprintf(stderr, "test setup failed: path too long: %s\n", rel);
        exit(1);
    }
}

static void mk_dir(const char *rel)
{
    char p[PATH_MAX];

    at(p, rel);
    if (mkdir(p, 0755) != 0)
        die("mkdir", p);
}

/* A file of exactly size bytes (sparse: only st_size matters to the map). */
static void mk_file(const char *rel, off_t size)
{
    char p[PATH_MAX];
    int fd;

    at(p, rel);
    fd = open(p, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        die("open", p);
    if (ftruncate(fd, size) != 0)
        die("ftruncate", p);
    if (close(fd) != 0)
        die("close", p);
}

/* rel becomes a symlink to the absolute path of target_rel (which may not exist). */
static void mk_link(const char *target_rel, const char *rel)
{
    char p[PATH_MAX], target[PATH_MAX];

    at(p, rel);
    at(target, target_rel);
    if (symlink(target, p) != 0)
        die("symlink", p);
}

static void mk_chmod(const char *rel, mode_t mode)
{
    char p[PATH_MAX];

    at(p, rel);
    if (chmod(p, mode) != 0)
        die("chmod", p);
}

/* Create the topic folder rel holding one file, atomically: build it under a
 * hidden staging name (ignored by the scanner), then rename() it into place.
 * A re-scan therefore never sees it half-built. */
static void publish_topic(const char *rel, off_t file_size)
{
    char from[PATH_MAX], to[PATH_MAX];

    mk_dir(".stage");
    mk_file(".stage/f.jpg", file_size);
    at(from, ".stage");
    at(to, rel);
    if (rename(from, to) != 0)
        die("rename", to);
}

static void build_tree(void)
{
    /* the topic "cane": all the file-level cases */
    mk_dir("cane");
    mk_file("cane/0byte.bin", 0);
    mk_file("cane/a.jpg", 100);
    mk_file("cane/big64k.bin", 65536);
    mk_file("cane/big64k1.bin", 65537);
    mk_file("cane/caf\xc3\xa9.jpg", 7);            /* UTF-8 name */
    mk_file("cane/with space.jpg", 10);
    mk_file("rootfile.txt", 42);                   /* noise at root level, and a link target */
    mk_link("rootfile.txt", "cane/link.jpg");      /* symlink to a regular file: included */
    mk_link("nowhere", "cane/dangling.jpg");       /* dangling symlink: stat fails, skipped */
    mk_file("cane/.DS_Store", 10);                 /* unsafe: leading '.' */
    mk_file("cane/bad\nname.jpg", 3);              /* unsafe: control character */
    mk_dir("cane/nested");                         /* nested folders are ignored */
    mk_file("cane/nested/inner.jpg", 5);

    /* mixed case, and strcmp order inside a topic ('Y' < 'x') */
    mk_dir("Gatto");
    mk_file("Gatto/x.jpg", 5);
    mk_file("Gatto/Y.jpg", 6);

    mk_dir("empty");                               /* a topic with no files */
    mk_dir("Mix");                                 /* two keys differing only in case */
    mk_dir("mix");
    mk_link("Gatto", "alias");                     /* symlink to a folder: a topic, abs_path resolved */

    mk_dir(".hidden");                             /* hidden folder: not a topic */
    mk_file(".hidden/h.jpg", 1);
    memset(g_long_name, 'a', FDS_TOPIC_MAX + 1);   /* 64-character name: not a valid topic */
    g_long_name[FDS_TOPIC_MAX + 1] = '\0';
    mk_dir(g_long_name);
}

/* Linear lookup that does not use the code under test. */
static const topic_t *by_key(const topic_map_t *m, const char *key)
{
    for (size_t i = 0; i < m->n_topics; i++)
        if (strcmp(m->topics[i]->key, key) == 0)
            return m->topics[i];
    return NULL;
}

/* 1 if the topic keys are strictly increasing in strcmp order (sorted, no duplicates). */
static int strictly_sorted(const topic_map_t *m)
{
    for (size_t i = 1; i < m->n_topics; i++)
        if (strcmp(m->topics[i - 1]->key, m->topics[i]->key) >= 0)
            return 0;
    return 1;
}

/* ---- load: what is a topic / a file, order, sizes ---- */

static void test_layout(topic_map_t *m)
{
    static const char *const want_keys[] = { "Gatto", "Mix", "alias", "cane", "empty", "mix" };
    static const struct { const char *name; uint64_t size; } want_cane[] = {
        { "0byte.bin", 0 }, { "a.jpg", 100 }, { "big64k.bin", 65536 },
        { "big64k1.bin", 65537 }, { "caf\xc3\xa9.jpg", 7 }, { "link.jpg", 42 },
        { "with space.jpg", 10 },
    };
    const size_t n_keys = sizeof(want_keys) / sizeof(want_keys[0]);
    const size_t n_cane = sizeof(want_cane) / sizeof(want_cane[0]);
    const topic_t *cane, *gatto, *alias;
    char p[PATH_MAX];

    CHECK_STR(m->root, g_root);
    CHECK_EQ(m->n_topics, n_keys);
    for (size_t i = 0; i < n_keys && i < m->n_topics; i++) {
        CHECK_STR(m->topics[i]->key, want_keys[i]);
        if (strcmp(want_keys[i], "alias") != 0) {
            at(p, want_keys[i]);
            CHECK_STR(m->topics[i]->abs_path, p);
        }
    }
    CHECK(strictly_sorted(m));

    cane = by_key(m, "cane");
    CHECK(cane != NULL);
    if (cane != NULL) {
        CHECK_EQ(cane->n_files, n_cane);
        for (size_t i = 0; i < n_cane && i < cane->n_files; i++) {
            CHECK_STR(cane->files[i].name, want_cane[i].name);
            CHECK_EQ(cane->files[i].size, want_cane[i].size);
        }
        /* 0 + 100 + 65536 + 65537 + 7 + 42 + 10 bytes;
         * chunks: 0-byte -> 0, 65536 -> 1, 65537 -> 2, every other file -> 1 */
        CHECK_EQ(cane->total_bytes, 131232);
        CHECK_EQ(cane->total_chunks, 7);
    }
    CHECK_EQ(fds_chunks_of(0), 0);
    CHECK_EQ(fds_chunks_of(1), 1);
    CHECK_EQ(fds_chunks_of(65536), 1);
    CHECK_EQ(fds_chunks_of(65537), 2);

    gatto = by_key(m, "Gatto");
    CHECK(gatto != NULL);
    if (gatto != NULL && gatto->n_files == 2) {
        CHECK_STR(gatto->files[0].name, "Y.jpg");      /* strcmp order: 'Y' (0x59) < 'x' (0x78) */
        CHECK_STR(gatto->files[1].name, "x.jpg");
        CHECK_EQ(gatto->total_bytes, 11);
        CHECK_EQ(gatto->total_chunks, 2);
    } else {
        CHECK(0);
    }
    alias = by_key(m, "alias");
    CHECK(alias != NULL);
    if (alias != NULL && gatto != NULL) {
        CHECK_STR(alias->abs_path, gatto->abs_path);    /* realpath() resolved the link */
        CHECK_EQ(alias->n_files, 2);
    }
    for (int i = 0; i < 3; i++) {
        const char *k = (const char *[]){ "empty", "Mix", "mix" }[i];
        const topic_t *t = by_key(m, k);
        CHECK(t != NULL);
        if (t != NULL) {
            CHECK_EQ(t->n_files, 0);
            CHECK_EQ(t->total_bytes, 0);
            CHECK_EQ(t->total_chunks, 0);
        }
    }
    CHECK(by_key(m, ".hidden") == NULL);
    CHECK(by_key(m, "rootfile.txt") == NULL);
    CHECK(by_key(m, g_long_name) == NULL);
}

/* ---- topic_find and topic_file_index ---- */

static void test_find(topic_map_t *m)
{
    const topic_t *cane = by_key(m, "cane"), *gatto = by_key(m, "Gatto");
    const topic_t *Mix = by_key(m, "Mix"), *mix = by_key(m, "mix"), *empty = by_key(m, "empty");

    CHECK(cane != NULL && gatto != NULL && Mix != NULL && mix != NULL && empty != NULL);
    if (cane == NULL || gatto == NULL || Mix == NULL || mix == NULL || empty == NULL)
        return;

    CHECK(topic_find(m, "cane") == cane);              /* exact */
    CHECK(topic_find(m, "Gatto") == gatto);
    CHECK(topic_find(m, "CANE") == cane);              /* case-insensitive fallback */
    CHECK(topic_find(m, "Cane") == cane);
    CHECK(topic_find(m, "gatto") == gatto);
    CHECK(topic_find(m, "gAtTo") == gatto);
    CHECK(topic_find(m, "mix") == mix);                /* an exact match beats a case-insensitive one */
    CHECK(topic_find(m, "Mix") == Mix);
    CHECK(topic_find(m, "MIX") == Mix);                /* tie: first in strcmp order ('M' < 'm') */
    CHECK(topic_find(m, "cavallo") == NULL);           /* unknown */
    CHECK(topic_find(m, "can") == NULL);               /* a prefix is not a match */
    CHECK(topic_find(m, "cane/") == NULL);
    CHECK(topic_find(m, "") == NULL);
    CHECK(topic_find(m, ".hidden") == NULL);
    CHECK(topic_find(m, "rootfile.txt") == NULL);
    CHECK(topic_find(m, g_long_name) == NULL);
    CHECK(topic_find(m, NULL) == NULL);

    CHECK_EQ(topic_file_index(cane, "0byte.bin"), 0);
    CHECK_EQ(topic_file_index(cane, "big64k1.bin"), 3);
    CHECK_EQ(topic_file_index(cane, "caf\xc3\xa9.jpg"), 4);
    CHECK_EQ(topic_file_index(cane, "link.jpg"), 5);
    CHECK_EQ(topic_file_index(cane, "with space.jpg"), 6);
    CHECK_EQ(topic_file_index(cane, "A.jpg"), -1);     /* file names are case-sensitive */
    CHECK_EQ(topic_file_index(cane, ".DS_Store"), -1);
    CHECK_EQ(topic_file_index(cane, "bad\nname.jpg"), -1);
    CHECK_EQ(topic_file_index(cane, "dangling.jpg"), -1);
    CHECK_EQ(topic_file_index(cane, "nested"), -1);
    CHECK_EQ(topic_file_index(cane, "inner.jpg"), -1);
    CHECK_EQ(topic_file_index(cane, ""), -1);
    CHECK_EQ(topic_file_index(cane, NULL), -1);
    CHECK_EQ(topic_file_index(empty, "a.jpg"), -1);
    CHECK_EQ(topic_file_index(NULL, "a.jpg"), -1);
}

/* ---- re-scan: new topics appear, old pointers stay valid, rate limit ---- */

static void test_rescan(topic_map_t *m)
{
    const topic_t *cane = topic_find(m, "cane"), *t;
    const file_entry_t *cane_files = cane != NULL ? cane->files : NULL;
    size_t n0 = m->n_topics;
    int64_t t0 = m->last_rescan_ns;

    /* lookups that need no re-scan leave the rate limiter alone */
    CHECK(topic_find_or_rescan(m, "cane") == cane);
    CHECK(topic_find_or_rescan(m, "a/b") == NULL);     /* can never be a folder: no re-scan */
    CHECK(topic_find_or_rescan(m, g_long_name) == NULL);
    CHECK(m->last_rescan_ns == t0);

    /* a new folder is found by topic_find_or_rescan, never by plain topic_find */
    mk_dir("nuovo");
    mk_file("nuovo/n1.jpg", 3);
    CHECK(topic_find(m, "nuovo") == NULL);
    t = topic_find_or_rescan(m, "nuovo");              /* the load back-dated the limiter */
    CHECK(t != NULL);
    if (t != NULL) {
        CHECK_STR(t->key, "nuovo");
        CHECK_EQ(t->n_files, 1);
        CHECK_EQ(t->total_bytes, 3);
    }
    CHECK(m->last_rescan_ns != t0);
    CHECK_EQ(m->n_topics, n0 + 1);
    CHECK(topic_find(m, "NUOVO") == t);
    CHECK(strictly_sorted(m));

    /* deviation 3: what a thread obtained before the re-scan is untouched
     * (ASan would also report a use-after-free here) */
    CHECK(topic_find(m, "cane") == cane);
    if (cane != NULL) {
        CHECK(cane->files == cane_files);
        CHECK_STR(cane->key, "cane");
        CHECK_EQ(cane->n_files, 7);
        CHECK_EQ(cane->total_bytes, 131232);
        CHECK_STR(cane->files[6].name, "with space.jpg");
    }

    /* rate limit: a second re-scan within FDS_RESCAN_MIN_MS does not happen */
    mk_dir("secondo");
    CHECK(topic_find_or_rescan(m, "secondo") == NULL);
    CHECK_EQ(m->n_topics, n0 + 1);
    sleep_ms(FDS_RESCAN_MIN_MS + 100);
    CHECK(topic_find_or_rescan(m, "secondo") != NULL);
    CHECK_EQ(m->n_topics, n0 + 2);

    /* topic_map_rescan returns the number added and is not rate limited */
    mk_dir("terzo");
    mk_dir("quarto");
    mk_file("quarto/q.jpg", 70000);
    CHECK_EQ(topic_map_rescan(m), 2);
    CHECK_EQ(topic_map_rescan(m), 0);
    CHECK_EQ(m->n_topics, n0 + 4);
    t = topic_find(m, "quarto");
    CHECK(t != NULL && t->total_chunks == 2);
    CHECK(topic_find(m, "terzo") != NULL);
    CHECK(strictly_sorted(m));

    /* a topic is an immutable snapshot: files added later are not picked up */
    mk_file("cane/late.jpg", 5);
    CHECK_EQ(topic_map_rescan(m), 0);
    CHECK_EQ(topic_file_index(cane, "late.jpg"), -1);
    CHECK(topic_find(m, "cane") == cane);
}

/* ---- an unreadable topic folder is skipped, and picked up once readable ---- */

static void test_unreadable(topic_map_t *m)
{
    const topic_t *t;
    char p[PATH_MAX];
    topic_map_t bad;

    if (geteuid() == 0) {
        printf("test_unreadable: skipped (root can read mode-000 folders)\n");
        return;
    }
    mk_dir("locked");
    mk_file("locked/l.jpg", 1);
    mk_chmod("locked", 0);
    CHECK_EQ(topic_map_rescan(m), 0);                  /* warning, skipped */
    CHECK(topic_find(m, "locked") == NULL);
    mk_chmod("locked", 0755);
    CHECK_EQ(topic_map_rescan(m), 1);                  /* retried and added */
    t = topic_find(m, "locked");
    CHECK(t != NULL && t->n_files == 1);

    /* an unreadable ROOT fails the load */
    mk_dir("noread");
    mk_chmod("noread", 0);
    at(p, "noread");
    CHECK_EQ(topic_map_load(&bad, p), -1);
    topic_map_free(&bad);
    mk_chmod("noread", 0755);
    CHECK_EQ(topic_map_rescan(m), 1);                  /* "noread" is an (empty) topic folder too */
}

/* ---- concurrency: lookups while topics are added (run under TSan) ---- */

#define CONC_READERS 8
#define CONC_PHASE   20                                /* topics per phase */
#define CONC_TOPICS  (2 * CONC_PHASE)
#define CONC_SIZE    1234

struct reader {
    pthread_t      tid;
    int            id;
    topic_map_t   *m;
    const topic_t *cane, *gatto;
    long           lookups;
    long           errors;
};

static atomic_int g_stop;

/* Each reader checks only its own counters (unit.h's are not thread-safe). */
static void *reader_main(void *arg)
{
    struct reader *r = arg;
    char key[32];

    while (!atomic_load(&g_stop)) {
        const topic_t *t;

        if (topic_find(r->m, "cane") != r->cane)           /* pointers never move */
            r->errors++;
        if (topic_find(r->m, "GATTO") != r->gatto)
            r->errors++;
        if (topic_find(r->m, "no_such_topic") != NULL)
            r->errors++;
        snprintf(key, sizeof(key), "conc_%02ld", r->lookups % CONC_TOPICS);
        /* Reader 0 also triggers (rate-limited) re-scans of its own, which
         * race with the main thread's re-scans on rescan_mu. */
        t = r->id == 0 ? topic_find_or_rescan(r->m, key) : topic_find(r->m, key);
        /* a topic, once visible, must be complete: it was filled before publication */
        if (t != NULL && (strcmp(t->key, key) != 0 || t->n_files != 1 ||
                          t->files[0].size != CONC_SIZE || t->total_chunks != 1))
            r->errors++;
        r->lookups++;
    }
    return NULL;
}

static void test_concurrency(topic_map_t *m)
{
    struct reader rd[CONC_READERS];
    size_t n0 = m->n_topics;
    long added = 0, lookups = 0, errors = 0;
    const topic_t *last = NULL;
    char rel[32];
    int rc;

    atomic_store(&g_stop, 0);
    for (int i = 0; i < CONC_READERS; i++) {
        rd[i] = (struct reader){ .id = i, .m = m, .cane = topic_find(m, "cane"),
                                 .gatto = topic_find(m, "Gatto") };
        if (pthread_create(&rd[i].tid, NULL, reader_main, &rd[i]) != 0) {
            fprintf(stderr, "test setup failed: pthread_create\n");
            exit(1);
        }
    }

    /* phase 1: the main thread adds topics and re-scans directly */
    for (int i = 0; i < CONC_PHASE; i++) {
        snprintf(rel, sizeof(rel), "conc_%02d", i);
        publish_topic(rel, CONC_SIZE);
        rc = topic_map_rescan(m);
        CHECK(rc == 0 || rc == 1);                         /* 0: reader 0 was faster */
        if (rc > 0)
            added += rc;
        sleep_ms(2);
    }
    /* phase 2: no direct re-scans; reader 0's topic_find_or_rescan must
     * pick the new folders up once the rate limit expires */
    for (int i = CONC_PHASE; i < CONC_TOPICS; i++) {
        snprintf(rel, sizeof(rel), "conc_%02d", i);
        publish_topic(rel, CONC_SIZE);
    }
    snprintf(rel, sizeof(rel), "conc_%02d", CONC_TOPICS - 1);
    for (int waited = 0; waited < 5000 && (last = topic_find(m, rel)) == NULL; waited += 10)
        sleep_ms(10);
    CHECK(last != NULL);

    atomic_store(&g_stop, 1);
    for (int i = 0; i < CONC_READERS; i++) {
        if (pthread_join(rd[i].tid, NULL) != 0) {
            fprintf(stderr, "test setup failed: pthread_join\n");
            exit(1);
        }
        lookups += rd[i].lookups;
        errors += rd[i].errors;
        CHECK(rd[i].lookups > 0);
    }
    rc = topic_map_rescan(m);                              /* stragglers, if any */
    CHECK(rc >= 0);
    printf("concurrency: %d readers, %ld lookups, %ld errors; main thread added %ld of %d topics\n",
           CONC_READERS, lookups, errors, added + (rc > 0 ? rc : 0), CONC_TOPICS);
    CHECK_EQ(errors, 0);
    CHECK_EQ(m->n_topics, n0 + CONC_TOPICS);               /* each folder added exactly once */
    CHECK(strictly_sorted(m));
    for (int i = 0; i < CONC_TOPICS; i++) {
        const topic_t *t;
        snprintf(rel, sizeof(rel), "conc_%02d", i);
        t = topic_find(m, rel);
        CHECK(t != NULL && t->n_files == 1);
    }
}

/* ---- load errors, and a root without topics ---- */

static void test_load_errors(void)
{
    topic_map_t bad;
    char p[PATH_MAX];

    CHECK_EQ(topic_map_load(&bad, "/nonexistent/fds_tm_missing"), -1);
    topic_map_free(&bad);                                  /* safe after a failed load */
    at(p, "rootfile.txt");
    CHECK_EQ(topic_map_load(&bad, p), -1);                 /* not a directory */
    topic_map_free(&bad);
    CHECK_EQ(topic_map_load(&bad, NULL), -1);
    topic_map_free(&bad);

    at(p, "empty");                                        /* zero topics: allowed (warning) */
    CHECK_EQ(topic_map_load(&bad, p), 0);
    CHECK_EQ(bad.n_topics, 0);
    CHECK(topic_find(&bad, "cane") == NULL);
    CHECK(topic_find_or_rescan(&bad, "cane") == NULL);
    CHECK_EQ(topic_map_rescan(&bad), 0);
    topic_map_free(&bad);
}

int main(void)
{
    topic_map_t m;
    char cmd[PATH_MAX + 16];
    int loaded;

    setenv("FDS_QUIET", "1", 1);       /* silence log_info; warnings still print */
    if (mkdtemp(g_tmp) == NULL)
        die("mkdtemp", g_tmp);
    if (realpath(g_tmp, g_root) == NULL)
        die("realpath", g_tmp);
    build_tree();

    loaded = topic_map_load(&m, g_tmp) == 0;
    CHECK(loaded);
    if (loaded) {
        test_layout(&m);
        test_find(&m);
        test_rescan(&m);
        test_unreadable(&m);
        test_concurrency(&m);
        topic_map_free(&m);
        CHECK(m.topics == NULL && m.n_topics == 0);
        topic_map_free(&m);            /* a second call does nothing */
    }
    test_load_errors();

    if (snprintf(cmd, sizeof(cmd), "rm -rf '%s'", g_tmp) < (int)sizeof(cmd))
        CHECK_EQ(system(cmd), 0);
    UNIT_DONE();
}
