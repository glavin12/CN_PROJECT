/*
 * test_checkpoint.c - unit tests for checkpoint.c (design section 5.4,
 * deviation 13). Works in a fresh mkdtemp() directory under $TMPDIR (or
 * /tmp) and removes it at the end.
 */
#define _GNU_SOURCE                      /* nftw() flags */
#include "unit.h"
#include "fds.h"
#include "checkpoint.h"
#include "util.h"

#include <errno.h>
#include <ftw.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

static char base[PATH_MAX];              /* the temporary test directory */

/* Path of `name` inside the test directory (static buffer, one at a time). */
static const char *in_base(const char *name)
{
    static char p[PATH_MAX];

    if (path_join(p, sizeof(p), base, name) != 0) {
        fprintf(stderr, "test path too long\n");
        exit(1);
    }
    return p;
}

/* Create a file with exactly n bytes of content. */
static int write_file(const char *path, const void *data, size_t n)
{
    FILE *f = fopen(path, "w");
    int ok;

    if (f == NULL)
        return -1;
    ok = fwrite(data, 1, n, f) == n;
    if (fclose(f) != 0)
        ok = 0;
    return ok ? 0 : -1;
}

static int write_str(const char *path, const char *s)
{
    return write_file(path, s, strlen(s));
}

/* Whole file into buf (NUL-terminated). Returns the length or -1. */
static long read_file(const char *path, char *buf, size_t cap)
{
    FILE *f = fopen(path, "r");
    size_t n;

    if (f == NULL)
        return -1;
    n = fread(buf, 1, cap - 1, f);
    fclose(f);
    buf[n] = '\0';
    return (long)n;
}

static int exists(const char *path)
{
    struct stat st;

    return lstat(path, &st) == 0;
}

static int same_ckpt(const ckpt_t *a, const ckpt_t *b)
{
    return memcmp(a->session_id, b->session_id, FDS_SESSION_ID_LEN) == 0 &&
           strcmp(a->topic, b->topic) == 0 && a->file_index == b->file_index &&
           strcmp(a->file_name, b->file_name) == 0 && a->k == b->k;
}

static void make_ckpt(ckpt_t *ck, const char *topic, int64_t idx, const char *name, int k)
{
    memset(ck, 0, sizeof(*ck));
    random_bytes(ck->session_id, FDS_SESSION_ID_LEN);
    snprintf(ck->topic, sizeof(ck->topic), "%s", topic);
    ck->file_index = idx;
    snprintf(ck->file_name, sizeof(ck->file_name), "%s", name);
    ck->k = k;
}

/* Save ck, load it back and compare; also: the tmp file never remains. */
static void round_trip(const ckpt_t *ck)
{
    ckpt_t back;

    memset(&back, 0x5a, sizeof(back));
    CHECK_EQ(ckpt_save(base, ck), 0);
    CHECK(exists(in_base(CKPT_FILE)));
    CHECK(!exists(in_base(CKPT_TMP_FILE)));
    CHECK_EQ(ckpt_load(base, &back), 0);
    CHECK(same_ckpt(ck, &back));
}

static void test_round_trip(void)
{
    ckpt_t ck, back;
    char text[1024], hex[2 * FDS_SESSION_ID_LEN + 1], want[1024];

    /* No record yet. */
    CHECK_EQ(ckpt_load(base, &back), 1);
    CHECK_EQ(ckpt_load(in_base("no-such-dir"), &back), 1);

    /* Name with spaces and '=' (split at the FIRST '='), no k. */
    make_ckpt(&ck, "gatto", 57, "OIP x7=Qm =a.jpeg", 0);
    round_trip(&ck);
    hex_encode(ck.session_id, FDS_SESSION_ID_LEN, hex);
    snprintf(want, sizeof(want),
             "session=%s\ntopic=gatto\nfile_index=57\nfile_name=OIP x7=Qm =a.jpeg\n", hex);
    CHECK(read_file(in_base(CKPT_FILE), text, sizeof(text)) > 0);
    CHECK_STR(text, want);                 /* exact format, and no "k=" line */

    /* No FILE_HDR yet (-1, empty name), Enhanced k present. */
    make_ckpt(&ck, "Cane", -1, "", 4);
    round_trip(&ck);
    CHECK(read_file(in_base(CKPT_FILE), text, sizeof(text)) > 0);
    CHECK(strstr(text, "\nk=4\n") != NULL);
    CHECK(strstr(text, "\nfile_index=-1\n") != NULL);

    /* UTF-8, a topic containing '=' and spaces, leading/trailing spaces in
     * the name (nothing is trimmed), the largest index and k. */
    make_ckpt(&ck, "my topic=x", (int64_t)UINT32_MAX, " gatto_\xc3\xa9\xe2\x82\xac.jpg ",
              FDS_ENH_MAX_K);
    round_trip(&ck);

    /* Longest topic and name. */
    {
        char longname[FDS_NAME_MAX + 1], longtopic[FDS_TOPIC_MAX + 1];
        memset(longname, 'n', FDS_NAME_MAX);
        longname[FDS_NAME_MAX] = '\0';
        memset(longtopic, 't', FDS_TOPIC_MAX);
        longtopic[FDS_TOPIC_MAX] = '\0';
        make_ckpt(&ck, longtopic, 0, longname, 1);
        round_trip(&ck);
    }

    /* A second save replaces the first atomically. */
    make_ckpt(&ck, "gatto", 3, "first.jpg", 0);
    CHECK_EQ(ckpt_save(base, &ck), 0);
    make_ckpt(&ck, "gatto", 4, "second.jpg", 0);
    round_trip(&ck);

    /* A stale tmp from a crash mid-save is ignored by load and does not
     * break the next save. */
    CHECK_EQ(write_str(in_base(CKPT_TMP_FILE), "session=garbage\n"), 0);
    CHECK_EQ(ckpt_load(base, &back), 0);
    CHECK(same_ckpt(&ck, &back));
    make_ckpt(&ck, "gatto", 5, "third.jpg", 0);
    round_trip(&ck);

    CHECK_EQ(ckpt_clear(base), 0);
    CHECK(!exists(in_base(CKPT_FILE)));
    CHECK_EQ(ckpt_load(base, &back), 1);
    CHECK_EQ(ckpt_clear(base), 0);         /* nothing left: still fine */
}

static void test_save_rejects(void)
{
    ckpt_t good, bad, back;

    make_ckpt(&good, "gatto", 1, "keep.jpg", 0);
    CHECK_EQ(ckpt_save(base, &good), 0);

    /* Records the loader would reject are never written; the old record
     * survives untouched and no tmp file remains. */
    make_ckpt(&bad, "", 1, "x.jpg", 0);
    CHECK_EQ(ckpt_save(base, &bad), -1);
    make_ckpt(&bad, "a/b", 1, "x.jpg", 0);
    CHECK_EQ(ckpt_save(base, &bad), -1);
    make_ckpt(&bad, "gatto", 1, "evil\nsession=00", 0);
    CHECK_EQ(ckpt_save(base, &bad), -1);
    make_ckpt(&bad, "gatto", 1, "../x.jpg", 0);
    CHECK_EQ(ckpt_save(base, &bad), -1);
    make_ckpt(&bad, "gatto", 1, ".hidden", 0);
    CHECK_EQ(ckpt_save(base, &bad), -1);
    make_ckpt(&bad, "gatto", -2, "x.jpg", 0);
    CHECK_EQ(ckpt_save(base, &bad), -1);
    make_ckpt(&bad, "gatto", 1, "x.jpg", FDS_ENH_MAX_K + 1);
    CHECK_EQ(ckpt_save(base, &bad), -1);
    make_ckpt(&bad, "gatto", 1, "x.jpg", -1);
    CHECK_EQ(ckpt_save(base, &bad), -1);
    CHECK_EQ(ckpt_load(base, &back), 0);
    CHECK(same_ckpt(&good, &back));
    CHECK(!exists(in_base(CKPT_TMP_FILE)));

    /* Directory missing: the save fails cleanly. */
    CHECK_EQ(ckpt_save(in_base("no-such-dir"), &good), -1);
    CHECK_EQ(ckpt_clear(base), 0);
}

/* Write raw text as the record and expect ckpt_load() to return `want`;
 * on failure *ck must be left untouched. */
static void expect_load(const char *text, size_t len, int want, int line)
{
    ckpt_t ck, sentinel;

    memset(&sentinel, 0x77, sizeof(sentinel));
    ck = sentinel;
    if (write_file(in_base(CKPT_FILE), text, len) != 0) {
        CHECK(0);
        return;
    }
    if (ckpt_load(base, &ck) != want) {
        fprintf(stderr, "  expect_load at line %d: wanted %d\n", line, want);
        CHECK(0);
        return;
    }
    CHECK(want == 0 || memcmp(&ck, &sentinel, sizeof(ck)) == 0);
}

#define LOAD_OK(s)   expect_load((s), strlen(s), 0, __LINE__)
#define LOAD_BAD(s)  expect_load((s), strlen(s), -1, __LINE__)
#define HEX32 "9f2c4e0b7d11a3c85e6f2a90b4d7c1e3"

static void test_corrupt(void)
{
    ckpt_t ck;

    /* Minimal valid record: the defaults fill the rest. */
    LOAD_OK("session=" HEX32 "\ntopic=gatto\n");
    CHECK_EQ(ckpt_load(base, &ck), 0);
    CHECK_EQ(ck.file_index, -1);
    CHECK_STR(ck.file_name, "");
    CHECK_EQ(ck.k, 0);
    CHECK_EQ(ck.session_id[0], 0x9f);
    CHECK_EQ(ck.session_id[15], 0xe3);

    /* Lenient on: unknown keys, blank lines, missing final newline,
     * upper-case hex, key order. */
    LOAD_OK("future=1=2\n\nsession=" HEX32 "\ntopic=gatto\nfile_index=3\nfile_name=a.jpg");
    LOAD_OK("topic=gatto\nfile_name=\nsession=9F2C4E0B7D11A3C85E6F2A90B4D7C1E3\nk=0\n");
    CHECK_EQ(ckpt_load(base, &ck), 0);
    CHECK_EQ(ck.session_id[0], 0x9f);

    /* Bad session hex. */
    LOAD_BAD("session=zz2c4e0b7d11a3c85e6f2a90b4d7c1e3\ntopic=gatto\n");
    LOAD_BAD("session=9f2c4e0b7d11a3c85e6f2a90b4d7c1\ntopic=gatto\n");       /* 31 digits */
    LOAD_BAD("session=" HEX32 "00\ntopic=gatto\n");                          /* 34 digits */
    LOAD_BAD("session= " HEX32 "\ntopic=gatto\n");                           /* blank */
    LOAD_BAD("session=\ntopic=gatto\n");
    /* Missing or invalid topic. */
    LOAD_BAD("session=" HEX32 "\nfile_index=3\nfile_name=a.jpg\n");
    LOAD_BAD("session=" HEX32 "\ntopic=\n");
    LOAD_BAD("session=" HEX32 "\ntopic=a/b\n");
    LOAD_BAD("session=" HEX32 "\ntopic=..\n");
    LOAD_BAD("session=" HEX32 "\ntopic=0123456789012345678901234567890123456789012345678901234567890123\n");
    LOAD_BAD("topic=gatto\n");                                               /* no session */
    /* Bad file_index. */
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nfile_index=-2\n");
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nfile_index=abc\n");
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nfile_index=5x\n");
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nfile_index=\n");
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nfile_index= 5\n");
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nfile_index=+5\n");
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nfile_index=4294967296\n");
    /* Unsafe file_name. */
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nfile_name=.hidden\n");
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nfile_name=a/b.jpg\n");
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nfile_name=a\r\n");
    /* Bad k. */
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nk=17\n");
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\nk=-1\n");
    /* Repeated key, line without '=', garbage, empty file. */
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\ntopic=cane\n");
    LOAD_BAD("session=" HEX32 "\ntopic=gatto\njunk\n");
    LOAD_BAD("hello world\n\x01\x02\x03");
    LOAD_BAD("");
    /* A NUL byte (zero-filled block after a crash). */
    {
        static const char nul_rec[] = "session=" HEX32 "\ntopic=ga\0to\n";
        static char zeros[512];

        expect_load(nul_rec, sizeof(nul_rec) - 1, -1, __LINE__);   /* without the final NUL */
        expect_load(zeros, sizeof(zeros), -1, __LINE__);
    }
    /* Oversized file (cap 4096 bytes). */
    {
        static char big[8192];
        int n = snprintf(big, sizeof(big), "session=" HEX32 "\ntopic=gatto\nfiller=");
        memset(big + n, 'x', sizeof(big) - (size_t)n - 2);
        big[sizeof(big) - 2] = '\n';
        big[sizeof(big) - 1] = '\0';
        LOAD_BAD(big);
    }
    /* A directory where the record should be. */
    CHECK_EQ(unlink(in_base(CKPT_FILE)), 0);
    CHECK_EQ(mkdir(in_base(CKPT_FILE), 0755), 0);
    CHECK_EQ(ckpt_load(base, &ck), -1);
    CHECK_EQ(rmdir(in_base(CKPT_FILE)), 0);
    CHECK_EQ(ckpt_load(base, &ck), 1);
}

static void test_sizes(void)
{
    uint64_t sz = 12345;
    char data[300];

    memset(data, 'x', sizeof(data));
    CHECK_EQ(part_size(base, "a.jpg", &sz), -1);
    CHECK_EQ(final_size(base, "a.jpg", &sz), -1);
    CHECK_EQ(write_file(in_base("a.jpg.part"), data, 123), 0);
    CHECK_EQ(part_size(base, "a.jpg", &sz), 0);
    CHECK_EQ(sz, 123);
    CHECK_EQ(final_size(base, "a.jpg", &sz), -1);
    CHECK_EQ(write_file(in_base("a.jpg"), data, 7), 0);
    CHECK_EQ(final_size(base, "a.jpg", &sz), 0);
    CHECK_EQ(sz, 7);
    CHECK_EQ(write_file(in_base("empty.jpg.part"), data, 0), 0);
    CHECK_EQ(part_size(base, "empty.jpg", &sz), 0);
    CHECK_EQ(sz, 0);
    /* not a regular file */
    CHECK_EQ(mkdir(in_base("d.jpg.part"), 0755), 0);
    CHECK_EQ(part_size(base, "d.jpg", &sz), -1);
    CHECK_EQ(mkdir(in_base("d.jpg"), 0755), 0);
    CHECK_EQ(final_size(base, "d.jpg", &sz), -1);
    /* names that would leave the directory are refused */
    CHECK_EQ(final_size(base, "../a.jpg", &sz), -1);
    CHECK_EQ(final_size(base, "", &sz), -1);
    CHECK_EQ(rmdir(in_base("d.jpg.part")), 0);
    CHECK_EQ(rmdir(in_base("d.jpg")), 0);
    CHECK_EQ(unlink(in_base("a.jpg.part")), 0);
    CHECK_EQ(unlink(in_base("a.jpg")), 0);
    CHECK_EQ(unlink(in_base("empty.jpg.part")), 0);
}

static void test_remove_partials(void)
{
    ckpt_t ck;

    CHECK_EQ(write_str(in_base("x.jpg.part"), "partial"), 0);
    CHECK_EQ(write_str(in_base("y z=1.png.part"), "partial"), 0);
    CHECK_EQ(write_str(in_base("done.jpg"), "complete"), 0);
    CHECK_EQ(write_str(in_base("notes.part.txt"), "keep"), 0);
    CHECK_EQ(write_str(in_base("keep.parts"), "keep"), 0);
    CHECK_EQ(write_str(in_base(CKPT_TMP_FILE), "tmp"), 0);
    CHECK_EQ(mkdir(in_base("sub.part"), 0755), 0);
    CHECK_EQ(symlink("done.jpg", in_base("link.part")), 0);
    make_ckpt(&ck, "gatto", 0, "x.jpg", 0);
    CHECK_EQ(ckpt_save(base, &ck), 0);

    CHECK_EQ(remove_partials(base), 0);
    CHECK(!exists(in_base("x.jpg.part")));
    CHECK(!exists(in_base("y z=1.png.part")));
    CHECK(!exists(in_base(CKPT_FILE)));
    CHECK(!exists(in_base(CKPT_TMP_FILE)));
    CHECK(exists(in_base("done.jpg")));
    CHECK(exists(in_base("notes.part.txt")));
    CHECK(exists(in_base("keep.parts")));
    CHECK(exists(in_base("sub.part")));        /* a directory is not a partial */
    CHECK(exists(in_base("link.part")));       /* nor is a symlink */
    CHECK(exists(in_base("done.jpg")));        /* link target untouched */

    /* Nothing left to remove; a missing directory is fine too. */
    CHECK_EQ(remove_partials(base), 0);
    CHECK_EQ(remove_partials(in_base("no-such-dir")), 0);

    CHECK_EQ(unlink(in_base("link.part")), 0);
    CHECK_EQ(rmdir(in_base("sub.part")), 0);
    CHECK_EQ(unlink(in_base("done.jpg")), 0);
    CHECK_EQ(unlink(in_base("notes.part.txt")), 0);
    CHECK_EQ(unlink(in_base("keep.parts")), 0);
}

static int rm_entry(const char *path, const struct stat *st, int flag, struct FTW *ftw)
{
    (void)st; (void)flag; (void)ftw;
    return remove(path);
}

int main(void)
{
    const char *tmp = env_str("TMPDIR");

    if (path_join(base, sizeof(base), tmp != NULL ? tmp : "/tmp", "fds_ckpt_XXXXXX") != 0 ||
        mkdtemp(base) == NULL) {
        perror("mkdtemp");
        return 1;
    }
    test_round_trip();
    test_save_rejects();
    test_corrupt();
    test_sizes();
    test_remove_partials();

    /* Every test cleans up after itself, so the directory must be empty. */
    CHECK_EQ(rmdir(base), 0);
    if (exists(base))
        nftw(base, rm_entry, 16, FTW_DEPTH | FTW_PHYS);
    UNIT_DONE();
}
