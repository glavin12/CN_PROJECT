/*
 * checkpoint.c - client-side checkpoint (design section 5.4, deviation 13).
 * The contract and the record format are in checkpoint.h.
 *
 * Why a text key=value file: it can be read with `cat` while debugging or in
 * the viva, and unknown keys can simply be skipped, so a later version can add
 * fields without breaking older records.
 *
 * Why the loader is strict: the client is authoritative for its checkpoint,
 * and the server can only cross-check it. A record we did not write ourselves
 * (truncated by a power loss, edited by hand, left by another program) must
 * never be trusted, so any doubt makes ckpt_load() return -1. The caller then
 * clears it and starts fresh, which costs time but can never produce a wrong
 * file.
 *
 * Not thread-safe for one directory: the tmp file name is fixed. That is fine
 * because only one thread of the client writes the record (Case 2: the only
 * thread; Enhanced: the manifest step, before the workers start).
 */
#include "checkpoint.h"
#include "util.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/*
 * Largest record ckpt_load() accepts. A real one is under 450 bytes (32 hex
 * digits, a 63-byte topic, a 255-byte name); the cap keeps a garbage file
 * from being read in full.
 */
#define CKPT_MAX_BYTES 4096

/* Bits of the keys seen by the loader (to require some and refuse repeats). */
enum {
    SEEN_SESSION    = 1 << 0,
    SEEN_TOPIC      = 1 << 1,
    SEEN_FILE_INDEX = 1 << 2,
    SEEN_FILE_NAME  = 1 << 3,
    SEEN_K          = 1 << 4
};

/* 1 if the n-byte buffer s holds a NUL-terminated string. */
static int terminated(const char *s, size_t n)
{
    return memchr(s, '\0', n) != NULL;
}

/*
 * The record would be accepted by ckpt_load(). ckpt_save() refuses anything
 * else: a name with a newline, for instance, would otherwise inject a line
 * into the record. Writing only what we can read back means a save can never
 * leave behind a record that the next run rejects.
 */
static int ckpt_valid(const ckpt_t *ck)
{
    if (!terminated(ck->topic, sizeof(ck->topic)) || !topic_is_valid(ck->topic))
        return 0;
    if (!terminated(ck->file_name, sizeof(ck->file_name)))
        return 0;
    if (ck->file_name[0] != '\0' && !name_is_safe(ck->file_name))
        return 0;
    /* file_index travels as a u32 on the wire; -1 = no FILE_HDR yet. */
    if (ck->file_index < -1 || ck->file_index > (int64_t)UINT32_MAX)
        return 0;
    return ck->k >= 0 && ck->k <= FDS_ENH_MAX_K;
}

int ckpt_save(const char *dir, const ckpt_t *ck)
{
    char  tmp[PATH_MAX], fin[PATH_MAX];
    char  hex[2 * FDS_SESSION_ID_LEN + 1];
    FILE *f;
    int   ok, saved_errno;

    if (dir == NULL || ck == NULL || !ckpt_valid(ck)) {
        errno = EINVAL;
        return -1;
    }
    if (path_join(tmp, sizeof(tmp), dir, CKPT_TMP_FILE) != 0 ||
        path_join(fin, sizeof(fin), dir, CKPT_FILE) != 0) {
        errno = ENAMETOOLONG;
        return -1;
    }
    hex_encode(ck->session_id, FDS_SESSION_ID_LEN, hex);

    /*
     * Write the whole record to a tmp file, then rename() it over .session.
     * rename() replaces the target atomically: a crash at any moment leaves
     * either the complete old record or the complete new one, never a torn
     * mix that could point at the wrong file.
     */
    f = fopen(tmp, "w");
    if (f == NULL)
        return -1;
    ok = fprintf(f, "session=%s\n", hex) >= 0 &&
         fprintf(f, "topic=%s\n", ck->topic) >= 0 &&
         fprintf(f, "file_index=%" PRId64 "\n", ck->file_index) >= 0 &&
         fprintf(f, "file_name=%s\n", ck->file_name) >= 0 &&
         (ck->k <= 0 || fprintf(f, "k=%d\n", ck->k) >= 0);
    /* fprintf only fills the stdio buffer; a write error such as a full disk
     * shows up when fclose() flushes it, so its result matters too. */
    if (fclose(f) != 0)
        ok = 0;
    /*
     * No fsync() before the rename, for speed (Case 2 saves on every
     * FILE_HDR). The fault model is a lost connection or a killed client
     * process, and the kernel keeps both files intact in the page cache in
     * that case. Only an OS crash or power loss could lose recent writes, and
     * the data files are not fsynced either (transfer.h), so syncing the
     * record alone would buy nothing: the offset is always re-read with
     * stat(<name>.part), and a damaged record is rejected by ckpt_load().
     */
    if (!ok || rename(tmp, fin) != 0) {
        saved_errno = errno;
        unlink(tmp);             /* best effort: never leave the tmp behind */
        errno = saved_errno;
        return -1;
    }
    return 0;
}

/*
 * Integer field of the record: an optional '-' and decimal digits only.
 * parse_long() alone would also accept leading blanks and '+', which our
 * writer never produces.
 */
static int parse_int_field(const char *val, long min, long max, long *out)
{
    if (!(val[0] == '-' || isdigit((unsigned char)val[0])))
        return -1;
    return parse_long(val, min, max, out);
}

/*
 * Apply one "key=value" line to *ck. The line is split at its FIRST '=',
 * because values may contain '=' (file names may). Nothing is trimmed: a
 * name may legally start or end with a space. Returns 0 or -1 (violation).
 */
static int parse_line(char *line, ckpt_t *ck, unsigned *seen)
{
    char    *eq = strchr(line, '=');
    char    *val;
    unsigned bit;
    long     v;

    if (eq == NULL)
        return -1;
    *eq = '\0';
    val = eq + 1;

    if (strcmp(line, "session") == 0) {
        bit = SEEN_SESSION;
        /* exactly 32 hex digits, nothing else */
        if (hex_decode(val, ck->session_id, FDS_SESSION_ID_LEN) != 0)
            return -1;
    } else if (strcmp(line, "topic") == 0) {
        bit = SEEN_TOPIC;
        /* topic_is_valid() also bounds the length to FDS_TOPIC_MAX. */
        if (!topic_is_valid(val))
            return -1;
        memcpy(ck->topic, val, strlen(val) + 1);
    } else if (strcmp(line, "file_index") == 0) {
        bit = SEEN_FILE_INDEX;
        if (parse_int_field(val, -1, (long)UINT32_MAX, &v) != 0)
            return -1;
        ck->file_index = v;
    } else if (strcmp(line, "file_name") == 0) {
        bit = SEEN_FILE_NAME;
        /* name_is_safe() also bounds the length to FDS_NAME_MAX and rejects
         * '/', so a record can never point outside the topic directory. */
        if (val[0] != '\0' && !name_is_safe(val))
            return -1;
        memcpy(ck->file_name, val, strlen(val) + 1);
    } else if (strcmp(line, "k") == 0) {
        bit = SEEN_K;
        if (parse_int_field(val, 0, FDS_ENH_MAX_K, &v) != 0)
            return -1;
        ck->k = (int)v;
    } else {
        return 0;                /* unknown key: skipped (forward compatible) */
    }
    /* A repeated key makes the record ambiguous: not one we wrote. */
    if (*seen & bit)
        return -1;
    *seen |= bit;
    return 0;
}

int ckpt_load(const char *dir, ckpt_t *ck)
{
    char     path[PATH_MAX];
    char     buf[CKPT_MAX_BYTES + 1];
    ckpt_t   tmp;
    unsigned seen = 0;
    FILE    *f;
    size_t   n;
    int      read_err;

    if (dir == NULL || ck == NULL || path_join(path, sizeof(path), dir, CKPT_FILE) != 0)
        return -1;
    f = fopen(path, "r");
    if (f == NULL)
        return errno == ENOENT ? 1 : -1;
    /* Read one byte more than the cap, so that an oversized file shows. */
    n = fread(buf, 1, sizeof(buf), f);
    read_err = ferror(f);
    if (fclose(f) != 0)
        read_err = 1;
    /*
     * A NUL byte cannot appear in a record we wrote; it does appear when a
     * crash leaves a zero-filled block, and the string functions below would
     * silently stop at it.
     */
    if (read_err || n > CKPT_MAX_BYTES || memchr(buf, '\0', n) != NULL)
        return -1;
    buf[n] = '\0';

    /* Parse into a local copy so that *ck is only changed on success. */
    memset(&tmp, 0, sizeof(tmp));
    tmp.file_index = -1;          /* default: no FILE_HDR received yet */
    for (char *line = buf; *line != '\0';) {
        char *nl = strchr(line, '\n');
        char *next;

        if (nl != NULL) {
            *nl = '\0';
            next = nl + 1;
        } else {
            next = line + strlen(line);   /* last line without '\n' */
        }
        /* Blank lines carry nothing; anything else must parse. */
        if (*line != '\0' && parse_line(line, &tmp, &seen) != 0)
            return -1;
        line = next;
    }
    if (!(seen & SEEN_SESSION) || !(seen & SEEN_TOPIC))
        return -1;
    *ck = tmp;
    return 0;
}

int ckpt_clear(const char *dir)
{
    static const char *const names[] = { CKPT_FILE, CKPT_TMP_FILE };
    char path[PATH_MAX];
    int  rc = 0;

    if (dir == NULL)
        return -1;
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        if (path_join(path, sizeof(path), dir, names[i]) != 0)
            rc = -1;
        else if (unlink(path) != 0 && errno != ENOENT)   /* already gone: fine */
            rc = -1;
    }
    return rc;
}

/*
 * Size of the regular file dir/<name><suffix>. The name is checked with
 * name_is_safe() so that no caller can make us stat a path outside dir
 * (defence in depth: names from a MANIFEST are validated already).
 * S_ISREG: a directory or device with that name is not our data.
 */
static int regular_size(const char *dir, const char *name, const char *suffix,
                        uint64_t *size)
{
    char        path[PATH_MAX];
    struct stat st;

    if (dir == NULL || !name_is_safe(name) ||
        path_join3(path, sizeof(path), dir, name, suffix) != 0)
        return -1;
    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode))
        return -1;
    *size = (uint64_t)st.st_size;
    return 0;
}

int part_size(const char *dir, const char *name, uint64_t *size)
{
    return regular_size(dir, name, PART_SUFFIX, size);
}

int final_size(const char *dir, const char *name, uint64_t *size)
{
    return regular_size(dir, name, "", size);
}

int remove_partials(const char *dir)
{
    const size_t   sfx = strlen(PART_SUFFIX);
    struct dirent *de;
    DIR           *d;
    int            dfd, rc = 0;

    if (dir == NULL)
        return -1;
    d = opendir(dir);
    if (d == NULL) {
        /* No directory: no partials and no checkpoint to remove. */
        return errno == ENOENT ? 0 : -1;
    }
    dfd = dirfd(d);
    /*
     * Unlinking entries while readdir() walks the directory is allowed:
     * POSIX only leaves open whether the removed entry itself is returned
     * again, and fstatat() below would then just report ENOENT.
     */
    for (;;) {
        struct stat st;
        size_t      len;

        errno = 0;
        de = readdir(d);
        if (de == NULL) {
            if (errno != 0)
                rc = -1;
            break;
        }
        len = strlen(de->d_name);
        /* "<name>.part" with a non-empty name, as recv_file() creates it */
        if (len <= sfx || strcmp(de->d_name + len - sfx, PART_SUFFIX) != 0)
            continue;
        /* d_type may be DT_UNKNOWN on some file systems, so ask fstatat().
         * AT_SYMLINK_NOFOLLOW: a symlink is not a partial we wrote. */
        if (fstatat(dfd, de->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
            if (errno != ENOENT)
                rc = -1;
            continue;
        }
        if (!S_ISREG(st.st_mode))
            continue;
        if (unlinkat(dfd, de->d_name, 0) != 0 && errno != ENOENT)
            rc = -1;
    }
    if (closedir(d) != 0)
        rc = -1;
    /*
     * Completed files stay: they will be overwritten by the fresh transfer
     * (deleting them would lose nothing but would cost time if this call
     * fails half-way). The checkpoint goes, because it describes the run
     * that was just abandoned.
     */
    if (ckpt_clear(dir) != 0)
        rc = -1;
    return rc;
}
