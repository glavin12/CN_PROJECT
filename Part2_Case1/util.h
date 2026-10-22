/*
 * util.h - argument parsing, path helpers, time, randomness and back-off.
 * No networking here; every other module may depend on this one.
 */
#ifndef FDS_UTIL_H
#define FDS_UTIL_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

/* ---- Command-line parsing: all return 0 on success, -1 on invalid input. ---- */

/* Decimal port 1..65535, no trailing junk. */
int parse_port(const char *s, uint16_t *out);

/* Real number in [0, 1] (strtod; "0", "0.01", "1e-3" accepted). */
int parse_prob(const char *s, double *out);

/* Decimal integer in [min, max]. */
int parse_long(const char *s, long min, long max, long *out);

/* ---- Name validation ------------------------------------------------------ */

/* Topic key: 1..FDS_TOPIC_MAX bytes, no '/', no control characters, not "." or "..". */
int topic_is_valid(const char *topic);

/*
 * File name safe to create inside the output directory: 1..FDS_NAME_MAX bytes,
 * no '/', no control characters (< 0x20 or 0x7f), does not start with '.'.
 * The server skips unsafe names when it loads a topic, and the client rejects
 * them (design section 7, "Path safety").
 */
int name_is_safe(const char *name);

/* ---- Paths ---------------------------------------------------------------- */

/* out = a + "/" + b. Returns -1 if it does not fit in cap bytes. */
int path_join(char *out, size_t cap, const char *a, const char *b);

/* out = dir + "/" + name + suffix (suffix may be "" or ".part"). */
int path_join3(char *out, size_t cap, const char *dir, const char *name, const char *suffix);

/* mkdir -p. Existing directories are fine. Returns 0 or -1 (errno set). */
int mkdir_p(const char *path, mode_t mode);

/* ---- Time ------------------------------------------------------------------ */

/* CLOCK_MONOTONIC in nanoseconds (used for every measured interval). */
int64_t now_ns(void);

/* CLOCK_REALTIME in nanoseconds (only for rough cross-machine alignment). */
int64_t wall_ns(void);

/* Sleep for ms milliseconds (restarts on EINTR). */
void sleep_ms(long ms);

/*
 * Retry back-off: 50 ms, 100, 200, 400, 800, then 1000 ms.
 * `streak` = number of consecutive failed attempts so far (1 for the first retry).
 */
long backoff_ms(int streak);

/* ---- Randomness and hex ---------------------------------------------------- */

/* Fill buf with n random bytes from getrandom(); falls back to a time-based mix. */
void random_bytes(uint8_t *buf, size_t n);

/* Lower-case hex of n bytes into out (needs 2n+1 bytes). */
void hex_encode(const uint8_t *in, size_t n, char *out);

/* Parse exactly 2n hex digits into out. Returns 0 or -1. */
int hex_decode(const char *hex, uint8_t *out, size_t n);

/* 1 if all n bytes are zero. */
int bytes_all_zero(const uint8_t *p, size_t n);

/* ---- Environment ----------------------------------------------------------- */

/* getenv() that treats unset and "" alike; returns NULL for both. */
const char *env_str(const char *name);

/* Integer environment variable, or def if unset/invalid. */
long env_long(const char *name, long def);

/* 1 when FDS_QUIET is set to a non-empty value other than "0". */
int fds_quiet(void);

#endif /* FDS_UTIL_H */
