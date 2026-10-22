/*
 * util.c - see util.h.
 */
#include "util.h"
#include "fds.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

int parse_long(const char *s, long min, long max, long *out)
{
    char *end;
    long v;

    if (s == NULL || *s == '\0')
        return -1;
    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || *end != '\0' || v < min || v > max)
        return -1;
    *out = v;
    return 0;
}

int parse_port(const char *s, uint16_t *out)
{
    long v;

    if (parse_long(s, 1, 65535, &v) != 0)
        return -1;
    *out = (uint16_t)v;
    return 0;
}

int parse_prob(const char *s, double *out)
{
    char *end;
    double v;

    if (s == NULL || *s == '\0')
        return -1;
    errno = 0;
    v = strtod(s, &end);
    /* !(v >= 0 && v <= 1) also rejects NaN */
    if (errno != 0 || *end != '\0' || !(v >= 0.0 && v <= 1.0))
        return -1;
    *out = v;
    return 0;
}

static int has_control_or_slash(const char *s)
{
    for (const unsigned char *p = (const unsigned char *)s; *p; p++)
        if (*p < 0x20 || *p == 0x7f || *p == '/')
            return 1;
    return 0;
}

int topic_is_valid(const char *topic)
{
    size_t n;

    if (topic == NULL)
        return 0;
    n = strlen(topic);
    if (n == 0 || n > FDS_TOPIC_MAX)
        return 0;
    if (strcmp(topic, ".") == 0 || strcmp(topic, "..") == 0)
        return 0;
    return !has_control_or_slash(topic);
}

int name_is_safe(const char *name)
{
    size_t n;

    if (name == NULL)
        return 0;
    n = strlen(name);
    if (n == 0 || n > FDS_NAME_MAX || name[0] == '.')
        return 0;
    return !has_control_or_slash(name);
}

int path_join(char *out, size_t cap, const char *a, const char *b)
{
    int n = snprintf(out, cap, "%s/%s", a, b);

    return (n < 0 || (size_t)n >= cap) ? -1 : 0;
}

int path_join3(char *out, size_t cap, const char *dir, const char *name, const char *suffix)
{
    int n = snprintf(out, cap, "%s/%s%s", dir, name, suffix);

    return (n < 0 || (size_t)n >= cap) ? -1 : 0;
}

int mkdir_p(const char *path, mode_t mode)
{
    char buf[PATH_MAX];
    size_t len = strlen(path);

    if (len == 0 || len >= sizeof(buf)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    memcpy(buf, path, len + 1);
    /* create every prefix ending before a '/', then the full path */
    for (char *p = buf + 1; *p; p++) {
        if (*p != '/')
            continue;
        *p = '\0';
        if (mkdir(buf, mode) != 0 && errno != EEXIST)
            return -1;
        *p = '/';
    }
    if (mkdir(buf, mode) != 0 && errno != EEXIST)
        return -1;
    {
        struct stat st;
        if (stat(buf, &st) != 0)
            return -1;
        if (!S_ISDIR(st.st_mode)) {
            errno = ENOTDIR;
            return -1;
        }
    }
    return 0;
}

static int64_t clock_ns(clockid_t id)
{
    struct timespec ts;

    clock_gettime(id, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

int64_t now_ns(void)  { return clock_ns(CLOCK_MONOTONIC); }
int64_t wall_ns(void) { return clock_ns(CLOCK_REALTIME); }

void sleep_ms(long ms)
{
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };

    while (nanosleep(&ts, &ts) != 0 && errno == EINTR)
        ;
}

long backoff_ms(int streak)
{
    long ms = FDS_BACKOFF_START_MS;

    for (int i = 1; i < streak && ms < FDS_BACKOFF_MAX_MS; i++)
        ms *= 2;
    return ms > FDS_BACKOFF_MAX_MS ? FDS_BACKOFF_MAX_MS : ms;
}

void random_bytes(uint8_t *buf, size_t n)
{
    size_t got = 0;

    while (got < n) {
        ssize_t r = getrandom(buf + got, n - got, 0);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        got += (size_t)r;
    }
    if (got < n) {
        /* fallback: not cryptographic, only needs to be unique per session */
        uint64_t x = (uint64_t)wall_ns() ^ ((uint64_t)getpid() << 32);
        for (size_t i = got; i < n; i++) {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            buf[i] = (uint8_t)x;
        }
    }
}

void hex_encode(const uint8_t *in, size_t n, char *out)
{
    static const char digits[] = "0123456789abcdef";

    for (size_t i = 0; i < n; i++) {
        out[2 * i]     = digits[in[i] >> 4];
        out[2 * i + 1] = digits[in[i] & 0xf];
    }
    out[2 * n] = '\0';
}

static int hexval(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

int hex_decode(const char *hex, uint8_t *out, size_t n)
{
    if (strlen(hex) != 2 * n)
        return -1;
    for (size_t i = 0; i < n; i++) {
        int hi = hexval(hex[2 * i]), lo = hexval(hex[2 * i + 1]);
        if (hi < 0 || lo < 0)
            return -1;
        out[i] = (uint8_t)(hi << 4 | lo);
    }
    return 0;
}

int bytes_all_zero(const uint8_t *p, size_t n)
{
    for (size_t i = 0; i < n; i++)
        if (p[i] != 0)
            return 0;
    return 1;
}

const char *env_str(const char *name)
{
    const char *v = getenv(name);

    return (v != NULL && *v != '\0') ? v : NULL;
}

long env_long(const char *name, long def)
{
    long v;
    const char *s = env_str(name);

    if (s == NULL || parse_long(s, LONG_MIN, LONG_MAX, &v) != 0)
        return def;
    return v;
}

int fds_quiet(void)
{
    const char *v = env_str("FDS_QUIET");

    return v != NULL && strcmp(v, "0") != 0;
}
