/*
 * log.c - see log.h.
 */
#include "log.h"
#include "fds.h"
#include "util.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static log_ctx_t g_ctx = { "fds", "-", "-", -1.0, 0, 0 };
static const char *g_run_id = "-";
static char g_client_id[64] = "-";
static int g_main_fd = -1;     /* FDS_LOG */
static int g_events_fd = -1;   /* FDS_EVENTS (server only) */

static const char SERVER_HDR[] =
    "run_id,case,mode,p,seed,conn_id,kind,session_id,topic,k,peer,"
    "start_ns,end_ns,start_wall_ns,data_bytes,wire_bytes,chunks,files_completed,faulted,status\n";
static const char EVENTS_HDR[] =
    "run_id,case,ts_ns,event,conn_id,session_id,topic,file_index,offset,chunk_index,conn_data_bytes,detail\n";
static const char CLIENT_HDR[] =
    "run_id,case,host,client_id,pid,topic,k,io,start_ns,end_ns,start_wall_ns,end_wall_ns,"
    "useful_bytes,data_bytes_rx,wire_bytes_rx,files,attempts,reconnects,rejects,status\n";

static int open_csv(const char *path)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);

    if (fd < 0)
        fprintf(stderr, "[%s] WARNING: cannot open log file %s: %m\n", g_ctx.role, path);
    return fd;
}

/* Append one row; the header goes first if the file is still empty. */
static void append_row(int fd, const char *header, const char *row, size_t len)
{
    struct stat st;

    if (fd < 0)
        return;
    flock(fd, LOCK_EX);
    if (fstat(fd, &st) == 0 && st.st_size == 0)
        (void)!write(fd, header, strlen(header));
    (void)!write(fd, row, len);
    flock(fd, LOCK_UN);
}

void log_init(const log_ctx_t *ctx)
{
    const char *v;

    pthread_mutex_lock(&g_mu);
    g_ctx = *ctx;
    if ((v = env_str("FDS_RUN_ID")) != NULL)
        g_run_id = v;
    if ((v = env_str("FDS_CLIENT_ID")) != NULL)
        snprintf(g_client_id, sizeof(g_client_id), "%s", v);
    else
        snprintf(g_client_id, sizeof(g_client_id), "%ld", (long)getpid());
    if ((v = env_str("FDS_LOG")) != NULL)
        g_main_fd = open_csv(v);
    if (strcmp(ctx->role, "server") == 0 && (v = env_str("FDS_EVENTS")) != NULL)
        g_events_fd = open_csv(v);
    pthread_mutex_unlock(&g_mu);
}

void log_close(void)
{
    pthread_mutex_lock(&g_mu);
    if (g_main_fd >= 0)
        close(g_main_fd);
    if (g_events_fd >= 0)
        close(g_events_fd);
    g_main_fd = g_events_fd = -1;
    pthread_mutex_unlock(&g_mu);
}

static void vlog(const char *tag, const char *fmt, va_list ap)
{
    char line[1024];
    int n = snprintf(line, sizeof(line), "[%s] %s", g_ctx.role, tag);

    if (n < 0)
        return;
    vsnprintf(line + n, sizeof(line) - (size_t)n, fmt, ap);
    pthread_mutex_lock(&g_mu);
    fprintf(stderr, "%s\n", line);
    pthread_mutex_unlock(&g_mu);
}

void log_info(const char *fmt, ...)
{
    va_list ap;

    if (fds_quiet())
        return;
    va_start(ap, fmt);
    vlog("", fmt, ap);
    va_end(ap);
}

void log_warn(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    vlog("WARNING: ", fmt, ap);
    va_end(ap);
}

static void sid_hex(const uint8_t *sid, char out[2 * FDS_SESSION_ID_LEN + 1])
{
    if (sid == NULL)
        strcpy(out, "-");
    else
        hex_encode(sid, FDS_SESSION_ID_LEN, out);
}

/* CSV fields we print never contain commas except topic/file names: quote those. */
static void csv_str(char *out, size_t cap, const char *s)
{
    size_t j = 0;

    if (s == NULL || *s == '\0') {
        snprintf(out, cap, "-");
        return;
    }
    if (cap < 3) {
        out[0] = '\0';
        return;
    }
    out[j++] = '"';
    for (; *s && j + 3 < cap; s++) {
        if (*s == '"')
            out[j++] = '"';
        out[j++] = *s;
    }
    out[j++] = '"';
    out[j] = '\0';
}

static void seed_str(char *out, size_t cap)
{
    if (g_ctx.have_seed)
        snprintf(out, cap, "%ld", g_ctx.seed);
    else
        snprintf(out, cap, "-");
}

void log_conn(const log_conn_row_t *r)
{
    char row[1024], sid[2 * FDS_SESSION_ID_LEN + 1], topic[160], seed[32];
    int n;

    if (g_main_fd < 0)
        return;
    sid_hex(r->session_id, sid);
    csv_str(topic, sizeof(topic), r->topic);
    seed_str(seed, sizeof(seed));
    n = snprintf(row, sizeof(row),
                 "%s,%s,%s,%g,%s,%llu,%s,%s,%s,%d,%s,%lld,%lld,%lld,%llu,%llu,%llu,%u,%d,%s\n",
                 g_run_id, g_ctx.case_name, g_ctx.mode, g_ctx.p, seed,
                 (unsigned long long)r->conn_id, r->kind ? r->kind : "-", sid, topic, r->k,
                 r->peer ? r->peer : "-", (long long)r->start_ns, (long long)r->end_ns,
                 (long long)r->start_wall_ns, (unsigned long long)r->data_bytes,
                 (unsigned long long)r->wire_bytes, (unsigned long long)r->chunks,
                 r->files_completed, r->faulted, r->status ? r->status : "-");
    if (n > 0 && (size_t)n < sizeof(row)) {
        pthread_mutex_lock(&g_mu);
        append_row(g_main_fd, SERVER_HDR, row, (size_t)n);
        pthread_mutex_unlock(&g_mu);
    }
}

void log_event(const char *event, uint64_t conn_id, const uint8_t *session_id,
               const char *topic, int64_t file_index, uint64_t offset,
               uint64_t chunk_index, uint64_t conn_data_bytes, const char *detail)
{
    char row[1024], sid[2 * FDS_SESSION_ID_LEN + 1], t[160], d[320];
    int n;

    if (g_events_fd < 0)
        return;
    sid_hex(session_id, sid);
    csv_str(t, sizeof(t), topic);
    csv_str(d, sizeof(d), detail);
    n = snprintf(row, sizeof(row), "%s,%s,%lld,%s,%llu,%s,%s,%lld,%llu,%llu,%llu,%s\n",
                 g_run_id, g_ctx.case_name, (long long)now_ns(), event,
                 (unsigned long long)conn_id, sid, t, (long long)file_index,
                 (unsigned long long)offset, (unsigned long long)chunk_index,
                 (unsigned long long)conn_data_bytes, d);
    if (n > 0 && (size_t)n < sizeof(row)) {
        pthread_mutex_lock(&g_mu);
        append_row(g_events_fd, EVENTS_HDR, row, (size_t)n);
        pthread_mutex_unlock(&g_mu);
    }
}

void log_client(const log_client_row_t *r)
{
    char row[1024], host[256], topic[160];
    int n;

    if (g_main_fd < 0)
        return;
    if (gethostname(host, sizeof(host)) != 0)
        strcpy(host, "-");
    host[sizeof(host) - 1] = '\0';
    csv_str(topic, sizeof(topic), r->topic);
    n = snprintf(row, sizeof(row),
                 "%s,%s,%s,%s,%ld,%s,%d,%s,%lld,%lld,%lld,%lld,%llu,%llu,%llu,%u,%u,%u,%u,%s\n",
                 g_run_id, g_ctx.case_name, host, g_client_id, (long)getpid(), topic, r->k,
                 r->io ? r->io : "-", (long long)r->start_ns, (long long)r->end_ns,
                 (long long)r->start_wall_ns, (long long)r->end_wall_ns,
                 (unsigned long long)r->useful_bytes, (unsigned long long)r->data_bytes_rx,
                 (unsigned long long)r->wire_bytes_rx, r->files, r->attempts, r->reconnects,
                 r->rejects, r->status ? r->status : "-");
    if (n > 0 && (size_t)n < sizeof(row)) {
        pthread_mutex_lock(&g_mu);
        append_row(g_main_fd, CLIENT_HDR, row, (size_t)n);
        pthread_mutex_unlock(&g_mu);
    }
}
