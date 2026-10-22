/*
 * transfer.c - streaming the files of a topic (design sections 3.5-3.8, 4,
 * 6.3; deviations 6 and 9). The contract is in transfer.h; this comment
 * explains how it is met.
 *
 * Server side: send_file_range() / serve_topic()
 *   - FILE_HDR goes out with MSG_MORE, so the kernel coalesces it with the
 *     first data bytes instead of sending a tiny packet of its own (6.3).
 *   - The data goes out with sendfile(): the kernel copies straight from the
 *     page cache to the socket, never through user space, and the explicit
 *     offset is exactly what resuming needs. If sendfile() is not supported
 *     for a file (EINVAL/ENOSYS on its first call) we fall back to pread() +
 *     send() for that file. sendfile() takes no flags, so unlike send() it
 *     cannot be given MSG_NOSIGNAL: on a reset peer it raises SIGPIPE. Every
 *     process that calls send_file_range() must therefore ignore SIGPIPE,
 *     which server_run() does before any connection exists.
 *   - Data is sent in 64 KB chunks. Before each chunk the fault hook (Part II)
 *     draws once (design 4.1). On a fault a random prefix of the chunk still
 *     goes out, then the connection is cut (deviation 9).
 *   - Byte accounting is exact on every path: the counters and the progress
 *     hook get the number of bytes the kernel actually ACCEPTED, even when a
 *     send fails half-way. That matters for Case 2 / Enhanced, where the
 *     session's "sent so far" must never be smaller than what the client can
 *     have stored, or a correct RESUME would be rejected.
 *   - The file size comes from the topic map, not from fstat(): a topic is an
 *     immutable snapshot (deviation 3), and the size already announced in
 *     TOPIC_BEGIN / MANIFEST must match what FILE_HDR says.
 *
 * Client side: recv_file() / recv_topic() / recv_expect()
 *   - Raw file bytes are received into a 64 KB buffer and each piece is
 *     written to the file BEFORE the next recv(). In RX_PART mode the size of
 *     <name>.part is therefore always exactly the number of bytes received,
 *     and Case 2 / Enhanced use it as the resume offset. This is also why the
 *     data loop calls recv() directly instead of recvall(): recvall() would
 *     wait to fill the whole buffer before anything could be written.
 *   - A file is renamed from .part to its real name only after its FILE_END,
 *     so a file with its final name is always complete.
 */
#include "transfer.h"
#include "log.h"
#include "net.h"
#include "protocol.h"
#include "util.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* Largest encoded FILE_HDR: header + index, size, offset, name length, name. */
#define FILE_HDR_MAX (FDS_HDR_LEN + 4 + 8 + 8 + 2 + FDS_NAME_MAX)
/* Encoded FILE_END, TOPIC_BEGIN and TOPIC_END are small and fixed. */
#define SMALL_MSG_MAX 64

/* ===========================================================================
 * Server side
 * ======================================================================== */

/* Outcome of moving file bytes to the socket. */
enum {
    SEND_OK = 0,
    SEND_PEER,          /* socket error: peer gone, reset, or SO_SNDTIMEO expired */
    SEND_SHORT,         /* the file ended early: it shrank after the topic was loaded */
    SEND_FILE_ERR       /* the file could not be read (EIO, ...) */
};

/* The file being sent and how to read it. */
typedef struct {
    int      in;                /* O_RDONLY descriptor of the file */
    int      use_sendfile;      /* 0 once sendfile() proved unsupported for it */
    int      sendfile_worked;   /* sendfile() has already moved bytes of it */
    uint8_t *buf;               /* fallback buffer, allocated on first use */
} src_t;

/* sendfile() mixes errors of both descriptors: tell the file's apart. */
static int file_side_errno(int err)
{
    return err == EIO || err == ENOMEM || err == EOVERFLOW || err == EINVAL ||
           err == ENOSYS || err == ESPIPE;
}

/*
 * Fallback path: pread() up to one buffer of the file at pos and send it.
 * *moved = bytes the socket accepted, on every path. send() is used directly
 * (with the same EINTR loop and MSG_NOSIGNAL as sendall()) because a failed
 * sendall() does not say how much of the buffer was already accepted.
 */
static int copy_piece(int sock, src_t *s, uint64_t pos, size_t left, size_t *moved)
{
    size_t want = FDS_MIN(left, (size_t)FDS_IO_BUF), done = 0;
    ssize_t r;

    *moved = 0;
    if (s->buf == NULL && (s->buf = malloc(FDS_IO_BUF)) == NULL)
        return SEND_FILE_ERR;           /* errno = ENOMEM */
    do
        r = pread(s->in, s->buf, want, (off_t)pos);
    while (r < 0 && errno == EINTR);
    if (r < 0)
        return SEND_FILE_ERR;
    if (r == 0)
        return SEND_SHORT;
    while (done < (size_t)r) {
        ssize_t w = send(sock, s->buf + done, (size_t)r - done, MSG_NOSIGNAL);

        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0) {
            if (w == 0)
                errno = EPIPE;          /* never happens for len > 0; do not spin */
            *moved = done;
            return SEND_PEER;
        }
        done += (size_t)w;
    }
    *moved = done;
    return SEND_OK;
}

/*
 * Hand exactly len bytes of the file, starting at *pos, to the socket.
 * *pos and *sent advance by what the kernel accepted, also when it fails, so
 * the caller can account for a partial chunk exactly. errno is set on failure.
 */
static int send_data(conn_t *c, src_t *s, uint64_t *pos, size_t len, size_t *sent)
{
    *sent = 0;
    while (*sent < len) {
        size_t left = len - *sent;

        if (s->use_sendfile) {
            off_t o = (off_t)*pos;
            ssize_t r = sendfile(c->fd, s->in, &o, left);

            if (r < 0) {
                if (errno == EINTR)
                    continue;
                /* Some file systems cannot feed sendfile(); only trust that
                 * reading when nothing of this file went through it yet. */
                if ((errno == EINVAL || errno == ENOSYS) && !s->sendfile_worked) {
                    s->use_sendfile = 0;
                    continue;
                }
                /* EAGAIN here is SO_SNDTIMEO expiring on a blocking socket:
                 * the peer stopped reading for 30 s, so it counts as lost. */
                return file_side_errno(errno) ? SEND_FILE_ERR : SEND_PEER;
            }
            if (r == 0)
                return SEND_SHORT;      /* end of file before the cached size */
            s->sendfile_worked = 1;
            *pos += (uint64_t)r;        /* partial transfers are normal: loop */
            *sent += (size_t)r;
        } else {
            size_t moved;
            int rc = copy_piece(c->fd, s, *pos, left, &moved);

            *pos += moved;
            *sent += moved;
            if (rc != SEND_OK)
                return rc;
        }
    }
    return SEND_OK;
}

/* Count n data bytes handed to the socket for file idx, now at file offset
 * pos, and tell the progress hook. */
static void account(conn_t *c, uint32_t idx, uint64_t pos, size_t n, int force_hook)
{
    c->data_bytes += n;
    c->wire_bytes += n;                 /* raw data is part of the wire bytes too */
    if (n > 0)
        c->chunks++;                    /* "complete or partial data chunks sent" */
    if (c->progress_hook != NULL && (n > 0 || force_hook))
        c->progress_hook(c, idx, pos, n);
}

/* The canonical topic key for log records, even if serve_client() did not
 * fill c->topic. */
static const char *topic_key(const conn_t *c, const topic_t *t)
{
    return c->topic[0] != '\0' ? c->topic : t->key;
}

/* A send of file data failed: set the status and choose the return code. */
static xfer_rc_t data_failed(conn_t *c, uint32_t idx, uint64_t pos, int why, int err)
{
    if (why == SEND_PEER) {
        log_info("conn %llu: peer lost in file #%u at offset %llu: %s",
                 (unsigned long long)c->conn_id, idx, (unsigned long long)pos, strerror(err));
        c->status = "PEER_LOST";
        return XFER_PEER;
    }
    /* The client is in the middle of the raw bytes announced by FILE_HDR: an
     * ERROR message now would be read as file data. Cutting the connection is
     * the only honest signal left. A Part II client then retries; a Part I
     * client fails, which is right because the file really is broken. */
    log_warn("conn %llu: file #%u %s at offset %llu; connection closed",
             (unsigned long long)c->conn_id, idx,
             why == SEND_SHORT ? "is shorter than when the topic was loaded" : strerror(err),
             (unsigned long long)pos);
    conn_abort(c);
    c->status = "ERR_500";
    return XFER_IOERR;
}

/* The fault hook said "fail" and the prefix has been sent: log it and cut. */
static void inject_fault(conn_t *c, const topic_t *t, uint32_t idx, uint64_t chunk_off,
                         uint64_t chunk_idx, size_t prefix, size_t n)
{
    char detail[64];

    snprintf(detail, sizeof(detail), "prefix=%zu/%zu", prefix, n);
    /* offset = where the faulted chunk began; chunk_index = 0-based index of
     * that chunk among the chunks of this connection; conn_data_bytes
     * includes the prefix (design 4.3, deviation 9). */
    log_event("FAULT", c->conn_id, c->have_session ? c->session_id : NULL, topic_key(c, t),
              (int64_t)idx, chunk_off, chunk_idx, c->data_bytes, detail);
    log_info("conn %llu: FAULT in file #%u at offset %llu (%zu of %zu chunk bytes sent)",
             (unsigned long long)c->conn_id, idx, (unsigned long long)chunk_off, prefix, n);
    c->faulted = 1;
    c->status = "FAULT";
    conn_abort(c);                      /* shutdown(SHUT_RDWR) + close() */
}

/* Send [off, size) of file idx in chunks, with a fault draw before each. */
static xfer_rc_t send_chunks(conn_t *c, src_t *s, const topic_t *t, uint32_t idx,
                             uint64_t off, uint64_t size)
{
    uint64_t pos = off;

    while (pos < size) {
        size_t n = (size_t)FDS_MIN((uint64_t)FDS_CHUNK_SIZE, size - pos);
        uint64_t chunk_off = pos, chunk_idx = c->chunks;
        size_t prefix = 0, sent;
        int faulted = 0, rc, err;

        /* One draw per chunk, before any byte of it is sent (design 4.1). */
        if (c->fault_hook != NULL && c->fault_hook(c->fault_state, n, &prefix) == 1) {
            faulted = 1;
            /* The contract is prefix < chunk length: at least one byte of the
             * chunk is always lost, so a fault always forces a recovery. */
            if (prefix >= n)
                prefix = n - 1;
        }
        rc = send_data(c, s, &pos, faulted ? prefix : n, &sent);
        err = errno;
        /* Even a failed send may have moved bytes the client can store. The
         * hook always fires after a fault prefix, even an empty one. */
        account(c, idx, pos, sent, faulted && rc == SEND_OK);
        if (rc != SEND_OK)
            return data_failed(c, idx, pos, rc, err);
        if (faulted) {
            inject_fault(c, t, idx, chunk_off, chunk_idx, prefix, n);
            return XFER_FAULT;
        }
    }
    return XFER_OK;
}

xfer_rc_t send_file_range(conn_t *c, const topic_t *t, uint32_t idx, uint64_t off)
{
    char path[PATH_MAX], msg[300];
    uint8_t buf[FILE_HDR_MAX];
    src_t src = { .in = -1, .use_sendfile = 1, .sendfile_worked = 0, .buf = NULL };
    const file_entry_t *fe;
    m_file_hdr_t h;
    size_t n, name_len;
    xfer_rc_t rc;

    if (idx >= t->n_files || off > t->files[idx].size) {
        /* The caller promised a validated range; refusing is safer than
         * announcing a negative byte count. */
        log_warn("conn %llu: invalid range: file #%u offset %llu in topic '%s'",
                 (unsigned long long)c->conn_id, idx, (unsigned long long)off, t->key);
        conn_send_error(c, FDS_ERR_IO, "internal error: invalid file range");
        return XFER_IOERR;
    }
    fe = &t->files[idx];
    if (path_join(path, sizeof(path), t->abs_path, fe->name) != 0)
        errno = ENAMETOOLONG;           /* path_join() does not set errno */
    else
        src.in = open(path, O_RDONLY | O_CLOEXEC);
    if (src.in < 0) {
        /* Still between messages, so the client can be told properly. */
        log_warn("conn %llu: cannot open %s/%s: %s", (unsigned long long)c->conn_id,
                 t->abs_path, fe->name, strerror(errno));
        snprintf(msg, sizeof(msg), "cannot read file '%s'", fe->name);
        conn_send_error(c, FDS_ERR_IO, msg);
        return XFER_IOERR;
    }

    memset(&h, 0, sizeof(h));
    h.file_index = idx;
    h.file_size = fe->size;
    h.start_offset = off;
    name_len = strnlen(fe->name, FDS_NAME_MAX);
    memcpy(h.name, fe->name, name_len);
    h.name[name_len] = '\0';
    n = enc_file_hdr(buf, sizeof(buf), &h);
    if (n == 0) {
        /* Cannot happen: the topic map only keeps names that pass
         * name_is_safe(), which is what the encoder checks. */
        conn_send_error(c, FDS_ERR_IO, "internal error: cannot encode FILE_HDR");
        rc = XFER_IOERR;
    } else if (conn_send(c, buf, n, 1) != 0) {
        c->status = "PEER_LOST";
        rc = XFER_PEER;
    } else {
        /* Documented: right after FILE_HDR, with n = 0, so the session knows
         * the current file even for a 0-byte file or a fault before chunk 1. */
        if (c->progress_hook != NULL)
            c->progress_hook(c, idx, off, 0);
        rc = send_chunks(c, &src, t, idx, off, fe->size);
    }

    if (rc == XFER_OK) {
        n = enc_file_end(buf, sizeof(buf), idx);
        if (conn_send(c, buf, n, 0) != 0) {     /* no MSG_MORE: flush now */
            c->status = "PEER_LOST";
            rc = XFER_PEER;
        } else {
            c->files_completed++;
        }
    }

    /* Read-only descriptor: close() cannot lose data, but report oddities. */
    if (close(src.in) != 0 && errno != EINTR)
        log_warn("close %s: %s", path, strerror(errno));
    free(src.buf);
    return rc;
}

xfer_rc_t serve_topic(conn_t *c, const topic_t *t, const uint8_t session_id[FDS_SESSION_ID_LEN],
                      uint32_t from_idx, uint64_t from_off, int send_begin)
{
    uint8_t buf[SMALL_MSG_MAX];
    m_topic_begin_t b;
    m_topic_end_t e;
    size_t n;

    /* from_idx == n_files is a valid resume point: everything was stored
     * except the TOPIC_END. Anything beyond is a bug in the caller. */
    if (from_idx > t->n_files ||
        (from_idx < t->n_files && from_off > t->files[from_idx].size)) {
        log_warn("conn %llu: invalid resume point #%u offset %llu in topic '%s'",
                 (unsigned long long)c->conn_id, from_idx, (unsigned long long)from_off, t->key);
        conn_send_error(c, FDS_ERR_IO, "internal error: invalid resume point");
        return XFER_IOERR;
    }

    if (send_begin) {
        memset(&b, 0, sizeof(b));
        if (session_id != NULL)         /* NULL = no session (Part I, Case 1): all zero */
            memcpy(b.session_id, session_id, FDS_SESSION_ID_LEN);
        b.file_count = t->n_files;
        b.total_bytes = t->total_bytes;
        n = enc_topic_begin(buf, sizeof(buf), &b);
        /* MSG_MORE: the first FILE_HDR follows at once. */
        if (conn_send(c, buf, n, 1) != 0) {
            c->status = "PEER_LOST";
            return XFER_PEER;
        }
    }

    for (uint32_t i = from_idx; i < t->n_files; i++) {
        xfer_rc_t rc = send_file_range(c, t, i, i == from_idx ? from_off : 0);

        if (rc != XFER_OK)
            return rc;
    }

    e.file_count = t->n_files;
    e.total_bytes = t->total_bytes;
    n = enc_topic_end(buf, sizeof(buf), &e);
    if (conn_send(c, buf, n, 0) != 0) {
        c->status = "PEER_LOST";
        return XFER_PEER;
    }
    return XFER_OK;
}

/* ===========================================================================
 * Client side
 * ======================================================================== */

rx_rc_t recv_expect(rx_ctx_t *rx, uint8_t want, uint8_t **payload, size_t *len)
{
    msg_hdr_t h;
    uint8_t *p = NULL;
    rx_rc_t rc;
    int r;

    *payload = NULL;
    if (len != NULL)
        *len = 0;
    r = recv_msg(rx->fd, &h, &p, &rx->wire_bytes);
    if (r == NET_EOF || r == NET_ERR)
        return RX_LOST;                 /* includes the SO_RCVTIMEO time-out (EAGAIN) */
    if (r != NET_OK)
        return RX_PROTO;                /* bad magic, version, type or length */

    if (h.type == MSG_ERROR)
        rc = dec_error(p, (size_t)h.payload_len, &rx->err) == 0 ? RX_SERVER_ERR : RX_PROTO;
    else if (h.type == MSG_RESUME_REJECT)
        rc = dec_resume_reject(p, (size_t)h.payload_len, &rx->reject_reason) == 0 ? RX_REJECT
                                                                                 : RX_PROTO;
    else if (h.type != want)
        rc = RX_PROTO;
    else
        rc = RX_OK;

    if (rc != RX_OK) {
        free(p);
        return rc;
    }
    *payload = p;
    if (len != NULL)
        *len = (size_t)h.payload_len;
    return RX_OK;
}

/* write() all n bytes (a regular file may take fewer per call). */
static int write_all(int fd, const uint8_t *p, size_t n)
{
    while (n > 0) {
        ssize_t w = write(fd, p, n);

        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0) {
            if (w == 0)
                errno = EIO;            /* no progress: do not spin */
            return -1;
        }
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

/*
 * RX_PART with start_offset > 0: reopen <name>.part and cut it back to
 * exactly start_offset bytes. Returns the descriptor, -2 when the file is
 * already complete under its final name (nothing to write), or -1 (RX_IOERR).
 */
static int reopen_part(const char *part, const char *final, const m_file_hdr_t *h)
{
    struct stat st;
    int fd;

    if (stat(part, &st) == 0) {
        /* The checkpoint can only come from the .part size, so a shorter file
         * means something else changed it: do not invent the missing bytes. */
        if ((uint64_t)st.st_size < h->start_offset) {
            log_warn("%s has %lld bytes but the server resumes at %llu",
                     part, (long long)st.st_size, (unsigned long long)h->start_offset);
            return -1;
        }
        fd = open(part, O_WRONLY | O_CLOEXEC);
        if (fd < 0) {
            log_warn("cannot open %s: %s", part, strerror(errno));
            return -1;
        }
        /* Longer than the offset: bytes written after the checkpoint was
         * taken (e.g. by a crashed earlier run). The server resends them. */
        if (ftruncate(fd, (off_t)h->start_offset) != 0 ||
            lseek(fd, (off_t)h->start_offset, SEEK_SET) == (off_t)-1) {
            log_warn("cannot resize %s: %s", part, strerror(errno));
            (void)close(fd);            /* already failing; nothing was written */
            return -1;
        }
        return fd;
    }
    if (errno != ENOENT) {
        log_warn("cannot stat %s: %s", part, strerror(errno));
        return -1;
    }
    /* No .part: the file was finished and renamed, but the connection broke
     * before the next FILE_HDR, so the checkpoint still names this file with
     * offset = its size. Only FILE_END is left to read. */
    if (h->start_offset == h->file_size && stat(final, &st) == 0 && S_ISREG(st.st_mode) &&
        (uint64_t)st.st_size == h->file_size)
        return -2;
    log_warn("cannot resume %s at offset %llu: no partial file", final,
             (unsigned long long)h->start_offset);
    return -1;
}

rx_rc_t recv_file(rx_ctx_t *rx, const m_file_hdr_t *h)
{
    char final[PATH_MAX], part[PATH_MAX];
    const char *target;
    uint64_t remaining;
    uint8_t *buf = NULL, *payload = NULL;
    size_t len;
    m_file_end_t fe;
    rx_rc_t rc = RX_OK;
    int fd = -1, have_file;

    /* Pure checks first, so a bad header never reaches the checkpoint hook.
     * dec_file_hdr() did them already, but recv_file() is public. */
    if (!name_is_safe(h->name) || h->start_offset > h->file_size)
        return RX_PROTO;
    if (rx->mode == RX_DIRECT && h->start_offset != 0)
        return RX_PROTO;                /* nothing to resume in Part I / Case 1 */
    if (path_join3(final, sizeof(final), rx->dir, h->name, "") != 0 ||
        path_join3(part, sizeof(part), rx->dir, h->name, ".part") != 0) {
        log_warn("output path for '%s' is too long", h->name);
        return RX_IOERR;
    }
    /* Case 2 saves its .session here, before the first data byte is written. */
    if (rx->on_file_hdr != NULL && rx->on_file_hdr(rx, h) != 0)
        return RX_IOERR;

    target = rx->mode == RX_PART ? part : final;
    if (rx->mode == RX_DIRECT || h->start_offset == 0) {
        /* O_TRUNC: a restarted Case 1 transfer overwrites the old attempt. */
        fd = open(target, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd < 0) {
            log_warn("cannot create %s: %s", target, strerror(errno));
            return RX_IOERR;
        }
    } else {
        fd = reopen_part(part, final, h);
        if (fd == -1)
            return RX_IOERR;
        if (fd == -2)
            fd = -1;                    /* complete already: only FILE_END follows */
    }
    have_file = fd >= 0;

    remaining = h->file_size - h->start_offset;
    if (remaining > 0 && (buf = malloc(FDS_IO_BUF)) == NULL) {
        log_warn("out of memory");
        rc = RX_IOERR;
    }
    while (rc == RX_OK && remaining > 0) {
        size_t want = (size_t)FDS_MIN((uint64_t)FDS_IO_BUF, remaining);
        ssize_t r = recv(rx->fd, buf, want, 0);

        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
            rc = RX_LOST;               /* EOF, reset or SO_RCVTIMEO: retry may help */
            break;
        }
        rx->wire_bytes += (uint64_t)r;
        /* Written before the next recv(): the .part size is the checkpoint. */
        if (write_all(fd, buf, (size_t)r) != 0) {
            log_warn("write %s: %s", target, strerror(errno));
            rc = RX_IOERR;
            break;
        }
        rx->data_bytes += (uint64_t)r;
        remaining -= (uint64_t)r;
    }
    free(buf);
    if (rc != RX_OK) {
        /* The bytes received so far stay on disk for the next attempt. */
        if (have_file)
            (void)close(fd);            /* already failing; the size on disk is what counts */
        return rc;
    }

    rc = recv_expect(rx, MSG_FILE_END, &payload, &len);
    if (rc == RX_OK && (dec_file_end(payload, len, &fe) != 0 || fe.file_index != h->file_index))
        rc = RX_PROTO;
    free(payload);
    if (rc != RX_OK) {
        if (have_file)
            (void)close(fd);
        return rc;
    }

    /* close() is where some file systems report a failed write-back (NFS,
     * quota): the file would be incomplete, so this is fatal. */
    if (have_file && close(fd) != 0) {
        log_warn("close %s: %s", target, strerror(errno));
        return RX_IOERR;
    }
    /* The final name appears only once the file is complete. rename() is
     * atomic and replaces an older copy. In the "already complete" case
     * there is no .part to rename. */
    if (rx->mode == RX_PART && have_file && rename(part, final) != 0) {
        log_warn("rename %s: %s", part, strerror(errno));
        return RX_IOERR;
    }
    rx->files_done++;
    return RX_OK;
}

rx_rc_t recv_topic(rx_ctx_t *rx, m_topic_end_t *end)
{
    m_topic_end_t local;

    if (end == NULL)
        end = &local;
    for (;;) {
        msg_hdr_t h;
        m_file_hdr_t fh;
        uint8_t *p = NULL;
        size_t len;
        rx_rc_t rc;
        int r = recv_msg(rx->fd, &h, &p, &rx->wire_bytes);

        if (r == NET_EOF || r == NET_ERR)
            return RX_LOST;             /* no TOPIC_END yet: the transfer was cut */
        if (r != NET_OK)
            return RX_PROTO;
        len = (size_t)h.payload_len;

        switch (h.type) {
        case MSG_FILE_HDR:
            rc = dec_file_hdr(p, len, &fh) == 0 ? RX_OK : RX_PROTO;
            free(p);
            if (rc != RX_OK)
                return rc;
            /* Files must arrive in index order with no gap: that is what
             * makes "next index + .part size" a complete checkpoint. */
            if (fh.file_index != rx->next_index)
                return RX_PROTO;
            rc = recv_file(rx, &fh);
            if (rc != RX_OK)
                return rc;
            rx->stored_bytes += fh.file_size;
            rx->next_index++;
            break;
        case MSG_TOPIC_END:
            rc = dec_topic_end(p, len, end) == 0 ? RX_OK : RX_PROTO;
            free(p);
            return rc;
        case MSG_ERROR:
            rc = dec_error(p, len, &rx->err) == 0 ? RX_SERVER_ERR : RX_PROTO;
            free(p);
            return rc;
        default:
            free(p);
            return RX_PROTO;
        }
    }
}
