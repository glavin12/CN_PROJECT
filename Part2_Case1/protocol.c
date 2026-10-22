/*
 * protocol.c - message framing, encoding and decoding (see protocol.h and
 * design section 3).
 *
 * Design rules applied throughout:
 *   - Structures are never sent raw. Every field is written byte by byte in
 *     big-endian order through wbuf_t / rbuf_t, so compiler padding and the
 *     host's byte order can never leak onto the wire.
 *   - Writers and readers are bounds-checked and "sticky": the first overflow
 *     or underflow sets err and every later call does nothing. An encoder or
 *     decoder can therefore do all its put/get calls and check err once at the
 *     end, and still never touch memory outside its buffer.
 *   - Decoders are strict. Every byte comes from the network, so a payload is
 *     accepted only if it has exactly the expected length, every string fits
 *     its limit, and topics and file names pass the same validation the rest
 *     of the program relies on (a file name from the server becomes a path on
 *     the client's disk).
 *   - Encoders refuse to produce anything the decoders would reject, so a bug
 *     on the sending side shows up where it happens.
 */
#include "protocol.h"
#include "net.h"
#include "util.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

/* ---- Message type table ---------------------------------------------------- */

/* The single list of valid types: msg_type_name() and hdr_decode() both use
 * it, so a type cannot be known to one and unknown to the other. */
static const struct {
    uint8_t     type;
    const char *name;
} msg_types[] = {
    { MSG_GET,           "GET" },
    { MSG_RESUME,        "RESUME" },
    { MSG_MANIFEST_REQ,  "MANIFEST_REQ" },
    { MSG_FETCH,         "FETCH" },
    { MSG_TOPIC_BEGIN,   "TOPIC_BEGIN" },
    { MSG_FILE_HDR,      "FILE_HDR" },
    { MSG_FILE_END,      "FILE_END" },
    { MSG_TOPIC_END,     "TOPIC_END" },
    { MSG_RESUME_OK,     "RESUME_OK" },
    { MSG_RESUME_REJECT, "RESUME_REJECT" },
    { MSG_MANIFEST,      "MANIFEST" },
    { MSG_ERROR,         "ERROR" },
};
#define N_MSG_TYPES (sizeof(msg_types) / sizeof(msg_types[0]))

/* Position of type in msg_types[], or -1 if it is not a valid type. */
static int type_index(uint8_t type)
{
    for (size_t i = 0; i < N_MSG_TYPES; i++)
        if (msg_types[i].type == type)
            return (int)i;
    return -1;
}

const char *msg_type_name(uint8_t type)
{
    int i = type_index(type);

    return i >= 0 ? msg_types[i].name : "?";
}

/* Longest ERROR text kept: m_error_t.msg has room for it plus the NUL. */
#define ERR_MSG_MAX 255u
_Static_assert(sizeof(((m_error_t *)0)->msg) == ERR_MSG_MAX + 1,
               "m_error_t.msg must hold ERR_MSG_MAX bytes plus a NUL");

/* MANIFEST sizes: the fixed part (session_id, file_count u32, total_bytes
 * u64) and the smallest possible entry (file_size u64, name_len u16, and a
 * name of at least 1 byte). */
#define MANIFEST_FIXED     (FDS_SESSION_ID_LEN + 4u + 8u)
#define MANIFEST_MIN_ENTRY (8u + 2u + 1u)

/* sum += v, or return -1 if the 64-bit total would wrap around (a hostile
 * peer could otherwise make huge sizes "add up" to a small total_bytes). */
static int add_u64(uint64_t *sum, uint64_t v)
{
    if (v > UINT64_MAX - *sum)
        return -1;
    *sum += v;
    return 0;
}

/* Indexed by reason code; index 0 is unused (codes start at 1, design 5.5). */
static const char *const reject_reasons[] = {
    [FDS_REJ_UNKNOWN_TOPIC]  = "unknown topic",
    [FDS_REJ_INDEX_RANGE]    = "file index out of range",
    [FDS_REJ_NAME_MISMATCH]  = "file name mismatch",
    [FDS_REJ_OFFSET_TOO_BIG] = "offset beyond file size",
    [FDS_REJ_AHEAD_OF_SENT]  = "checkpoint ahead of data sent",
};

const char *reject_reason_str(uint16_t reason)
{
    if (reason < sizeof(reject_reasons) / sizeof(reject_reasons[0]) &&
        reject_reasons[reason] != NULL)
        return reject_reasons[reason];
    return "unknown reason";
}

/* ---- Byte-buffer writer ---------------------------------------------------- */

void wb_init(wbuf_t *w, uint8_t *buf, size_t cap)
{
    w->p = buf;
    w->cap = cap;
    w->len = 0;
    w->err = 0;
}

/* 1 if n more bytes fit. Written as n > cap - len (never len + n > cap) so
 * that a huge n cannot overflow the addition and pass the check. */
static int wb_room(wbuf_t *w, size_t n)
{
    if (w->err || n > w->cap - w->len) {
        w->err = 1;
        return 0;
    }
    return 1;
}

void put_u8(wbuf_t *w, uint8_t v)
{
    if (!wb_room(w, 1))
        return;
    w->p[w->len++] = v;
}

void put_u16(wbuf_t *w, uint16_t v)
{
    if (!wb_room(w, 2))
        return;
    w->p[w->len++] = (uint8_t)(v >> 8);    /* most significant byte first */
    w->p[w->len++] = (uint8_t)v;
}

void put_u32(wbuf_t *w, uint32_t v)
{
    if (!wb_room(w, 4))
        return;
    for (int shift = 24; shift >= 0; shift -= 8)
        w->p[w->len++] = (uint8_t)(v >> shift);
}

void put_u64(wbuf_t *w, uint64_t v)
{
    if (!wb_room(w, 8))
        return;
    for (int shift = 56; shift >= 0; shift -= 8)
        w->p[w->len++] = (uint8_t)(v >> shift);
}

void put_bytes(wbuf_t *w, const void *src, size_t n)
{
    if (!wb_room(w, n))
        return;
    if (n > 0)                          /* memcpy(dst, NULL, 0) is undefined */
        memcpy(w->p + w->len, src, n);
    w->len += n;
}

/* ---- Byte-buffer reader ---------------------------------------------------- */

void rb_init(rbuf_t *r, const uint8_t *buf, size_t len)
{
    r->p = buf;
    r->len = len;
    r->pos = 0;
    r->err = 0;
}

/* Consume n bytes: returns a pointer to them, or NULL (and err=1) if fewer
 * than n remain. The same overflow-safe comparison as wb_room(). */
static const uint8_t *rb_take(rbuf_t *r, size_t n)
{
    const uint8_t *q;

    if (r->err || n > r->len - r->pos) {
        r->err = 1;
        return NULL;
    }
    q = r->p + r->pos;
    r->pos += n;
    return q;
}

uint8_t get_u8(rbuf_t *r)
{
    const uint8_t *q = rb_take(r, 1);

    return q ? q[0] : 0;
}

uint16_t get_u16(rbuf_t *r)
{
    const uint8_t *q = rb_take(r, 2);

    return q ? (uint16_t)((uint16_t)q[0] << 8 | q[1]) : 0;
}

uint32_t get_u32(rbuf_t *r)
{
    const uint8_t *q = rb_take(r, 4);
    uint32_t v = 0;

    if (q != NULL)
        for (int i = 0; i < 4; i++)
            v = v << 8 | q[i];
    return v;
}

uint64_t get_u64(rbuf_t *r)
{
    const uint8_t *q = rb_take(r, 8);
    uint64_t v = 0;

    if (q != NULL)
        for (int i = 0; i < 8; i++)
            v = v << 8 | q[i];
    return v;
}

void get_bytes(rbuf_t *r, void *dst, size_t n)
{
    const uint8_t *q = rb_take(r, n);

    if (q != NULL && n > 0)
        memcpy(dst, q, n);
}

void get_str(rbuf_t *r, char *dst, size_t n, size_t cap)
{
    const uint8_t *q;

    /* The length prefix came from the peer: a string that cannot fit with its
     * NUL is a protocol error, not something to truncate silently (a cut-off
     * file name would name a different file). */
    if (n >= cap) {
        r->err = 1;
        return;
    }
    q = rb_take(r, n);
    if (q == NULL)
        return;
    memcpy(dst, q, n);
    dst[n] = '\0';
    /* The wire format is length-prefixed, so a NUL inside means the C string
     * would silently be shorter than what the peer meant: reject it. */
    if (memchr(q, '\0', n) != NULL)
        r->err = 1;
}

/* 1 if the whole payload was consumed without error. Strict decoders use it
 * to reject both short payloads (err) and trailing bytes (pos != len). */
static int rb_done(const rbuf_t *r)
{
    return !r->err && r->pos == r->len;
}

/* ---- Header ---------------------------------------------------------------- */

uint64_t max_payload_for_type(uint8_t type)
{
    if (type_index(type) < 0)
        return 0;
    /* Deviation 2: the MANIFEST of a big topic (cane: 4 863 files, ~250 KB)
     * does not fit the 64 KB control-message limit. */
    return type == MSG_MANIFEST ? FDS_MAX_MANIFEST_PAYLOAD : FDS_MAX_CTRL_PAYLOAD;
}

void hdr_encode(uint8_t out[FDS_HDR_LEN], uint8_t type, uint64_t payload_len)
{
    wbuf_t w;

    wb_init(&w, out, FDS_HDR_LEN);
    put_u16(&w, FDS_MAGIC);
    put_u8(&w, FDS_VERSION);
    put_u8(&w, type);
    put_u32(&w, 0);                     /* reserved for future flags */
    put_u64(&w, payload_len);
}

int hdr_decode(const uint8_t in[FDS_HDR_LEN], msg_hdr_t *h)
{
    rbuf_t r;
    uint16_t magic;
    uint8_t version, type;
    uint64_t len;

    rb_init(&r, in, FDS_HDR_LEN);
    magic = get_u16(&r);
    version = get_u8(&r);
    type = get_u8(&r);
    (void)get_u32(&r);                  /* reserved: ignored so that a future
                                           version may define flags there */
    len = get_u64(&r);

    /* Magic and version catch a peer that is not speaking this protocol, or
     * a stream that lost its framing (e.g. after a bug in a length). */
    if (magic != FDS_MAGIC || version != FDS_VERSION)
        return -1;
    /* Checking the length here, before anything is allocated, is what stops a
     * hostile header from making recv_msg() malloc gigabytes. */
    if (type_index(type) < 0 || len > max_payload_for_type(type))
        return -1;
    h->type = type;
    h->payload_len = len;
    return 0;
}

/* ---- Encoder helpers -------------------------------------------------------- */

/* Start a message: leave room for the header, which is filled in by
 * enc_finish() once the payload length is known. */
static void enc_begin(wbuf_t *w, uint8_t *buf, size_t cap)
{
    wb_init(w, buf, cap);
    if (!wb_room(w, FDS_HDR_LEN))
        return;
    w->len = FDS_HDR_LEN;
}

/* Finish a message: returns its total length, or 0 if anything overflowed or
 * the payload exceeds what hdr_decode() would accept for this type. */
static size_t enc_finish(wbuf_t *w, uint8_t type)
{
    uint64_t payload_len;

    if (w->err)
        return 0;
    payload_len = w->len - FDS_HDR_LEN;
    if (payload_len > max_payload_for_type(type))
        return 0;
    hdr_encode(w->p, type, payload_len);
    return w->len;
}

/* Validate the char[] fields of the message structs. strnlen() comes first so
 * that a field the caller forgot to NUL-terminate is rejected instead of
 * being read past its end by strlen() inside the validators. */
static int topic_field_ok(const char *topic)
{
    return strnlen(topic, FDS_TOPIC_MAX + 1) <= FDS_TOPIC_MAX && topic_is_valid(topic);
}

static int name_field_ok(const char *name)
{
    return strnlen(name, FDS_NAME_MAX + 1) <= FDS_NAME_MAX && name_is_safe(name);
}

/* ---- Encoders --------------------------------------------------------------- */

size_t enc_get(uint8_t *buf, size_t cap, const char *topic)
{
    wbuf_t w;

    if (!topic_is_valid(topic))
        return 0;
    enc_begin(&w, buf, cap);
    /* Exactly the request text of the assignment: no NUL, no newline. */
    put_bytes(&w, "GET ", 4);
    put_bytes(&w, topic, strlen(topic));
    return enc_finish(&w, MSG_GET);
}

size_t enc_topic_begin(uint8_t *buf, size_t cap, const m_topic_begin_t *m)
{
    wbuf_t w;

    if (m == NULL)
        return 0;
    enc_begin(&w, buf, cap);
    put_bytes(&w, m->session_id, FDS_SESSION_ID_LEN);
    put_u32(&w, m->file_count);
    put_u64(&w, m->total_bytes);
    return enc_finish(&w, MSG_TOPIC_BEGIN);
}

size_t enc_file_hdr(uint8_t *buf, size_t cap, const m_file_hdr_t *m)
{
    wbuf_t w;

    /* start_offset > file_size would announce a negative number of data
     * bytes; the receiver would reject it, so never send it. */
    if (m == NULL || !name_field_ok(m->name) || m->start_offset > m->file_size)
        return 0;
    enc_begin(&w, buf, cap);
    put_u32(&w, m->file_index);
    put_u64(&w, m->file_size);
    put_u64(&w, m->start_offset);
    put_u16(&w, (uint16_t)strlen(m->name));
    put_bytes(&w, m->name, strlen(m->name));
    return enc_finish(&w, MSG_FILE_HDR);
}

size_t enc_file_end(uint8_t *buf, size_t cap, uint32_t file_index)
{
    wbuf_t w;

    enc_begin(&w, buf, cap);
    put_u32(&w, file_index);
    return enc_finish(&w, MSG_FILE_END);
}

size_t enc_topic_end(uint8_t *buf, size_t cap, const m_topic_end_t *m)
{
    wbuf_t w;

    if (m == NULL)
        return 0;
    enc_begin(&w, buf, cap);
    put_u32(&w, m->file_count);
    put_u64(&w, m->total_bytes);
    return enc_finish(&w, MSG_TOPIC_END);
}

size_t enc_resume(uint8_t *buf, size_t cap, uint8_t type, const m_resume_t *m)
{
    wbuf_t w;

    /* RESUME and FETCH share one layout (design 3.3); only the type differs. */
    if ((type != MSG_RESUME && type != MSG_FETCH) || m == NULL ||
        !topic_field_ok(m->topic) || !name_field_ok(m->name))
        return 0;
    enc_begin(&w, buf, cap);
    put_bytes(&w, m->session_id, FDS_SESSION_ID_LEN);
    put_u32(&w, m->file_index);
    put_u64(&w, m->offset);
    put_u8(&w, (uint8_t)strlen(m->topic));
    put_bytes(&w, m->topic, strlen(m->topic));
    put_u16(&w, (uint16_t)strlen(m->name));
    put_bytes(&w, m->name, strlen(m->name));
    return enc_finish(&w, type);
}

size_t enc_resume_ok(uint8_t *buf, size_t cap, const m_resume_ok_t *m)
{
    wbuf_t w;

    if (m == NULL)
        return 0;
    enc_begin(&w, buf, cap);
    put_u32(&w, m->file_index);
    put_u64(&w, m->offset);
    return enc_finish(&w, MSG_RESUME_OK);
}

size_t enc_resume_reject(uint8_t *buf, size_t cap, uint16_t reason)
{
    wbuf_t w;

    enc_begin(&w, buf, cap);
    put_u16(&w, reason);
    return enc_finish(&w, MSG_RESUME_REJECT);
}

size_t enc_manifest_req(uint8_t *buf, size_t cap, const m_manifest_req_t *m)
{
    wbuf_t w;

    if (m == NULL || !topic_field_ok(m->topic))
        return 0;
    enc_begin(&w, buf, cap);
    put_u8(&w, (uint8_t)strlen(m->topic));
    put_bytes(&w, m->topic, strlen(m->topic));
    put_u8(&w, m->connections);
    return enc_finish(&w, MSG_MANIFEST_REQ);
}

size_t enc_error(uint8_t *buf, size_t cap, uint16_t code, const char *msg)
{
    wbuf_t w;
    /* m_error_t holds at most 255 bytes of text, so never send more: the
     * receiver could not keep it anyway. strnlen() also bounds the scan. */
    size_t n = msg != NULL ? strnlen(msg, ERR_MSG_MAX) : 0;

    enc_begin(&w, buf, cap);
    put_u16(&w, code);
    put_u16(&w, (uint16_t)n);
    put_bytes(&w, msg, n);
    return enc_finish(&w, MSG_ERROR);
}

uint8_t *enc_manifest_alloc(const uint8_t session_id[FDS_SESSION_ID_LEN],
                            const file_entry_t *files, uint32_t n_files,
                            uint64_t total_bytes, size_t *len_out)
{
    uint64_t payload = MANIFEST_FIXED;
    uint64_t sum = 0;
    uint8_t *buf;
    size_t total;
    wbuf_t w;

    if (len_out != NULL)
        *len_out = 0;
    if (session_id == NULL || (files == NULL && n_files > 0)) {
        errno = EINVAL;
        return NULL;
    }
    /* First pass: validate and size, so exactly the needed bytes are
     * allocated. Stopping as soon as the cap is passed also keeps `payload`
     * far from overflowing. */
    for (uint32_t i = 0; i < n_files; i++) {
        if (!name_field_ok(files[i].name) || add_u64(&sum, files[i].size) != 0) {
            errno = EINVAL;
            return NULL;
        }
        payload += 8 + 2 + strlen(files[i].name);
        if (payload > FDS_MAX_MANIFEST_PAYLOAD) {
            errno = EMSGSIZE;
            return NULL;
        }
    }
    /* The receiver checks that the sizes add up to total_bytes; an
     * inconsistent manifest is a sender bug, so report it here. */
    if (sum != total_bytes) {
        errno = EINVAL;
        return NULL;
    }
    total = FDS_HDR_LEN + (size_t)payload;
    buf = malloc(total);
    if (buf == NULL)
        return NULL;                    /* errno = ENOMEM */

    enc_begin(&w, buf, total);
    put_bytes(&w, session_id, FDS_SESSION_ID_LEN);
    put_u32(&w, n_files);
    put_u64(&w, total_bytes);
    for (uint32_t i = 0; i < n_files; i++) {
        size_t nl = strlen(files[i].name);

        put_u64(&w, files[i].size);
        put_u16(&w, (uint16_t)nl);
        put_bytes(&w, files[i].name, nl);
    }
    /* Cannot fail after the sizing pass; checked so a future change to one
     * pass but not the other is caught instead of sending garbage. */
    if (enc_finish(&w, MSG_MANIFEST) != total) {
        free(buf);
        errno = EINVAL;
        return NULL;
    }
    if (len_out != NULL)
        *len_out = total;
    return buf;
}

/* ---- Decoders --------------------------------------------------------------- */

int dec_get(const uint8_t *p, size_t len, char topic[FDS_TOPIC_MAX + 1])
{
    size_t n;

    topic[0] = '\0';
    /* The prefix is case-sensitive and needs exactly one space: "GET <topic>"
     * is the form fixed by the assignment. */
    if (len < 4 || len - 4 > FDS_TOPIC_MAX || memcmp(p, "GET ", 4) != 0)
        return -1;
    n = len - 4;
    /* An embedded NUL would make the topic shorter than what was sent. */
    if (n > 0 && memchr(p + 4, '\0', n) != NULL)
        return -1;
    if (n > 0)
        memcpy(topic, p + 4, n);
    topic[n] = '\0';
    if (!topic_is_valid(topic)) {      /* also rejects the empty topic */
        topic[0] = '\0';
        return -1;
    }
    return 0;
}

int dec_topic_begin(const uint8_t *p, size_t len, m_topic_begin_t *m)
{
    rbuf_t r;

    rb_init(&r, p, len);
    get_bytes(&r, m->session_id, FDS_SESSION_ID_LEN);
    m->file_count = get_u32(&r);
    m->total_bytes = get_u64(&r);
    return rb_done(&r) ? 0 : -1;
}

int dec_file_hdr(const uint8_t *p, size_t len, m_file_hdr_t *m)
{
    rbuf_t r;
    uint16_t name_len;

    rb_init(&r, p, len);
    m->name[0] = '\0';
    m->file_index = get_u32(&r);
    m->file_size = get_u64(&r);
    m->start_offset = get_u64(&r);
    name_len = get_u16(&r);
    get_str(&r, m->name, name_len, sizeof(m->name));
    if (!rb_done(&r))
        return -1;
    /* The client turns this name into a path: no '/', no leading '.', no
     * control characters (design 3.4 and 7). */
    if (!name_is_safe(m->name))
        return -1;
    /* The number of raw bytes that follow is file_size - start_offset; it must
     * not be negative. */
    if (m->start_offset > m->file_size)
        return -1;
    return 0;
}

int dec_file_end(const uint8_t *p, size_t len, m_file_end_t *m)
{
    rbuf_t r;

    rb_init(&r, p, len);
    m->file_index = get_u32(&r);
    return rb_done(&r) ? 0 : -1;
}

int dec_topic_end(const uint8_t *p, size_t len, m_topic_end_t *m)
{
    rbuf_t r;

    rb_init(&r, p, len);
    m->file_count = get_u32(&r);
    m->total_bytes = get_u64(&r);
    return rb_done(&r) ? 0 : -1;
}

int dec_resume(const uint8_t *p, size_t len, m_resume_t *m)
{
    rbuf_t r;
    uint8_t topic_len;
    uint16_t name_len;

    rb_init(&r, p, len);
    m->topic[0] = '\0';
    m->name[0] = '\0';
    get_bytes(&r, m->session_id, FDS_SESSION_ID_LEN);
    m->file_index = get_u32(&r);
    m->offset = get_u64(&r);
    topic_len = get_u8(&r);
    get_str(&r, m->topic, topic_len, sizeof(m->topic));
    name_len = get_u16(&r);
    get_str(&r, m->name, name_len, sizeof(m->name));
    if (!rb_done(&r))
        return -1;
    /* The server looks the topic up and compares the name with its own file
     * list; validating both here keeps that code free of odd inputs. */
    if (!topic_is_valid(m->topic) || !name_is_safe(m->name))
        return -1;
    return 0;
}

int dec_resume_ok(const uint8_t *p, size_t len, m_resume_ok_t *m)
{
    rbuf_t r;

    rb_init(&r, p, len);
    m->file_index = get_u32(&r);
    m->offset = get_u64(&r);
    return rb_done(&r) ? 0 : -1;
}

int dec_resume_reject(const uint8_t *p, size_t len, uint16_t *reason)
{
    rbuf_t r;

    rb_init(&r, p, len);
    /* Any reason value is accepted: an unknown one still means "rejected"
     * and reject_reason_str() prints it as "unknown reason". */
    *reason = get_u16(&r);
    return rb_done(&r) ? 0 : -1;
}

int dec_manifest_req(const uint8_t *p, size_t len, m_manifest_req_t *m)
{
    rbuf_t r;
    uint8_t topic_len;

    rb_init(&r, p, len);
    m->topic[0] = '\0';
    topic_len = get_u8(&r);
    get_str(&r, m->topic, topic_len, sizeof(m->topic));
    /* The range of K is policy, so the server checks it, not the decoder. */
    m->connections = get_u8(&r);
    if (!rb_done(&r) || !topic_is_valid(m->topic))
        return -1;
    return 0;
}

int dec_manifest(const uint8_t *p, size_t len, m_manifest_t *m)
{
    rbuf_t r;
    uint8_t sid[FDS_SESSION_ID_LEN] = { 0 };
    uint32_t count;
    uint64_t total, sum = 0;
    file_entry_t *files;

    m->files = NULL;                    /* manifest_free() is always safe */
    if (len < MANIFEST_FIXED)
        return -1;
    rb_init(&r, p, len);
    get_bytes(&r, sid, sizeof(sid));
    count = get_u32(&r);
    total = get_u64(&r);
    /* Reject a file_count the payload cannot possibly hold BEFORE allocating:
     * otherwise 28 hostile bytes claiming 4 billion files would make us
     * calloc() about 1 TB. */
    if ((uint64_t)count > (len - MANIFEST_FIXED) / MANIFEST_MIN_ENTRY)
        return -1;
    /* At least one element, so that success always comes with a valid
     * pointer, even for an empty topic. */
    files = calloc(count > 0 ? count : 1, sizeof(*files));
    if (files == NULL)
        return -1;
    for (uint32_t i = 0; i < count; i++) {
        uint16_t name_len;

        files[i].size = get_u64(&r);
        name_len = get_u16(&r);
        get_str(&r, files[i].name, name_len, sizeof(files[i].name));
        if (r.err || !name_is_safe(files[i].name) || add_u64(&sum, files[i].size) != 0)
            goto fail;
    }
    /* No trailing bytes, and the per-file sizes must add up to the announced
     * total: the client uses total_bytes to check that it received everything. */
    if (!rb_done(&r) || sum != total)
        goto fail;
    memcpy(m->session_id, sid, sizeof(sid));
    m->file_count = count;
    m->total_bytes = total;
    m->files = files;
    return 0;

fail:
    free(files);
    m->files = NULL;
    return -1;
}

void manifest_free(m_manifest_t *m)
{
    if (m == NULL)
        return;
    free(m->files);
    m->files = NULL;                    /* a second call is harmless */
}

int dec_error(const uint8_t *p, size_t len, m_error_t *m)
{
    rbuf_t r;
    uint16_t msg_len;
    size_t keep;

    rb_init(&r, p, len);
    m->msg[0] = '\0';
    m->code = get_u16(&r);
    msg_len = get_u16(&r);
    /* The framing stays strict (the payload must be exactly 4 + msg_len
     * bytes), but the text is only diagnostics: a longer one is truncated to
     * what m->msg can hold instead of rejecting the whole error. */
    keep = FDS_MIN((size_t)msg_len, (size_t)ERR_MSG_MAX);
    get_bytes(&r, m->msg, keep);
    (void)rb_take(&r, msg_len - keep);  /* skip the part that does not fit */
    if (!rb_done(&r)) {
        m->msg[0] = '\0';
        return -1;
    }
    m->msg[keep] = '\0';
    /* The client prints this text: replace control characters (and NULs) so
     * a server cannot inject terminal escape sequences or hide text. */
    for (size_t i = 0; i < keep; i++) {
        unsigned char c = (unsigned char)m->msg[i];

        if (c < 0x20 || c == 0x7f)
            m->msg[i] = '?';
    }
    return 0;
}

/* ---- Blocking fd helpers ------------------------------------------------------ */

int send_encoded(int fd, const uint8_t *buf, size_t len, int more)
{
    /* No valid message is shorter than its header. This catches the common
     * mistake of passing the 0 that an encoder returned on failure, which
     * would otherwise "succeed" by sending nothing. */
    if (buf == NULL || len < FDS_HDR_LEN) {
        errno = EINVAL;
        return -1;
    }
    /* MSG_MORE tells TCP that more data follows at once (e.g. the file bytes
     * after a FILE_HDR), so the header is not sent in a tiny packet of its own. */
    return sendall(fd, buf, len, more ? MSG_MORE : 0);
}

void send_error(int fd, uint16_t code, const char *msg)
{
    int saved = errno;
    uint8_t buf[FDS_HDR_LEN + 4 + ERR_MSG_MAX];
    size_t n = enc_error(buf, sizeof(buf), code, msg);

    /* Best effort: the connection is about to be closed anyway, and the
     * caller wants to report its own error, so errno is left as it was. */
    if (n > 0)
        (void)send_encoded(fd, buf, n, 0);
    errno = saved;
}

int recv_msg(int fd, msg_hdr_t *h, uint8_t **payload, uint64_t *wire)
{
    uint8_t hb[FDS_HDR_LEN];
    uint8_t *buf;
    size_t got = 0;
    int rc, err;

    *payload = NULL;                    /* free(*payload) is safe on every path */

    rc = recvall(fd, hb, sizeof(hb), &got);
    if (wire != NULL)
        *wire += got;                   /* counted even when the read failed */
    if (rc == NET_EOF) {
        /* Nothing at all: the peer closed between two messages, which is how
         * a server ends a conversation. Part of a header: the connection was
         * cut inside a message, i.e. a failure. */
        if (got == 0)
            return NET_EOF;
        errno = ECONNRESET;
        return NET_ERR;
    }
    if (rc != NET_OK)
        return NET_ERR;                 /* errno from recv() */
    if (hdr_decode(hb, h) != 0)
        return NET_PROTO;
    if (h->payload_len == 0)
        return NET_OK;

    /* payload_len is at most 64 MiB here (checked by hdr_decode()). */
    buf = malloc((size_t)h->payload_len);
    if (buf == NULL)
        return NET_ERR;                 /* errno = ENOMEM */
    rc = recvall(fd, buf, (size_t)h->payload_len, &got);
    if (wire != NULL)
        *wire += got;
    if (rc != NET_OK) {
        err = rc == NET_EOF ? ECONNRESET : errno;
        free(buf);                      /* free() may change errno */
        errno = err;
        return NET_ERR;
    }
    *payload = buf;
    return NET_OK;
}
