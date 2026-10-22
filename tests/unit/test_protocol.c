/*
 * test_protocol.c - unit tests for protocol.c: header layout, byte buffers,
 * every encoder/decoder pair, size boundaries and malformed input.
 *
 * Every byte a decoder sees comes from the network, so most of this file is
 * about rejection: truncation at every length, trailing bytes, strings at and
 * just over their limits, unsafe names and hostile counts.
 */
#include "unit.h"
#include "fds.h"
#include "protocol.h"
#include "util.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ---- Helpers --------------------------------------------------------------- */

/* Decode a payload of any type into throw-away structs. 0 = accepted. */
static int dec_any(uint8_t type, const uint8_t *p, size_t len)
{
    switch (type) {
    case MSG_GET: {
        char t[FDS_TOPIC_MAX + 1];
        return dec_get(p, len, t);
    }
    case MSG_RESUME:
    case MSG_FETCH: {
        m_resume_t m;
        return dec_resume(p, len, &m);
    }
    case MSG_MANIFEST_REQ: {
        m_manifest_req_t m;
        return dec_manifest_req(p, len, &m);
    }
    case MSG_TOPIC_BEGIN: {
        m_topic_begin_t m;
        return dec_topic_begin(p, len, &m);
    }
    case MSG_FILE_HDR: {
        m_file_hdr_t m;
        return dec_file_hdr(p, len, &m);
    }
    case MSG_FILE_END: {
        m_file_end_t m;
        return dec_file_end(p, len, &m);
    }
    case MSG_TOPIC_END: {
        m_topic_end_t m;
        return dec_topic_end(p, len, &m);
    }
    case MSG_RESUME_OK: {
        m_resume_ok_t m;
        return dec_resume_ok(p, len, &m);
    }
    case MSG_RESUME_REJECT: {
        uint16_t r;
        return dec_resume_reject(p, len, &r);
    }
    case MSG_MANIFEST: {
        m_manifest_t m;
        int rc = dec_manifest(p, len, &m);
        manifest_free(&m);
        return rc;
    }
    case MSG_ERROR: {
        m_error_t m;
        return dec_error(p, len, &m);
    }
    }
    return -1;
}

/* Decode a payload from an exact-size heap copy, so that an over-read would
 * be caught by AddressSanitizer instead of reading neighbouring bytes. */
static int dec_copy(uint8_t type, const uint8_t *p, size_t len)
{
    uint8_t *c = malloc(len > 0 ? len : 1);
    int rc;

    if (len > 0)
        memcpy(c, p, len);
    rc = dec_any(type, len > 0 ? c : NULL, len);
    free(c);
    return rc;
}

/*
 * Check a complete encoded message: its header decodes to `type` with the
 * right length, the exact payload is accepted, and every shorter prefix and
 * the payload plus one trailing byte are rejected.
 *
 * GET is the one text message: a shorter or longer topic is still a topic.
 * For it, prefixes of at least `valid_prefix` bytes are skipped, and the
 * trailing bytes tried are only '\n' and NUL (the realistic mistakes).
 */
static void check_strict(const uint8_t *msg, size_t n, uint8_t type, size_t valid_prefix)
{
    static const uint8_t tails[] = { 'x', 0x00, '\n' };
    msg_hdr_t h;
    const uint8_t *pl = msg + FDS_HDR_LEN;
    size_t plen = n - FDS_HDR_LEN;
    uint8_t *longer;

    CHECK(n >= FDS_HDR_LEN);
    CHECK_EQ(hdr_decode(msg, &h), 0);
    CHECK_EQ(h.type, type);
    CHECK_EQ(h.payload_len, plen);
    CHECK_EQ(dec_copy(type, pl, plen), 0);
    for (size_t k = 0; k < plen && k < valid_prefix; k++) {
        int rc = dec_copy(type, pl, k);

        if (rc != -1)
            fprintf(stderr, "type %s: truncated payload of %zu/%zu bytes accepted\n",
                    msg_type_name(type), k, plen);
        CHECK_EQ(rc, -1);
    }
    longer = malloc(plen + 1);
    memcpy(longer, pl, plen);
    for (size_t i = type == MSG_GET ? 1 : 0; i < sizeof(tails); i++) {
        int rc;

        longer[plen] = tails[i];
        rc = dec_copy(type, longer, plen + 1);
        if (rc != -1)
            fprintf(stderr, "type %s: trailing byte 0x%02x accepted\n",
                    msg_type_name(type), tails[i]);
        CHECK_EQ(rc, -1);
    }
    free(longer);
}

/* Fill s with n copies of c and terminate it. */
static char *fill(char *s, char c, size_t n)
{
    memset(s, c, n);
    s[n] = '\0';
    return s;
}

/* ---- Sample messages ------------------------------------------------------- */

static m_topic_begin_t  s_tb;
static m_file_hdr_t     s_fh;
static m_topic_end_t    s_te;
static m_resume_t       s_rs;
static m_resume_ok_t    s_ro;
static m_manifest_req_t s_mr;

static void init_samples(void)
{
    for (int i = 0; i < 16; i++)
        s_tb.session_id[i] = (uint8_t)(0xA0 + i);
    s_tb.file_count = 1668;
    s_tb.total_bytes = 0x0000000123456789ULL;

    s_fh.file_index = 57;
    s_fh.file_size = 23114;
    s_fh.start_offset = 4096;
    strcpy(s_fh.name, "OIP-x7Qm.jpeg");

    s_te.file_count = 1668;
    s_te.total_bytes = 0x0000000123456789ULL;

    memcpy(s_rs.session_id, s_tb.session_id, 16);
    s_rs.file_index = 57;
    s_rs.offset = 4096;
    strcpy(s_rs.topic, "gatto");
    strcpy(s_rs.name, "OIP-x7Qm.jpeg");

    s_ro.file_index = 57;
    s_ro.offset = 4096;

    strcpy(s_mr.topic, "cane");
    s_mr.connections = 4;
}

enum { E_GET, E_TB, E_FH, E_FE, E_TE, E_RESUME, E_FETCH, E_RO, E_RJ, E_MR, E_ERR, E_COUNT };
static const uint8_t enc_types[E_COUNT] = {
    MSG_GET, MSG_TOPIC_BEGIN, MSG_FILE_HDR, MSG_FILE_END, MSG_TOPIC_END, MSG_RESUME,
    MSG_FETCH, MSG_RESUME_OK, MSG_RESUME_REJECT, MSG_MANIFEST_REQ, MSG_ERROR
};

/* Encode sample `which` into buf with capacity cap. */
static size_t enc_any(int which, uint8_t *buf, size_t cap)
{
    switch (which) {
    case E_GET:    return enc_get(buf, cap, "gatto");
    case E_TB:     return enc_topic_begin(buf, cap, &s_tb);
    case E_FH:     return enc_file_hdr(buf, cap, &s_fh);
    case E_FE:     return enc_file_end(buf, cap, 57);
    case E_TE:     return enc_topic_end(buf, cap, &s_te);
    case E_RESUME: return enc_resume(buf, cap, MSG_RESUME, &s_rs);
    case E_FETCH:  return enc_resume(buf, cap, MSG_FETCH, &s_rs);
    case E_RO:     return enc_resume_ok(buf, cap, &s_ro);
    case E_RJ:     return enc_resume_reject(buf, cap, FDS_REJ_NAME_MISMATCH);
    case E_MR:     return enc_manifest_req(buf, cap, &s_mr);
    case E_ERR:    return enc_error(buf, cap, FDS_ERR_NOT_FOUND, "unknown topic");
    }
    return 0;
}

/* ---- Header ---------------------------------------------------------------- */

static void test_header_layout(void)
{
    uint8_t h[FDS_HDR_LEN];
    msg_hdr_t mh;
    static const uint8_t exp1[FDS_HDR_LEN] = {
        0x54, 0x46,                     /* magic "TF" */
        0x01,                           /* version */
        0x11,                           /* FILE_HDR */
        0x00, 0x00, 0x00, 0x00,         /* reserved */
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08
    };
    static const uint8_t exp2[FDS_HDR_LEN] = {
        0x54, 0x46, 0x01, 0x16, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x0B, 0x0C
    };

    hdr_encode(h, MSG_FILE_HDR, 0x0102030405060708ULL);
    CHECK(memcmp(h, exp1, FDS_HDR_LEN) == 0);
    CHECK_EQ(hdr_decode(h, &mh), -1);             /* far over the 64 KB cap */

    hdr_encode(h, MSG_MANIFEST, 0x0A0B0CULL);
    CHECK(memcmp(h, exp2, FDS_HDR_LEN) == 0);
    memset(&mh, 0, sizeof(mh));
    CHECK_EQ(hdr_decode(h, &mh), 0);
    CHECK_EQ(mh.type, MSG_MANIFEST);
    CHECK_EQ(mh.payload_len, 0x0A0B0C);

    /* Non-zero reserved bits are future flags: ignored. */
    h[4] = 0xFF; h[7] = 0x01;
    CHECK_EQ(hdr_decode(h, &mh), 0);

    /* Bad magic, bad version. */
    hdr_encode(h, MSG_GET, 9);
    h[0] = 0x46; h[1] = 0x54;                     /* byte-swapped magic */
    CHECK_EQ(hdr_decode(h, &mh), -1);
    hdr_encode(h, MSG_GET, 9);
    h[1] = 0x47;
    CHECK_EQ(hdr_decode(h, &mh), -1);
    hdr_encode(h, MSG_GET, 9);
    h[2] = 0;
    CHECK_EQ(hdr_decode(h, &mh), -1);
    h[2] = 2;
    CHECK_EQ(hdr_decode(h, &mh), -1);

    /* Exactly the 12 message types are valid. */
    {
        int accepted = 0;

        for (int t = 0; t < 256; t++) {
            hdr_encode(h, (uint8_t)t, 0);
            if (hdr_decode(h, &mh) == 0)
                accepted++;
            CHECK_EQ(max_payload_for_type((uint8_t)t) != 0, hdr_decode(h, &mh) == 0);
        }
        CHECK_EQ(accepted, 12);
    }
    hdr_encode(h, 0x05, 0);
    CHECK_EQ(hdr_decode(h, &mh), -1);
    hdr_encode(h, 0x00, 0);
    CHECK_EQ(hdr_decode(h, &mh), -1);
    hdr_encode(h, 0xFF, 0);
    CHECK_EQ(hdr_decode(h, &mh), -1);
}

static void test_payload_caps(void)
{
    static const uint8_t types[] = {
        MSG_GET, MSG_RESUME, MSG_MANIFEST_REQ, MSG_FETCH, MSG_TOPIC_BEGIN, MSG_FILE_HDR,
        MSG_FILE_END, MSG_TOPIC_END, MSG_RESUME_OK, MSG_RESUME_REJECT, MSG_MANIFEST, MSG_ERROR
    };
    uint8_t h[FDS_HDR_LEN];
    msg_hdr_t mh;

    CHECK_EQ(max_payload_for_type(MSG_MANIFEST), 64ULL * 1024 * 1024);
    CHECK_EQ(max_payload_for_type(MSG_GET), 65536);
    CHECK_EQ(max_payload_for_type(MSG_FILE_HDR), 65536);
    CHECK_EQ(max_payload_for_type(0x42), 0);
    for (size_t i = 0; i < sizeof(types); i++) {
        uint64_t cap = max_payload_for_type(types[i]);

        hdr_encode(h, types[i], cap);              /* exactly at the cap */
        CHECK_EQ(hdr_decode(h, &mh), 0);
        CHECK_EQ(mh.payload_len, cap);
        hdr_encode(h, types[i], cap + 1);          /* one byte over */
        CHECK_EQ(hdr_decode(h, &mh), -1);
        hdr_encode(h, types[i], UINT64_MAX);
        CHECK_EQ(hdr_decode(h, &mh), -1);
    }
}

static void test_names(void)
{
    CHECK_STR(msg_type_name(MSG_GET), "GET");
    CHECK_STR(msg_type_name(MSG_RESUME), "RESUME");
    CHECK_STR(msg_type_name(MSG_MANIFEST_REQ), "MANIFEST_REQ");
    CHECK_STR(msg_type_name(MSG_FETCH), "FETCH");
    CHECK_STR(msg_type_name(MSG_TOPIC_BEGIN), "TOPIC_BEGIN");
    CHECK_STR(msg_type_name(MSG_FILE_HDR), "FILE_HDR");
    CHECK_STR(msg_type_name(MSG_FILE_END), "FILE_END");
    CHECK_STR(msg_type_name(MSG_TOPIC_END), "TOPIC_END");
    CHECK_STR(msg_type_name(MSG_RESUME_OK), "RESUME_OK");
    CHECK_STR(msg_type_name(MSG_RESUME_REJECT), "RESUME_REJECT");
    CHECK_STR(msg_type_name(MSG_MANIFEST), "MANIFEST");
    CHECK_STR(msg_type_name(MSG_ERROR), "ERROR");
    CHECK_STR(msg_type_name(0), "?");
    CHECK_STR(msg_type_name(0x7E), "?");

    CHECK_STR(reject_reason_str(FDS_REJ_UNKNOWN_TOPIC), "unknown topic");
    CHECK_STR(reject_reason_str(FDS_REJ_INDEX_RANGE), "file index out of range");
    CHECK_STR(reject_reason_str(FDS_REJ_NAME_MISMATCH), "file name mismatch");
    CHECK_STR(reject_reason_str(FDS_REJ_OFFSET_TOO_BIG), "offset beyond file size");
    CHECK_STR(reject_reason_str(FDS_REJ_AHEAD_OF_SENT), "checkpoint ahead of data sent");
    CHECK_STR(reject_reason_str(0), "unknown reason");
    CHECK_STR(reject_reason_str(6), "unknown reason");
    CHECK_STR(reject_reason_str(65535), "unknown reason");
}

/* ---- Byte buffers ---------------------------------------------------------- */

static void test_wbuf(void)
{
    uint8_t mem[24];
    wbuf_t w;
    static const uint8_t exp[15] = {
        0x7F, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF,
        0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF
    };

    wb_init(&w, mem, sizeof(mem));
    put_u8(&w, 0x7F);
    put_u16(&w, 0xBEEF);
    put_u32(&w, 0xDEADBEEF);
    put_u64(&w, 0x0123456789ABCDEFULL);
    CHECK_EQ(w.err, 0);
    CHECK_EQ(w.len, 15);
    CHECK(memcmp(mem, exp, sizeof(exp)) == 0);

    /* Overflow: the write that does not fit sets err and writes nothing, and
     * later writes are ignored even when they would fit. Bytes 8.. of mem are
     * a guard zone that must stay untouched. */
    memset(mem, 0xAA, sizeof(mem));
    wb_init(&w, mem, 8);
    put_u32(&w, 1);
    put_u64(&w, 2);
    CHECK_EQ(w.err, 1);
    CHECK_EQ(w.len, 4);
    put_u8(&w, 3);
    put_u16(&w, 3);
    put_bytes(&w, "ab", 2);
    CHECK_EQ(w.len, 4);
    for (size_t i = 4; i < sizeof(mem); i++)
        CHECK_EQ(mem[i], 0xAA);

    /* A size that would wrap len + n around must not pass the check. */
    wb_init(&w, mem, 8);
    put_u8(&w, 1);
    put_bytes(&w, mem, SIZE_MAX);
    CHECK_EQ(w.err, 1);
    CHECK_EQ(w.len, 1);

    /* Zero capacity and zero-length writes. */
    wb_init(&w, NULL, 0);
    put_bytes(&w, NULL, 0);
    CHECK_EQ(w.err, 0);
    put_u8(&w, 1);
    CHECK_EQ(w.err, 1);
}

static void test_rbuf(void)
{
    static const uint8_t in[] = {
        0x7F, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF,
        0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF
    };
    rbuf_t r;
    uint8_t dst[8];
    char s[8];

    rb_init(&r, in, sizeof(in));
    CHECK_EQ(get_u8(&r), 0x7F);
    CHECK_EQ(get_u16(&r), 0xBEEF);
    CHECK_EQ(get_u32(&r), 0xDEADBEEFU);
    CHECK(get_u64(&r) == 0x0123456789ABCDEFULL);
    CHECK_EQ(r.err, 0);
    CHECK_EQ(r.pos, sizeof(in));

    /* Underflow returns 0, leaves dst untouched and is sticky. */
    rb_init(&r, in, 3);
    CHECK_EQ(get_u32(&r), 0);
    CHECK_EQ(r.err, 1);
    CHECK_EQ(get_u8(&r), 0);                      /* would fit, but err is set */
    memset(dst, 0x55, sizeof(dst));
    get_bytes(&r, dst, 1);
    CHECK_EQ(dst[0], 0x55);
    rb_init(&r, in, 3);
    get_bytes(&r, dst, 4);
    CHECK_EQ(r.err, 1);
    CHECK_EQ(dst[0], 0x55);
    rb_init(&r, in, 3);
    get_bytes(&r, dst, SIZE_MAX);                 /* no wrap-around */
    CHECK_EQ(r.err, 1);
    rb_init(&r, NULL, 0);
    CHECK_EQ(get_u16(&r), 0);
    CHECK_EQ(r.err, 1);

    /* get_str: normal, n == cap - 1, n >= cap, embedded NUL, underflow. */
    rb_init(&r, (const uint8_t *)"hello", 5);
    get_str(&r, s, 5, sizeof(s));
    CHECK_EQ(r.err, 0);
    CHECK_STR(s, "hello");
    rb_init(&r, (const uint8_t *)"1234567", 7);
    get_str(&r, s, 7, sizeof(s));
    CHECK_EQ(r.err, 0);
    CHECK_STR(s, "1234567");
    strcpy(s, "keep");
    rb_init(&r, (const uint8_t *)"12345678", 8);
    get_str(&r, s, 8, sizeof(s));
    CHECK_EQ(r.err, 1);
    CHECK_STR(s, "keep");                         /* skipped: dst untouched */
    rb_init(&r, (const uint8_t *)"ab\0cd", 5);
    get_str(&r, s, 5, sizeof(s));
    CHECK_EQ(r.err, 1);
    strcpy(s, "keep");
    rb_init(&r, (const uint8_t *)"abc", 3);
    get_str(&r, s, 4, sizeof(s));
    CHECK_EQ(r.err, 1);
    CHECK_STR(s, "keep");
    rb_init(&r, (const uint8_t *)"", 0);
    get_str(&r, s, 0, sizeof(s));                 /* empty string is fine here */
    CHECK_EQ(r.err, 0);
    CHECK_STR(s, "");
    get_str(&r, s, 0, 0);                         /* but never with cap 0 */
    CHECK_EQ(r.err, 1);
}

/* ---- Exact byte layouts ----------------------------------------------------- */

static void test_file_hdr_bytes(void)
{
    m_file_hdr_t m;
    uint8_t out[128];
    size_t n;
    static const uint8_t exp[] = {
        0x54, 0x46, 0x01, 0x11, 0x00, 0x00, 0x00, 0x00,          /* header */
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1B,          /* payload_len 27 */
        0x01, 0x02, 0x03, 0x04,                                  /* file_index */
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,          /* file_size */
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x05,          /* start_offset */
        0x00, 0x05,                                              /* name_len */
        'a', '.', 'j', 'p', 'g'
    };

    memset(&m, 0, sizeof(m));
    m.file_index = 0x01020304;
    m.file_size = 0x1122334455667788ULL;
    m.start_offset = 0x1005;
    strcpy(m.name, "a.jpg");
    n = enc_file_hdr(out, sizeof(out), &m);
    CHECK_EQ(n, sizeof(exp));
    CHECK(memcmp(out, exp, sizeof(exp)) == 0);
}

static void test_get_bytes(void)
{
    uint8_t out[64];
    static const uint8_t exp[] = {
        0x54, 0x46, 0x01, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9,
        'G', 'E', 'T', ' ', 'g', 'a', 't', 't', 'o'
    };

    CHECK_EQ(enc_get(out, sizeof(out), "gatto"), sizeof(exp));
    CHECK(memcmp(out, exp, sizeof(exp)) == 0);
}

/* ---- Round trips ------------------------------------------------------------ */

static void test_round_trips(void)
{
    uint8_t b[1024];
    size_t n;

    {   /* GET */
        char t[FDS_TOPIC_MAX + 1];

        n = enc_get(b, sizeof(b), "gatto");
        check_strict(b, n, MSG_GET, 5);
        CHECK_EQ(dec_get(b + FDS_HDR_LEN, n - FDS_HDR_LEN, t), 0);
        CHECK_STR(t, "gatto");
        n = enc_get(b, sizeof(b), "with space");
        CHECK_EQ(dec_get(b + FDS_HDR_LEN, n - FDS_HDR_LEN, t), 0);
        CHECK_STR(t, "with space");
    }
    {   /* TOPIC_BEGIN */
        m_topic_begin_t m;

        n = enc_topic_begin(b, sizeof(b), &s_tb);
        CHECK_EQ(n, FDS_HDR_LEN + 28);
        check_strict(b, n, MSG_TOPIC_BEGIN, SIZE_MAX);
        memset(&m, 0, sizeof(m));
        CHECK_EQ(dec_topic_begin(b + FDS_HDR_LEN, n - FDS_HDR_LEN, &m), 0);
        CHECK(memcmp(m.session_id, s_tb.session_id, 16) == 0);
        CHECK_EQ(m.file_count, 1668);
        CHECK(m.total_bytes == s_tb.total_bytes);
    }
    {   /* FILE_HDR */
        m_file_hdr_t m;

        n = enc_file_hdr(b, sizeof(b), &s_fh);
        CHECK_EQ(n, FDS_HDR_LEN + 22 + strlen(s_fh.name));
        check_strict(b, n, MSG_FILE_HDR, SIZE_MAX);
        memset(&m, 0xEE, sizeof(m));
        CHECK_EQ(dec_file_hdr(b + FDS_HDR_LEN, n - FDS_HDR_LEN, &m), 0);
        CHECK_EQ(m.file_index, 57);
        CHECK_EQ(m.file_size, 23114);
        CHECK_EQ(m.start_offset, 4096);
        CHECK_STR(m.name, "OIP-x7Qm.jpeg");
    }
    {   /* FILE_END */
        m_file_end_t m;

        n = enc_file_end(b, sizeof(b), 0xFFFFFFFFU);
        CHECK_EQ(n, FDS_HDR_LEN + 4);
        check_strict(b, n, MSG_FILE_END, SIZE_MAX);
        CHECK_EQ(dec_file_end(b + FDS_HDR_LEN, 4, &m), 0);
        CHECK(m.file_index == 0xFFFFFFFFU);
    }
    {   /* TOPIC_END */
        m_topic_end_t m;

        n = enc_topic_end(b, sizeof(b), &s_te);
        CHECK_EQ(n, FDS_HDR_LEN + 12);
        check_strict(b, n, MSG_TOPIC_END, SIZE_MAX);
        CHECK_EQ(dec_topic_end(b + FDS_HDR_LEN, 12, &m), 0);
        CHECK_EQ(m.file_count, 1668);
        CHECK(m.total_bytes == s_te.total_bytes);
    }
    {   /* RESUME and FETCH: same layout, different type */
        m_resume_t m;
        msg_hdr_t h;
        static const uint8_t types[2] = { MSG_RESUME, MSG_FETCH };

        for (int i = 0; i < 2; i++) {
            n = enc_resume(b, sizeof(b), types[i], &s_rs);
            CHECK_EQ(n, FDS_HDR_LEN + 16 + 4 + 8 + 1 + 5 + 2 + 13);
            check_strict(b, n, types[i], SIZE_MAX);
            CHECK_EQ(hdr_decode(b, &h), 0);
            CHECK_EQ(h.type, types[i]);
            memset(&m, 0xEE, sizeof(m));
            CHECK_EQ(dec_resume(b + FDS_HDR_LEN, n - FDS_HDR_LEN, &m), 0);
            CHECK(memcmp(m.session_id, s_rs.session_id, 16) == 0);
            CHECK_EQ(m.file_index, 57);
            CHECK_EQ(m.offset, 4096);
            CHECK_STR(m.topic, "gatto");
            CHECK_STR(m.name, "OIP-x7Qm.jpeg");
        }
        CHECK_EQ(enc_resume(b, sizeof(b), MSG_GET, &s_rs), 0);
        CHECK_EQ(enc_resume(b, sizeof(b), MSG_RESUME_OK, &s_rs), 0);
        CHECK_EQ(enc_resume(b, sizeof(b), MSG_RESUME, NULL), 0);
    }
    {   /* RESUME_OK */
        m_resume_ok_t m;

        n = enc_resume_ok(b, sizeof(b), &s_ro);
        CHECK_EQ(n, FDS_HDR_LEN + 12);
        check_strict(b, n, MSG_RESUME_OK, SIZE_MAX);
        CHECK_EQ(dec_resume_ok(b + FDS_HDR_LEN, 12, &m), 0);
        CHECK_EQ(m.file_index, 57);
        CHECK_EQ(m.offset, 4096);
    }
    {   /* RESUME_REJECT */
        uint16_t reason = 0;

        n = enc_resume_reject(b, sizeof(b), FDS_REJ_AHEAD_OF_SENT);
        CHECK_EQ(n, FDS_HDR_LEN + 2);
        check_strict(b, n, MSG_RESUME_REJECT, SIZE_MAX);
        CHECK_EQ(dec_resume_reject(b + FDS_HDR_LEN, 2, &reason), 0);
        CHECK_EQ(reason, FDS_REJ_AHEAD_OF_SENT);
    }
    {   /* MANIFEST_REQ */
        m_manifest_req_t m;

        n = enc_manifest_req(b, sizeof(b), &s_mr);
        CHECK_EQ(n, FDS_HDR_LEN + 1 + 4 + 1);
        check_strict(b, n, MSG_MANIFEST_REQ, SIZE_MAX);
        memset(&m, 0xEE, sizeof(m));
        CHECK_EQ(dec_manifest_req(b + FDS_HDR_LEN, n - FDS_HDR_LEN, &m), 0);
        CHECK_STR(m.topic, "cane");
        CHECK_EQ(m.connections, 4);
    }
    {   /* ERROR */
        m_error_t m;

        n = enc_error(b, sizeof(b), FDS_ERR_NOT_FOUND, "unknown topic");
        CHECK_EQ(n, FDS_HDR_LEN + 4 + 13);
        check_strict(b, n, MSG_ERROR, SIZE_MAX);
        CHECK_EQ(dec_error(b + FDS_HDR_LEN, n - FDS_HDR_LEN, &m), 0);
        CHECK_EQ(m.code, 404);
        CHECK_STR(m.msg, "unknown topic");
        n = enc_error(b, sizeof(b), FDS_ERR_BUSY, NULL);  /* no text */
        CHECK_EQ(n, FDS_HDR_LEN + 4);
        CHECK_EQ(dec_error(b + FDS_HDR_LEN, 4, &m), 0);
        CHECK_EQ(m.code, 503);
        CHECK_STR(m.msg, "");
    }
    {   /* MANIFEST */
        file_entry_t f[3];
        m_manifest_t m;
        uint8_t *mb;
        size_t len = 99;

        memset(f, 0, sizeof(f));
        strcpy(f[0].name, "1.jpeg");
        f[0].size = 23114;
        strcpy(f[1].name, "empty file");
        f[1].size = 0;
        strcpy(f[2].name, "big.bin");
        f[2].size = 5ULL << 30;
        mb = enc_manifest_alloc(s_tb.session_id, f, 3, 23114 + (5ULL << 30), &len);
        CHECK(mb != NULL);
        CHECK_EQ(len, FDS_HDR_LEN + 28 + (10 + 6) + (10 + 10) + (10 + 7));
        check_strict(mb, len, MSG_MANIFEST, SIZE_MAX);
        CHECK_EQ(dec_manifest(mb + FDS_HDR_LEN, len - FDS_HDR_LEN, &m), 0);
        CHECK(memcmp(m.session_id, s_tb.session_id, 16) == 0);
        CHECK_EQ(m.file_count, 3);
        CHECK(m.total_bytes == 23114 + (5ULL << 30));
        CHECK(m.files != NULL);
        for (int i = 0; i < 3; i++) {
            CHECK_STR(m.files[i].name, f[i].name);
            CHECK(m.files[i].size == f[i].size);
        }
        manifest_free(&m);
        CHECK(m.files == NULL);
        manifest_free(&m);                        /* second call is harmless */
        free(mb);

        /* Empty topic: valid, and still a non-NULL array on success. */
        mb = enc_manifest_alloc(s_tb.session_id, NULL, 0, 0, &len);
        CHECK(mb != NULL);
        CHECK_EQ(len, FDS_HDR_LEN + 28);
        CHECK_EQ(dec_manifest(mb + FDS_HDR_LEN, 28, &m), 0);
        CHECK_EQ(m.file_count, 0);
        CHECK(m.files != NULL);
        manifest_free(&m);
        free(mb);
    }
}

/* Every encoder: 0 for every capacity below the message size, never writing
 * at or beyond cap; exactly the size when cap is just enough. */
static void test_small_caps(void)
{
    uint8_t b[512];

    for (int which = 0; which < E_COUNT; which++) {
        size_t n = enc_any(which, b, sizeof(b));
        msg_hdr_t h;

        CHECK(n > FDS_HDR_LEN);
        CHECK_EQ(hdr_decode(b, &h), 0);
        CHECK_EQ(h.type, enc_types[which]);
        for (size_t cap = 0; cap < n; cap++) {
            int clean = 1;

            memset(b, 0xAA, sizeof(b));
            CHECK_EQ(enc_any(which, b, cap), 0);
            for (size_t i = cap; i < sizeof(b); i++)
                if (b[i] != 0xAA)
                    clean = 0;
            CHECK(clean);
        }
        CHECK_EQ(enc_any(which, b, n), n);
    }
    CHECK_EQ(enc_get(NULL, 0, "gatto"), 0);
}

/* ---- Boundaries ------------------------------------------------------------- */

/* Build a RESUME payload by hand (bypassing enc_resume's validation). */
static size_t raw_resume(uint8_t *out, size_t cap, size_t topic_len, const char *topic,
                         size_t name_len, const char *name)
{
    wbuf_t w;

    wb_init(&w, out, cap);
    put_bytes(&w, s_rs.session_id, 16);
    put_u32(&w, 1);
    put_u64(&w, 2);
    put_u8(&w, (uint8_t)topic_len);
    put_bytes(&w, topic, topic_len);
    put_u16(&w, (uint16_t)name_len);
    put_bytes(&w, name, name_len);
    return w.err ? 0 : w.len;
}

/* Build a FILE_HDR payload by hand. */
static size_t raw_file_hdr(uint8_t *out, size_t cap, uint64_t size, uint64_t off,
                           size_t name_len, const char *name)
{
    wbuf_t w;

    wb_init(&w, out, cap);
    put_u32(&w, 3);
    put_u64(&w, size);
    put_u64(&w, off);
    put_u16(&w, (uint16_t)name_len);
    put_bytes(&w, name, name_len);
    return w.err ? 0 : w.len;
}

static void test_topic_boundaries(void)
{
    char t63[64], t64[65], back[FDS_TOPIC_MAX + 1];
    uint8_t b[1024], p[512];
    size_t n;
    m_resume_t r;
    m_manifest_req_t mr;

    fill(t63, 't', 63);
    fill(t64, 't', 64);

    /* GET */
    n = enc_get(b, sizeof(b), t63);
    CHECK_EQ(n, FDS_HDR_LEN + 4 + 63);
    CHECK_EQ(dec_get(b + FDS_HDR_LEN, n - FDS_HDR_LEN, back), 0);
    CHECK_STR(back, t63);
    CHECK_EQ(enc_get(b, sizeof(b), t64), 0);
    memcpy(p, "GET ", 4);
    memcpy(p + 4, t64, 64);
    CHECK_EQ(dec_get(p, 68, back), -1);

    /* RESUME: 63 is fine; a 64-byte topic does not fit m.topic with its NUL,
     * so the field is left unterminated and must be refused, not over-read. */
    r = s_rs;
    strcpy(r.topic, t63);
    n = enc_resume(b, sizeof(b), MSG_RESUME, &r);
    CHECK(n > 0);
    CHECK_EQ(dec_resume(b + FDS_HDR_LEN, n - FDS_HDR_LEN, &r), 0);
    CHECK_STR(r.topic, t63);
    r = s_rs;
    memset(r.topic, 't', sizeof(r.topic));
    CHECK_EQ(enc_resume(b, sizeof(b), MSG_RESUME, &r), 0);
    n = raw_resume(p, sizeof(p), 63, t63, 5, "a.jpg");
    CHECK_EQ(dec_resume(p, n, &r), 0);
    n = raw_resume(p, sizeof(p), 64, t64, 5, "a.jpg");
    CHECK_EQ(dec_resume(p, n, &r), -1);
    n = raw_resume(p, sizeof(p), 0, "", 5, "a.jpg");      /* empty topic */
    CHECK_EQ(dec_resume(p, n, &r), -1);
    n = raw_resume(p, sizeof(p), 3, "a/b", 5, "a.jpg");
    CHECK_EQ(dec_resume(p, n, &r), -1);
    n = raw_resume(p, sizeof(p), 2, "..", 5, "a.jpg");
    CHECK_EQ(dec_resume(p, n, &r), -1);

    /* MANIFEST_REQ */
    memset(&mr, 0, sizeof(mr));
    strcpy(mr.topic, t63);
    mr.connections = 16;
    n = enc_manifest_req(b, sizeof(b), &mr);
    CHECK_EQ(n, FDS_HDR_LEN + 1 + 63 + 1);
    CHECK_EQ(dec_manifest_req(b + FDS_HDR_LEN, n - FDS_HDR_LEN, &mr), 0);
    CHECK_STR(mr.topic, t63);
    CHECK_EQ(mr.connections, 16);
    memset(mr.topic, 't', sizeof(mr.topic));
    CHECK_EQ(enc_manifest_req(b, sizeof(b), &mr), 0);
    p[0] = 64;
    memcpy(p + 1, t64, 64);
    p[65] = 4;
    CHECK_EQ(dec_manifest_req(p, 66, &mr), -1);
    strcpy(mr.topic, "a/b");
    CHECK_EQ(enc_manifest_req(b, sizeof(b), &mr), 0);
}

static void test_name_boundaries(void)
{
    char n255[256], n256[257];
    uint8_t b[1024], p[1024];
    size_t n;
    m_file_hdr_t fh;
    m_resume_t r;

    fill(n255, 'n', 255);
    fill(n256, 'n', 256);

    /* FILE_HDR */
    memset(&fh, 0, sizeof(fh));
    fh.file_size = 10;
    strcpy(fh.name, n255);
    n = enc_file_hdr(b, sizeof(b), &fh);
    CHECK_EQ(n, FDS_HDR_LEN + 22 + 255);
    CHECK_EQ(dec_file_hdr(b + FDS_HDR_LEN, n - FDS_HDR_LEN, &fh), 0);
    CHECK_STR(fh.name, n255);
    memset(fh.name, 'n', sizeof(fh.name));         /* 256 bytes, no NUL */
    CHECK_EQ(enc_file_hdr(b, sizeof(b), &fh), 0);
    n = raw_file_hdr(p, sizeof(p), 10, 0, 255, n255);
    CHECK_EQ(dec_file_hdr(p, n, &fh), 0);
    n = raw_file_hdr(p, sizeof(p), 10, 0, 256, n256);
    CHECK_EQ(dec_file_hdr(p, n, &fh), -1);
    n = raw_file_hdr(p, sizeof(p), 10, 0, 0, "");          /* empty name */
    CHECK_EQ(dec_file_hdr(p, n, &fh), -1);

    /* RESUME */
    r = s_rs;
    strcpy(r.name, n255);
    n = enc_resume(b, sizeof(b), MSG_FETCH, &r);
    CHECK(n > 0);
    CHECK_EQ(dec_resume(b + FDS_HDR_LEN, n - FDS_HDR_LEN, &r), 0);
    CHECK_STR(r.name, n255);
    memset(r.name, 'n', sizeof(r.name));
    CHECK_EQ(enc_resume(b, sizeof(b), MSG_FETCH, &r), 0);
    n = raw_resume(p, sizeof(p), 5, "gatto", 256, n256);
    CHECK_EQ(dec_resume(p, n, &r), -1);
    n = raw_resume(p, sizeof(p), 5, "gatto", 0, "");
    CHECK_EQ(dec_resume(p, n, &r), -1);
}

static void test_unsafe_names(void)
{
    static const char *bad[] = {
        "../x", "..", ".", ".hidden", "a/b", "/etc/passwd", "x/", "bad\x01name",
        "tab\tname", "new\nline", "del\x7f", "\x1b[31mred"
    };
    uint8_t b[512], p[512];
    m_file_hdr_t fh;
    m_resume_t r;
    file_entry_t fe;
    size_t n, len = 7;

    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        size_t nl = strlen(bad[i]);

        memset(&fh, 0, sizeof(fh));
        fh.file_size = 1;
        strcpy(fh.name, bad[i]);
        CHECK_EQ(enc_file_hdr(b, sizeof(b), &fh), 0);
        n = raw_file_hdr(p, sizeof(p), 1, 0, nl, bad[i]);
        CHECK_EQ(dec_file_hdr(p, n, &fh), -1);

        r = s_rs;
        strcpy(r.name, bad[i]);
        CHECK_EQ(enc_resume(b, sizeof(b), MSG_RESUME, &r), 0);
        n = raw_resume(p, sizeof(p), 5, "gatto", nl, bad[i]);
        CHECK_EQ(dec_resume(p, n, &r), -1);

        memset(&fe, 0, sizeof(fe));
        strcpy(fe.name, bad[i]);
        fe.size = 1;
        CHECK(enc_manifest_alloc(s_tb.session_id, &fe, 1, 1, &len) == NULL);
        CHECK_EQ(len, 0);
    }
    /* A NUL inside the declared name length: "ab\0c" must not become "ab". */
    n = raw_file_hdr(p, sizeof(p), 1, 0, 4, "ab\0c");
    CHECK_EQ(dec_file_hdr(p, n, &fh), -1);
    n = raw_resume(p, sizeof(p), 5, "ga\0to", 5, "a.jpg");
    CHECK_EQ(dec_resume(p, n, &r), -1);
    /* Names with spaces, dashes and dots inside are fine. */
    n = raw_file_hdr(p, sizeof(p), 1, 0, 14, "a b-c.d.e.jpeg");
    CHECK_EQ(dec_file_hdr(p, n, &fh), 0);
}

static void test_offsets(void)
{
    m_file_hdr_t fh;
    uint8_t b[512], p[512];
    size_t n;

    memset(&fh, 0, sizeof(fh));
    strcpy(fh.name, "x.jpg");
    fh.file_size = 100;
    fh.start_offset = 100;                         /* == size: nothing left, valid */
    n = enc_file_hdr(b, sizeof(b), &fh);
    CHECK(n > 0);
    CHECK_EQ(dec_file_hdr(b + FDS_HDR_LEN, n - FDS_HDR_LEN, &fh), 0);
    fh.start_offset = 101;
    CHECK_EQ(enc_file_hdr(b, sizeof(b), &fh), 0);
    n = raw_file_hdr(p, sizeof(p), 100, 101, 5, "x.jpg");
    CHECK_EQ(dec_file_hdr(p, n, &fh), -1);
    n = raw_file_hdr(p, sizeof(p), 0, UINT64_MAX, 5, "x.jpg");
    CHECK_EQ(dec_file_hdr(p, n, &fh), -1);
    n = raw_file_hdr(p, sizeof(p), 0, 0, 5, "x.jpg");      /* empty file */
    CHECK_EQ(dec_file_hdr(p, n, &fh), 0);
    CHECK(enc_file_hdr(b, sizeof(b), NULL) == 0);
}

static void test_get_malformed(void)
{
    static const char *bad[] = {
        "", "G", "GE", "GET", "GET ", "get x", "Get x", "GETx", "GET\tx", "GET a/b",
        "GET ..", "GET .", "GET x\n", "GET x\r\n", " GET x", "POST x"
    };
    char t[FDS_TOPIC_MAX + 1];

    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        strcpy(t, "junk");
        CHECK_EQ(dec_get((const uint8_t *)bad[i], strlen(bad[i]), t), -1);
        CHECK_STR(t, "");                          /* never left half-filled */
    }
    CHECK_EQ(dec_get((const uint8_t *)"GET x\0y", 7, t), -1);
    CHECK_EQ(dec_get((const uint8_t *)"GET x", 5, t), 0);
    CHECK_STR(t, "x");
    CHECK_EQ(dec_get(NULL, 0, t), -1);
    {
        uint8_t b[128];
        CHECK_EQ(enc_get(b, sizeof(b), ""), 0);
        CHECK_EQ(enc_get(b, sizeof(b), "a/b"), 0);
        CHECK_EQ(enc_get(b, sizeof(b), ".."), 0);
        CHECK_EQ(enc_get(b, sizeof(b), "x\n"), 0);
        CHECK_EQ(enc_get(b, sizeof(b), NULL), 0);
    }
}

static void test_error_msgs(void)
{
    char long_msg[301];
    uint8_t b[1024], p[512];
    size_t n;
    m_error_t m;
    wbuf_t w;

    /* Encoder truncates the text to 255 bytes. */
    fill(long_msg, 'e', 300);
    n = enc_error(b, sizeof(b), 500, long_msg);
    CHECK_EQ(n, FDS_HDR_LEN + 4 + 255);
    CHECK_EQ(dec_error(b + FDS_HDR_LEN, n - FDS_HDR_LEN, &m), 0);
    CHECK_EQ(strlen(m.msg), 255);
    CHECK_EQ(m.code, 500);

    /* Decoder: msg_len 300 with exactly 300 bytes is accepted and truncated;
     * one byte short or one too many is malformed. */
    wb_init(&w, p, sizeof(p));
    put_u16(&w, 400);
    put_u16(&w, 300);
    put_bytes(&w, long_msg, 300);
    CHECK_EQ(w.err, 0);
    CHECK_EQ(dec_error(p, w.len, &m), 0);
    CHECK_EQ(m.code, 400);
    CHECK_EQ(strlen(m.msg), 255);
    CHECK(m.msg[0] == 'e' && m.msg[254] == 'e');
    CHECK_EQ(dec_error(p, w.len - 1, &m), -1);
    p[w.len] = 'x';
    CHECK_EQ(dec_error(p, w.len + 1, &m), -1);

    /* Control characters (and a NUL) are neutralised for printing. */
    wb_init(&w, p, sizeof(p));
    put_u16(&w, 404);
    put_u16(&w, 6);
    put_bytes(&w, "a\x1b[\nb\0", 6);
    CHECK_EQ(dec_error(p, w.len, &m), 0);
    CHECK_STR(m.msg, "a?[?b?");

    /* Truncated fixed part. */
    CHECK_EQ(dec_error(p, 3, &m), -1);
    CHECK_EQ(dec_error(NULL, 0, &m), -1);
}

static void test_manifest_large(void)
{
    enum { N = 5000 };
    file_entry_t *f = calloc(N, sizeof(*f));
    m_manifest_t m;
    msg_hdr_t h;
    uint64_t total = 0;
    uint8_t *mb;
    size_t len = 0;
    int same = 1;

    for (int i = 0; i < N; i++) {
        snprintf(f[i].name, sizeof(f[i].name), "file_%05d.jpeg", i);
        f[i].size = (uint64_t)i * 37 + 1;
        total += f[i].size;
    }
    mb = enc_manifest_alloc(s_tb.session_id, f, N, total, &len);
    CHECK(mb != NULL);
    CHECK_EQ(len, FDS_HDR_LEN + 28 + (size_t)N * (10 + 15));
    CHECK(len - FDS_HDR_LEN > FDS_MAX_CTRL_PAYLOAD);         /* over 64 KB */
    CHECK_EQ(hdr_decode(mb, &h), 0);
    CHECK_EQ(h.type, MSG_MANIFEST);
    CHECK_EQ(h.payload_len, len - FDS_HDR_LEN);
    CHECK_EQ(dec_manifest(mb + FDS_HDR_LEN, len - FDS_HDR_LEN, &m), 0);
    CHECK_EQ(m.file_count, N);
    CHECK(m.total_bytes == total);
    for (int i = 0; i < N; i++)
        if (strcmp(m.files[i].name, f[i].name) != 0 || m.files[i].size != f[i].size)
            same = 0;
    CHECK(same);
    manifest_free(&m);
    /* The other message types stay at the 64 KB cap. */
    hdr_encode(mb, MSG_FILE_HDR, len - FDS_HDR_LEN);
    CHECK_EQ(hdr_decode(mb, &h), -1);
    free(mb);
    free(f);
}

/* A MANIFEST payload of exactly FDS_MAX_MANIFEST_PAYLOAD is produced and
 * accepted; one more byte and enc_manifest_alloc refuses it. */
static void test_manifest_cap(void)
{
    /* 255-byte names: each entry is 8 + 2 + 255 = 265 bytes. With 253 240 of
     * them the payload is 28 + 67 108 600 bytes, 236 short of 64 MiB: one
     * more entry with a 226-byte name (236 bytes) hits the cap exactly. */
    const uint32_t full = 253240, n = full + 1;
    file_entry_t *f = calloc(n, sizeof(*f));
    m_manifest_t m;
    msg_hdr_t h;
    uint8_t *mb;
    size_t len = 0;

    for (uint32_t i = 0; i < full; i++) {
        fill(f[i].name, 'a', 255);
        f[i].size = 1;
    }
    fill(f[full].name, 'b', 226);
    f[full].size = 1;
    mb = enc_manifest_alloc(s_tb.session_id, f, n, n, &len);
    CHECK(mb != NULL);
    CHECK_EQ(len, FDS_HDR_LEN + (size_t)FDS_MAX_MANIFEST_PAYLOAD);
    if (mb != NULL) {
        CHECK_EQ(hdr_decode(mb, &h), 0);
        CHECK_EQ(h.payload_len, FDS_MAX_MANIFEST_PAYLOAD);
        CHECK_EQ(dec_manifest(mb + FDS_HDR_LEN, len - FDS_HDR_LEN, &m), 0);
        CHECK_EQ(m.file_count, n);
        manifest_free(&m);
        free(mb);
    }
    fill(f[full].name, 'b', 227);                  /* cap + 1 */
    errno = 0;
    CHECK(enc_manifest_alloc(s_tb.session_id, f, n, n, &len) == NULL);
    CHECK_EQ(errno, EMSGSIZE);
    CHECK_EQ(len, 0);
    free(f);
}

/* Build a MANIFEST payload by hand: count as given, entries as given. */
static size_t raw_manifest(uint8_t *out, size_t cap, uint32_t count, uint64_t total,
                           size_t n_entries, const uint64_t *sizes, const char *const *names)
{
    wbuf_t w;

    wb_init(&w, out, cap);
    put_bytes(&w, s_tb.session_id, 16);
    put_u32(&w, count);
    put_u64(&w, total);
    for (size_t i = 0; i < n_entries; i++) {
        put_u64(&w, sizes[i]);
        put_u16(&w, (uint16_t)strlen(names[i]));
        put_bytes(&w, names[i], strlen(names[i]));
    }
    return w.err ? 0 : w.len;
}

static void test_manifest_malformed(void)
{
    static const char *const names[] = { "a.jpg", "b.jpg", "c.jpg" };
    const uint64_t sizes[] = { 10, 20, 30 };
    static file_entry_t stale;
    uint8_t p[1024];
    m_manifest_t m;
    size_t n;

    n = raw_manifest(p, sizeof(p), 3, 60, 3, sizes, names);
    CHECK_EQ(dec_manifest(p, n, &m), 0);
    manifest_free(&m);

    /* Hostile counts: rejected before any allocation (under ASan a calloc of
     * 4 G entries would abort the test, so passing proves the order). */
    n = raw_manifest(p, sizeof(p), 0xFFFFFFFFU, 0, 0, sizes, names);
    m.files = &stale;                              /* must be reset to NULL */
    CHECK_EQ(dec_manifest(p, n, &m), -1);
    CHECK(m.files == NULL);
    n = raw_manifest(p, sizeof(p), 0xFFFFFFFFU, 60, 3, sizes, names);
    CHECK_EQ(dec_manifest(p, n, &m), -1);
    CHECK(m.files == NULL);
    /* count larger than the entries present, but still "fits" by size: fails
     * while parsing, and the array is freed (leak-checked under ASan). */
    n = raw_manifest(p, sizeof(p), 4, 60, 3, sizes, names);
    CHECK_EQ(dec_manifest(p, n, &m), -1);
    CHECK(m.files == NULL);
    /* count smaller than the entries present: trailing bytes. */
    n = raw_manifest(p, sizeof(p), 2, 30, 3, sizes, names);
    CHECK_EQ(dec_manifest(p, n, &m), -1);
    CHECK(m.files == NULL);
    /* sum of sizes != total_bytes */
    n = raw_manifest(p, sizeof(p), 3, 61, 3, sizes, names);
    CHECK_EQ(dec_manifest(p, n, &m), -1);
    CHECK(m.files == NULL);
    n = raw_manifest(p, sizeof(p), 3, 59, 3, sizes, names);
    CHECK_EQ(dec_manifest(p, n, &m), -1);
    /* sizes that only "add up" by wrapping around 2^64 */
    {
        const uint64_t wrap[] = { UINT64_MAX, 2, 0 };

        n = raw_manifest(p, sizeof(p), 3, 1, 3, wrap, names);
        CHECK_EQ(dec_manifest(p, n, &m), -1);
        CHECK(m.files == NULL);
    }
    /* unsafe name in the middle */
    {
        static const char *const bad[] = { "a.jpg", "../b", "c.jpg" };

        n = raw_manifest(p, sizeof(p), 3, 60, 3, sizes, bad);
        CHECK_EQ(dec_manifest(p, n, &m), -1);
        CHECK(m.files == NULL);
    }
    /* shorter than the fixed part */
    for (size_t k = 0; k < 28; k++) {
        CHECK_EQ(dec_manifest(p, k, &m), -1);
        CHECK(m.files == NULL);
    }
    /* name_len pointing past the end */
    n = raw_manifest(p, sizeof(p), 1, 10, 1, sizes, names);
    p[16 + 4 + 8 + 8 + 1] = 200;                   /* low byte of name_len */
    CHECK_EQ(dec_manifest(p, n, &m), -1);
    CHECK(m.files == NULL);

    /* Encoder input errors. */
    {
        file_entry_t f[2];
        size_t len = 5;

        memset(f, 0, sizeof(f));
        strcpy(f[0].name, "a");
        f[0].size = 1;
        strcpy(f[1].name, "b");
        f[1].size = 2;
        CHECK(enc_manifest_alloc(s_tb.session_id, f, 2, 4, &len) == NULL);   /* sum */
        CHECK_EQ(len, 0);
        CHECK(enc_manifest_alloc(NULL, f, 2, 3, &len) == NULL);
        CHECK(enc_manifest_alloc(s_tb.session_id, NULL, 2, 3, &len) == NULL);
        memset(f[1].name, 'z', sizeof(f[1].name));                          /* no NUL */
        CHECK(enc_manifest_alloc(s_tb.session_id, f, 2, 3, &len) == NULL);
        f[1].size = UINT64_MAX;                                             /* overflow */
        strcpy(f[1].name, "b");
        CHECK(enc_manifest_alloc(s_tb.session_id, f, 2, 0, &len) == NULL);
        f[1].size = 2;
        {
            uint8_t *mb = enc_manifest_alloc(s_tb.session_id, f, 2, 3, NULL);  /* NULL len_out ok */

            CHECK(mb != NULL);
            free(mb);
        }
    }
}

/* Fixed-size messages: every wrong length is rejected (shorter is covered by
 * check_strict; this adds a range of longer payloads and the NULL/0 case). */
static void test_fixed_lengths(void)
{
    static const struct { uint8_t type; size_t len; } fixed[] = {
        { MSG_TOPIC_BEGIN, 28 }, { MSG_FILE_END, 4 }, { MSG_TOPIC_END, 12 },
        { MSG_RESUME_OK, 12 }, { MSG_RESUME_REJECT, 2 }
    };
    uint8_t zeros[64];

    memset(zeros, 0, sizeof(zeros));
    for (size_t i = 0; i < sizeof(fixed) / sizeof(fixed[0]); i++) {
        CHECK_EQ(dec_any(fixed[i].type, NULL, 0), -1);
        for (size_t len = 0; len < sizeof(zeros); len++)
            CHECK_EQ(dec_any(fixed[i].type, zeros, len) == 0, len == fixed[i].len);
    }
}

int main(void)
{
    init_samples();
    test_header_layout();
    test_payload_caps();
    test_names();
    test_wbuf();
    test_rbuf();
    test_file_hdr_bytes();
    test_get_bytes();
    test_round_trips();
    test_small_caps();
    test_topic_boundaries();
    test_name_boundaries();
    test_unsafe_names();
    test_offsets();
    test_get_malformed();
    test_error_msgs();
    test_manifest_large();
    test_manifest_cap();
    test_manifest_malformed();
    test_fixed_lengths();
    UNIT_DONE();
}
