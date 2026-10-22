/*
 * protocol.h - message framing, encoding and decoding (design section 3).
 *
 * Every message = 16-byte header + payload_len bytes of payload:
 *
 *   offset 0  magic       u16  0x5446
 *          2  version     u8   1
 *          3  type        u8   msg_type_t
 *          4  reserved    u32  0
 *          8  payload_len u64  bytes of payload that follow
 *
 * All integers are big-endian (network byte order). Structures are never sent
 * raw: every field is packed individually with put_u* / get_u*.
 *
 * FILE_HDR is special: its payload_len covers only the metadata. It is followed
 * by exactly (file_size - start_offset) raw file bytes that are not framed.
 *
 * Two layers:
 *   - pure buffer encoders/decoders (enc_* / dec_*) usable by blocking code and
 *     by the non-blocking poll() client;
 *   - fd helpers (send_* / recv_msg) for blocking sockets.
 * All encoders return the total number of bytes written to buf (header +
 * payload), or 0 if cap is too small or an argument is invalid.
 * All decoders take the PAYLOAD only and return 0 on success, -1 if malformed
 * (wrong length, string too long, unsafe name, trailing bytes, ...).
 */
#ifndef FDS_PROTOCOL_H
#define FDS_PROTOCOL_H

#include <stddef.h>
#include <stdint.h>
#include "fds.h"

typedef enum {
    MSG_GET           = 0x01,  /* C->S  "GET <topic>" (ASCII)                  Part I, Case 1, Case 2 */
    MSG_RESUME        = 0x02,  /* C->S  checkpoint                              Case 2 */
    MSG_MANIFEST_REQ  = 0x03,  /* C->S  topic + K                               Enhanced */
    MSG_FETCH         = 0x04,  /* C->S  same layout as RESUME                   Enhanced */
    MSG_TOPIC_BEGIN   = 0x10,  /* S->C  session id, file count, total bytes     Part I, Case 1, Case 2 */
    MSG_FILE_HDR      = 0x11,  /* S->C  + raw bytes                             all */
    MSG_FILE_END      = 0x12,  /* S->C  file index                              all */
    MSG_TOPIC_END     = 0x13,  /* S->C  file count, total bytes                 Part I, Case 1, Case 2 */
    MSG_RESUME_OK     = 0x14,  /* S->C  resume point                            Case 2 */
    MSG_RESUME_REJECT = 0x15,  /* S->C  reason                                  Case 2, Enhanced */
    MSG_MANIFEST      = 0x16,  /* S->C  file list                               Enhanced */
    MSG_ERROR         = 0x7F   /* S->C  code + text                             all */
} msg_type_t;

/* Human-readable name of a message type ("GET", "FILE_HDR", ..., "?"). */
const char *msg_type_name(uint8_t type);

/* ---- Header --------------------------------------------------------------- */

typedef struct {
    uint8_t  type;
    uint64_t payload_len;
} msg_hdr_t;

/* Write a 16-byte header. */
void hdr_encode(uint8_t out[FDS_HDR_LEN], uint8_t type, uint64_t payload_len);

/*
 * Parse and validate a 16-byte header: magic, version, known type, and
 * payload_len <= max_payload_for_type(type). Returns 0 or -1.
 */
int hdr_decode(const uint8_t in[FDS_HDR_LEN], msg_hdr_t *h);

/* Largest payload accepted for a type: FDS_MAX_MANIFEST_PAYLOAD for MANIFEST,
 * FDS_MAX_CTRL_PAYLOAD for everything else; 0 for an unknown type. */
uint64_t max_payload_for_type(uint8_t type);

/* ---- Byte-buffer packing --------------------------------------------------- */

/* Writer: on overflow sets err=1 and ignores further writes. */
typedef struct { uint8_t *p; size_t cap; size_t len; int err; } wbuf_t;
void wb_init(wbuf_t *w, uint8_t *buf, size_t cap);
void put_u8(wbuf_t *w, uint8_t v);
void put_u16(wbuf_t *w, uint16_t v);
void put_u32(wbuf_t *w, uint32_t v);
void put_u64(wbuf_t *w, uint64_t v);
void put_bytes(wbuf_t *w, const void *src, size_t n);

/* Reader: on underflow sets err=1 and returns 0 / leaves dst untouched. */
typedef struct { const uint8_t *p; size_t len; size_t pos; int err; } rbuf_t;
void rb_init(rbuf_t *r, const uint8_t *buf, size_t len);
uint8_t  get_u8(rbuf_t *r);
uint16_t get_u16(rbuf_t *r);
uint32_t get_u32(rbuf_t *r);
uint64_t get_u64(rbuf_t *r);
void     get_bytes(rbuf_t *r, void *dst, size_t n);
/* Read a length-prefixed string of n bytes into dst (cap includes the NUL). */
void     get_str(rbuf_t *r, char *dst, size_t n, size_t cap);

/* ---- Message payloads (design section 3.3) --------------------------------- */

typedef struct {                       /* TOPIC_BEGIN: session_id[16] file_count u32 total_bytes u64 */
    uint8_t  session_id[FDS_SESSION_ID_LEN];
    uint32_t file_count;
    uint64_t total_bytes;
} m_topic_begin_t;

typedef struct {                       /* FILE_HDR: file_index u32 file_size u64 start_offset u64 name_len u16 name */
    uint32_t file_index;
    uint64_t file_size;
    uint64_t start_offset;             /* 0 except when resuming; <= file_size */
    char     name[FDS_NAME_MAX + 1];
} m_file_hdr_t;

typedef struct {                       /* FILE_END: file_index u32 */
    uint32_t file_index;
} m_file_end_t;

typedef struct {                       /* TOPIC_END: file_count u32 total_bytes u64 */
    uint32_t file_count;
    uint64_t total_bytes;
} m_topic_end_t;

typedef struct {                       /* RESUME and FETCH: session_id[16] file_index u32 offset u64
                                          topic_len u8 topic name_len u16 name */
    uint8_t  session_id[FDS_SESSION_ID_LEN];
    uint32_t file_index;
    uint64_t offset;
    char     topic[FDS_TOPIC_MAX + 1];
    char     name[FDS_NAME_MAX + 1];
} m_resume_t;

typedef struct {                       /* RESUME_OK: file_index u32 offset u64 */
    uint32_t file_index;
    uint64_t offset;
} m_resume_ok_t;

typedef struct {                       /* MANIFEST_REQ: topic_len u8 topic connections u8 */
    char    topic[FDS_TOPIC_MAX + 1];
    uint8_t connections;
} m_manifest_req_t;

typedef struct {                       /* MANIFEST: session_id[16] file_count u32 total_bytes u64,
                                          then per file: file_size u64 name_len u16 name */
    uint8_t       session_id[FDS_SESSION_ID_LEN];
    uint32_t      file_count;
    uint64_t      total_bytes;
    file_entry_t *files;               /* decoder: malloc'd array of file_count entries */
} m_manifest_t;

typedef struct {                       /* ERROR: code u16 msg_len u16 msg */
    uint16_t code;
    char     msg[256];                 /* truncated to 255 bytes */
} m_error_t;

/* ---- Pure encoders: full message (header + payload) into buf ---------------- */

size_t enc_get(uint8_t *buf, size_t cap, const char *topic);
size_t enc_topic_begin(uint8_t *buf, size_t cap, const m_topic_begin_t *m);
size_t enc_file_hdr(uint8_t *buf, size_t cap, const m_file_hdr_t *m);
size_t enc_file_end(uint8_t *buf, size_t cap, uint32_t file_index);
size_t enc_topic_end(uint8_t *buf, size_t cap, const m_topic_end_t *m);
/* type must be MSG_RESUME or MSG_FETCH */
size_t enc_resume(uint8_t *buf, size_t cap, uint8_t type, const m_resume_t *m);
size_t enc_resume_ok(uint8_t *buf, size_t cap, const m_resume_ok_t *m);
size_t enc_resume_reject(uint8_t *buf, size_t cap, uint16_t reason);
size_t enc_manifest_req(uint8_t *buf, size_t cap, const m_manifest_req_t *m);
size_t enc_error(uint8_t *buf, size_t cap, uint16_t code, const char *msg);

/*
 * MANIFEST can be large: returns a malloc'd buffer holding header + payload
 * and stores its length in *len_out, or NULL on allocation failure / when the
 * payload would exceed FDS_MAX_MANIFEST_PAYLOAD.
 */
uint8_t *enc_manifest_alloc(const uint8_t session_id[FDS_SESSION_ID_LEN],
                            const file_entry_t *files, uint32_t n_files,
                            uint64_t total_bytes, size_t *len_out);

/* ---- Pure decoders: payload only -------------------------------------------- */

/* "GET <topic>": exactly "GET " followed by a topic that passes topic_is_valid(). */
int dec_get(const uint8_t *p, size_t len, char topic[FDS_TOPIC_MAX + 1]);
int dec_topic_begin(const uint8_t *p, size_t len, m_topic_begin_t *m);
/* also checks name_is_safe() and start_offset <= file_size */
int dec_file_hdr(const uint8_t *p, size_t len, m_file_hdr_t *m);
int dec_file_end(const uint8_t *p, size_t len, m_file_end_t *m);
int dec_topic_end(const uint8_t *p, size_t len, m_topic_end_t *m);
/* RESUME and FETCH; checks topic_is_valid() and name_is_safe() */
int dec_resume(const uint8_t *p, size_t len, m_resume_t *m);
int dec_resume_ok(const uint8_t *p, size_t len, m_resume_ok_t *m);
int dec_resume_reject(const uint8_t *p, size_t len, uint16_t *reason);
int dec_manifest_req(const uint8_t *p, size_t len, m_manifest_req_t *m);
/* allocates m->files (free with manifest_free); checks every name and that
 * the sizes add up to total_bytes */
int dec_manifest(const uint8_t *p, size_t len, m_manifest_t *m);
void manifest_free(m_manifest_t *m);
int dec_error(const uint8_t *p, size_t len, m_error_t *m);

/* Short text for a RESUME_REJECT reason ("unknown topic", ...). */
const char *reject_reason_str(uint16_t reason);

/* ---- Blocking fd helpers ------------------------------------------------------ */

/*
 * Send an already-encoded message (from an enc_* function). more != 0 adds
 * MSG_MORE so the kernel may coalesce it with what follows (used for
 * FILE_HDR before sendfile()). Returns 0 or -1 (errno set).
 */
int send_encoded(int fd, const uint8_t *buf, size_t len, int more);

/* Convenience: encode + send an ERROR message. Errors are ignored (best effort). */
void send_error(int fd, uint16_t code, const char *msg);

/*
 * Receive one message: the 16-byte header, validated, then the payload into a
 * malloc'd buffer (*payload, NULL when payload_len == 0; caller frees).
 * For FILE_HDR only the metadata payload is read; the raw data is left in the
 * socket for the caller.
 * Returns NET_OK, NET_EOF (clean close before a full header), NET_ERR
 * (errno set; also used for EOF in the middle of a message, errno=ECONNRESET),
 * or NET_PROTO (bad header).
 * If wire != NULL, the number of bytes read is added to *wire.
 */
int recv_msg(int fd, msg_hdr_t *h, uint8_t **payload, uint64_t *wire);

#endif /* FDS_PROTOCOL_H */
