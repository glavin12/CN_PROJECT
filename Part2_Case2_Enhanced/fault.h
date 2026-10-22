/*
 * fault.h - probabilistic fault injection (design section 4, deviation 9).
 *
 * WHEN: inside the server's file-send loop (send_file_range), immediately
 * before every 64 KB chunk of file data. Headers and control messages never
 * get a draw. A 0-byte file has no chunk and no draw.
 *
 * HOW: one independent uniform draw r in [0,1) per chunk from a per-connection
 * drand48_r() generator (thread-safe, no global state). If r < p the fault is
 * injected: a second draw picks a prefix length uniformly in [0, chunk_len-1];
 * the server sends that prefix of the chunk, logs a FAULT event, then calls
 * shutdown(SHUT_RDWR) + close(). The cut therefore lands at a random byte
 * inside the chunk, so client checkpoints are not 64 KB aligned.
 *
 * SEED: with the optional [seed] server argument the generator is seeded with
 * seed ^ conn_id (repeatable per connection); without it with
 * time ^ thread id ^ conn_id.
 *
 * p is therefore the probability that the connection is cut during any given
 * 64 KB chunk. p = 0 never cuts; p = 1 cuts every chunk.
 */
#ifndef FDS_FAULT_H
#define FDS_FAULT_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct fault_state {
    double              p;
    struct drand48_data rng;
    uint64_t            draws;     /* chunks that got a draw */
    uint64_t            faults;    /* faults injected (0 or 1 per connection) */
} fault_state_t;

/* Seed the generator for one connection (see SEED above). */
void fault_init(fault_state_t *f, double p, int have_seed, long seed, uint64_t conn_id);

/*
 * The fault_hook_fn of server_core.h: one draw for a chunk of chunk_len bytes.
 * Returns 1 and sets *prefix (0 <= *prefix < chunk_len) to inject a fault,
 * 0 otherwise. fault_state is a fault_state_t *.
 */
int fault_should_fail(void *fault_state, size_t chunk_len, size_t *prefix);

#endif /* FDS_FAULT_H */
