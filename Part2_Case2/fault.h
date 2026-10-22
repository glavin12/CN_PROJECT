/*
 * fault.h - probabilistic fault injection (design section 4, deviations 9,
 * 20 and 28).
 *
 * p is the probability that a CONNECTION is interrupted (deviation 28: the
 * design drew once per 64 KB chunk, which made Case 1 unable to finish a
 * real-size topic at any useful p).
 *
 * WHEN: one decision per connection, taken when the server accepts the
 * connection's first data request and knows how many file bytes it is about
 * to send for it (fault_plan()):
 *   Case 1   GET            the whole topic
 *   Case 2   GET / RESUME   the rest of the topic from the checkpoint
 *   Enhanced first FETCH    the rest of the requested file
 * A request with 0 data bytes does not use up the decision; the next one
 * takes it. Control messages never get a decision.
 *
 * HOW: a per-connection drand48_r() generator (thread-safe, no global state).
 * Draw r in [0,1); the connection will be cut iff r < p. If so, a second draw
 * picks the cut byte uniformly in [0, bytes-1] of that request's data. The
 * file-send loop (send_file_range) calls fault_should_fail() before every
 * 64 KB chunk; for the chunk that contains the cut byte it returns the prefix
 * to send. The server sends that prefix, logs a FAULT event, then calls
 * shutdown(SHUT_RDWR) + close(). At most one fault per connection, and at
 * least one byte of the request is always lost, so a fault always forces a
 * recovery. p = 0 never cuts; p = 1 cuts every connection, so nothing can
 * finish.
 *
 * SEED: with the optional [seed] server argument the generator state is a
 * hash of (seed, conn_id), which is repeatable per connection (deviation 20:
 * the design's seed ^ conn_id makes runs with nearby seeds replay the same
 * sequences); without it, a hash of time, thread id and conn_id.
 */
#ifndef FDS_FAULT_H
#define FDS_FAULT_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct fault_state {
    double              p;
    struct drand48_data rng;
    int                 planned;   /* the connection's decision has been taken */
    int                 doomed;    /* ... and it said: cut this connection */
    uint64_t            cut_at;    /* data byte, counted from the plan, where the cut falls */
    uint64_t            pos;       /* data bytes offered to fault_should_fail() since the plan */
    uint64_t            draws;     /* decisions taken (0 or 1 per connection) */
    uint64_t            faults;    /* faults injected (0 or 1 per connection) */
} fault_state_t;

/* Seed the generator for one connection (see SEED above). No decision yet. */
void fault_init(fault_state_t *f, double p, int have_seed, long seed, uint64_t conn_id);

/*
 * Take the connection's one decision for a request that will send `bytes`
 * file data bytes (see WHEN above). Does nothing if the decision was already
 * taken or if bytes == 0.
 */
void fault_plan(fault_state_t *f, uint64_t bytes);

/*
 * The fault_hook_fn of server_core.h, called before every chunk of
 * chunk_len data bytes. Returns 1 and sets *prefix (0 <= *prefix < chunk_len)
 * when the planned cut falls inside this chunk, 0 otherwise.
 * fault_state is a fault_state_t *.
 */
int fault_should_fail(void *fault_state, size_t chunk_len, size_t *prefix);

#endif /* FDS_FAULT_H */
