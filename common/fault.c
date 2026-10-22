/*
 * fault.c - probabilistic fault injection for Part II (design section 4,
 * deviation 9). The contract is in fault.h. Only the Part II servers link this
 * file; Part I contains no fault code at all.
 *
 * WHEN the failure decision is made
 *   The server's file-send loop (send_file_range() in transfer.c) calls
 *   fault_should_fail() through conn_t.fault_hook immediately before it sends
 *   each chunk of file data. A chunk is FDS_CHUNK_SIZE = 64 KB, or less for
 *   the last piece of a file. Therefore:
 *     - every chunk gets exactly one decision, taken before any of its bytes
 *       is handed to the socket;
 *     - headers and control messages (TOPIC_BEGIN, FILE_HDR, FILE_END,
 *       TOPIC_END, RESUME_OK, MANIFEST, ...) never get a decision, so every
 *       injected fault interrupts file data;
 *     - a 0-byte file has no chunk, and so no decision;
 *     - a reconnection is a new connection with its own generator (new
 *       conn_id), so its chunks are decided afresh.
 *   p is therefore a per-64-KB-chunk probability, not a per-connection one.
 *   The same rule holds in Case 1, Case 2 and Enhanced, where each of the K
 *   connections draws in its own worker thread.
 *
 * HOW the failure decision is made
 *   1. Each connection owns one fault_state_t with its own drand48_r()
 *      generator. drand48_r() keeps all of its state in the struct we pass.
 *      rand() and drand48() share hidden global state between threads, so
 *      they would need a lock, and one connection's draws would shift
 *      another's. With drand48_r() the worker threads never interact.
 *   2. Per chunk: r = drand48_r() is uniform in [0, 1). The chunk faults iff
 *      r < p, which happens with probability exactly p, independently of
 *      every other chunk. With N chunks, P(no fault) = (1 - p)^N (section 4.4).
 *   3. On a fault, a second draw r2 decides how much of the chunk still goes
 *      out: prefix = floor(r2 * chunk_len), uniform in [0, chunk_len - 1].
 *      The caller sends exactly that prefix, logs the FAULT event, then calls
 *      shutdown(SHUT_RDWR) + close() and ends the connection; the server
 *      keeps running. So the cut lands at a random byte of the chunk, not
 *      only on 64 KB boundaries. That exercises the clients' byte-offset
 *      resume: most images are under 64 KB, so whole-chunk faults would
 *      always cut at offset 0 of a file (deviation 9). Keeping
 *      prefix < chunk_len means at least one byte of the chunk is always
 *      lost, so every injected fault really forces a recovery.
 *      The second draw happens only on a fault, which is the last draw on
 *      that connection. It therefore never changes WHICH chunk faults.
 *   4. p <= 0 returns at once without drawing: p = 0 behaves exactly like
 *      Part I (and Part I's p = -1 would too). p >= 1 makes every r < p, so
 *      every chunk faults. Each fault still sends a random prefix, so Case 2
 *      and Enhanced keep advancing; only Case 1 can never finish
 *      (deviation 9).
 *
 * SEEDING (fault_init(), once per accepted connection)
 *   With the [seed] argument: the 48-bit generator state is a hash of
 *   (seed, conn_id). The same seed reproduces the same fault positions on
 *   every connection, so an experiment run can be repeated exactly, and each
 *   connection still gets its own sequence. The design said
 *   srand48_r(seed ^ conn_id). Plain XOR is a bad seed combiner here: the
 *   repetitions use seeds 1001, 1002, ..., which differ only in their low
 *   bits, so {1001 ^ c} and {1002 ^ c} over conn_ids 1..32 share 30 of 32
 *   values. Two "independent" repetitions would replay the same fault
 *   sequences in a different order (deviation 20). Hashing each input
 *   separately makes every (seed, conn_id) pair start an unrelated
 *   sequence.
 *   Without it: wall-clock time, thread id, conn_id and monotonic time are
 *   hashed together into all 48 bits of the generator state (see
 *   fault_init()).
 */
#include "fault.h"
#include "server_core.h"   /* fault_hook_fn, checked below */
#include "util.h"          /* now_ns(), wall_ns() */

#include <pthread.h>
#include <string.h>

/* fault_should_fail() is installed as conn_t.fault_hook. The compiler proves
 * that the two types still match if either header ever changes. */
_Static_assert(__builtin_types_compatible_p(__typeof__(&fault_should_fail), fault_hook_fn),
               "fault_should_fail() must have the type fault_hook_fn");

/*
 * splitmix64 finaliser: a bijection on 64-bit values in which every input bit
 * affects every output bit. Used only to build a seed when the user gave none.
 */
static uint64_t mix64(uint64_t x)
{
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

void fault_init(fault_state_t *f, double p, int have_seed, long seed, uint64_t conn_id)
{
    memset(f, 0, sizeof(*f));           /* draws = faults = 0 */
    f->p = p;
    if (have_seed) {
        /* Repeatable: depends only on (seed, conn_id). See SEEDING above
         * for why this is a hash and not seed ^ conn_id. */
        unsigned short xsubi[3];
        uint64_t h = mix64(mix64((uint64_t)(unsigned long)seed) ^ conn_id);

        xsubi[0] = (unsigned short)h;
        xsubi[1] = (unsigned short)(h >> 16);
        xsubi[2] = (unsigned short)(h >> 32);
        (void)seed48_r(xsubi, &f->rng);
    } else {
        /*
         * time ^ thread id ^ conn_id (design section 4.2), and monotonic time
         * so that two connections set up in the same wall-clock tick still
         * differ. Plain XOR is not good enough: correlated inputs can cancel
         * out (two threads' ids differ only in a few bits), and srand48_r()
         * would keep only 32 bits of the result. Hashing after each input
         * makes every bit of every input count, and seed48_r() loads all 48
         * bits of the drand48 state.
         */
        unsigned short xsubi[3];
        uint64_t h = mix64((uint64_t)wall_ns());

        h = mix64(h ^ (uint64_t)(unsigned long)pthread_self());
        h = mix64(h ^ conn_id);
        h = mix64(h ^ (uint64_t)now_ns());
        xsubi[0] = (unsigned short)h;
        xsubi[1] = (unsigned short)(h >> 16);
        xsubi[2] = (unsigned short)(h >> 32);
        (void)seed48_r(xsubi, &f->rng);
    }
    /* srand48_r()/seed48_r() only store values into f->rng and cannot fail
     * in glibc, hence the (void). */
}

/*
 * The fault_hook_fn: one decision for the chunk of chunk_len bytes that is
 * about to be sent (see WHEN and HOW at the top of this file). Called only by
 * the connection's own worker thread, so f needs no lock.
 */
int fault_should_fail(void *fault_state, size_t chunk_len, size_t *prefix)
{
    fault_state_t *f = fault_state;
    double r, r2;
    size_t pre;

    /* p <= 0 (the !(p > 0) form also treats NaN as 0): no fault is possible,
     * so draw nothing. chunk_len == 0 is not a chunk: no 0 <= prefix < 0
     * exists, and a 0-byte file gets no draw. */
    if (!(f->p > 0.0) || chunk_len == 0)
        return 0;

    f->draws++;
    /* glibc's drand48_r() cannot fail. If it ever did, skipping the fault
     * keeps the transfer correct, and the fault is the optional part. */
    if (drand48_r(&f->rng, &r) != 0 || !(r < f->p))
        return 0;

    if (drand48_r(&f->rng, &r2) != 0)
        r2 = 0.0;
    /* r2 < 1, so r2 * chunk_len < chunk_len in exact arithmetic. The clamp
     * only guards against floating-point rounding, which keeps
     * prefix < chunk_len a hard guarantee (chunk_len == 1 gives prefix 0). */
    pre = (size_t)(r2 * (double)chunk_len);
    if (pre >= chunk_len)
        pre = chunk_len - 1;
    *prefix = pre;
    f->faults++;
    return 1;
}
