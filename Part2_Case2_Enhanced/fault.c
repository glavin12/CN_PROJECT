/*
 * fault.c - probabilistic fault injection for Part II (design section 4,
 * deviations 9, 20 and 28). The contract is in fault.h. Only the Part II
 * servers link this file; Part I contains no fault code at all.
 *
 * WHAT p means (deviation 28)
 *   p is the probability that one connection is interrupted. The guidelines
 *   say p "specifies how frequently the client-server session is
 *   interrupted". The design drew once per 64 KB chunk instead, so an attempt
 *   of N chunks survived with probability (1 - p)^N: for a real topic (gatto,
 *   about 1 800 chunks) Case 1 could not finish even at p = 0.01. With one
 *   decision per connection a Case 1 attempt succeeds with probability
 *   1 - p whatever the topic size, so every case finishes for any p < 1, and
 *   Case 1 vs Case 2 differ exactly where they should: in how much data a
 *   cut wastes.
 *
 * WHEN the failure decision is made
 *   Once per connection, in fault_plan(), when the server has accepted the
 *   connection's first data request and knows how many file bytes it is
 *   about to send for it:
 *     - Case 1 GET: the whole topic (it is always re-sent from file 0);
 *     - Case 2 GET or RESUME: the rest of the topic from the checkpoint;
 *     - Enhanced: the rest of the file named by the connection's first FETCH.
 *       The server cannot know which later FETCHes the client will send on
 *       the same connection, so the cut is placed in what it does know.
 *   A request with 0 data bytes (an empty topic, a 0-byte file) has nothing
 *   to cut, so it leaves the decision to the connection's next request.
 *   Control messages (TOPIC_BEGIN, FILE_HDR, MANIFEST, ...) never get a
 *   decision, so every injected fault interrupts file data. A reconnection is
 *   a new connection with its own generator (new conn_id), so it is decided
 *   afresh.
 *
 * HOW the failure decision is made
 *   1. Each connection owns one fault_state_t with its own drand48_r()
 *      generator. drand48_r() keeps all of its state in the struct we pass.
 *      rand() and drand48() share hidden global state between threads, so
 *      they would need a lock, and one connection's draws would shift
 *      another's. With drand48_r() the worker threads never interact.
 *   2. r = drand48_r() is uniform in [0, 1). The connection is cut iff
 *      r < p, which happens with probability exactly p.
 *   3. If it is cut, a second draw r2 picks the cut byte:
 *      cut_at = floor(r2 * bytes), uniform in [0, bytes - 1]. Bytes before
 *      cut_at still go out, the byte at cut_at and everything after it do
 *      not. So the cut lands at a random byte (not on a 64 KB or file
 *      boundary), which is what exercises the clients' byte-offset resume,
 *      and at least one byte is always lost, so every fault really forces a
 *      recovery.
 *   4. transfer.c calls fault_should_fail() before each chunk of file data
 *      (64 KB, or less at the end of a file). It counts the chunks' bytes;
 *      for the chunk that contains cut_at it returns prefix = cut_at - pos.
 *      The caller sends exactly that prefix, logs the FAULT event, then calls
 *      shutdown(SHUT_RDWR) + close() and ends the connection; the server
 *      keeps running.
 *   5. p <= 0 never cuts and draws nothing: p = 0 behaves exactly like
 *      Part I. p >= 1 makes every r < p: every connection is cut before the
 *      last byte of its first request, so no transfer can ever finish and the
 *      clients give up after max_retries.
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
 * affects every output bit. Builds the generator state from the inputs.
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
    memset(f, 0, sizeof(*f));           /* no plan yet; draws = faults = 0 */
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

void fault_plan(fault_state_t *f, uint64_t bytes)
{
    double r, r2;

    /* One decision per connection; a request without data cannot be cut, so
     * it leaves the decision to the next request. */
    if (f->planned || bytes == 0)
        return;
    f->planned = 1;
    f->pos = 0;
    /* p <= 0 (the !(p > 0) form also treats NaN as 0): no fault is possible,
     * so draw nothing. */
    if (!(f->p > 0.0))
        return;

    f->draws++;
    /* glibc's drand48_r() cannot fail. If it ever did, skipping the fault
     * keeps the transfer correct, and the fault is the optional part. */
    if (drand48_r(&f->rng, &r) != 0 || !(r < f->p))
        return;
    if (drand48_r(&f->rng, &r2) != 0)
        r2 = 0.0;
    /* r2 < 1, so r2 * bytes < bytes in exact arithmetic. The clamp only
     * guards against floating-point rounding (a uint64_t has more digits
     * than a double), which keeps cut_at < bytes a hard guarantee. */
    f->cut_at = (uint64_t)(r2 * (double)bytes);
    if (f->cut_at >= bytes)
        f->cut_at = bytes - 1;
    f->doomed = 1;
}

/*
 * The fault_hook_fn, called before every chunk of file data (see HOW, step 4).
 * Called only by the connection's own worker thread, so f needs no lock.
 */
int fault_should_fail(void *fault_state, size_t chunk_len, size_t *prefix)
{
    fault_state_t *f = fault_state;

    /* No plan yet (cannot happen for a data chunk, but then: no fault), no
     * cut planned, or the one cut already happened. chunk_len == 0 is not a
     * chunk: no 0 <= prefix < 0 exists. */
    if (!f->planned || chunk_len == 0)
        return 0;
    if (!f->doomed || f->faults > 0 || f->cut_at >= f->pos + chunk_len) {
        f->pos += chunk_len;
        return 0;
    }
    /* cut_at is in [pos, pos + chunk_len): send the bytes before it. If the
     * chunks did not start exactly at the plan (cannot happen), cut at once. */
    *prefix = f->cut_at >= f->pos ? (size_t)(f->cut_at - f->pos) : 0;
    f->faults++;
    return 1;
}
