/*
 * test_fault.c - unit tests for fault.c: the per-chunk fault decision
 * (design section 4, deviation 9).
 *
 * Every test calls the decision through a fault_hook_fn pointer, exactly as
 * send_file_range() does. The seeded tests are deterministic, so the
 * statistical bounds below are checked against one fixed sequence and cannot
 * flake. The bounds are still several standard deviations wide, so they would
 * hold for any seed.
 */
#include "unit.h"
#include "fds.h"
#include "fault.h"
#include "server_core.h"

#include <stdint.h>
#include <string.h>

#define RATE_DRAWS    200000
#define UNIFORM_DRAWS 160000
#define SEQ_DRAWS     10000

static const fault_hook_fn hook = fault_should_fail;

/* Run n decisions on chunks of len bytes; return the number of faults. */
static long run(fault_state_t *f, long n, size_t len)
{
    long faults = 0;
    size_t prefix;

    for (long i = 0; i < n; i++)
        faults += hook(f, len, &prefix);
    return faults;
}

/* Number of positions where two generators give a different (rc, prefix). */
static long differences(fault_state_t *a, fault_state_t *b, long n, size_t len)
{
    long diff = 0;

    for (long i = 0; i < n; i++) {
        size_t pa = (size_t)-1, pb = (size_t)-1;
        int ra = hook(a, len, &pa), rb = hook(b, len, &pb);
        if (ra != rb || (ra && pa != pb))
            diff++;
    }
    return diff;
}

int main(void)
{
    fault_state_t f, g;
    struct drand48_data before;
    size_t prefix;

    /* ---- p = 0 (and p < 0, as Part I's p = -1): never faults, never draws ---- */
    fault_init(&f, 0.0, 1, 42, 1);
    before = f.rng;
    CHECK_EQ(run(&f, 100000, FDS_CHUNK_SIZE), 0);
    CHECK_EQ(f.draws, 0);
    CHECK_EQ(f.faults, 0);
    CHECK(memcmp(&before, &f.rng, sizeof(before)) == 0);   /* generator untouched */
    fault_init(&f, -1.0, 1, 42, 1);
    CHECK_EQ(run(&f, 1000, FDS_CHUNK_SIZE), 0);
    CHECK_EQ(f.draws, 0);

    /* ---- p = 1: every chunk faults, prefix always < chunk_len ---- */
    fault_init(&f, 1.0, 1, 42, 1);
    {
        long bad = 0, n = 0;
        for (size_t len = 1; len <= FDS_CHUNK_SIZE; len += 7) {
            prefix = (size_t)-1;
            if (hook(&f, len, &prefix) != 1 || prefix >= len)
                bad++;
            n++;
        }
        CHECK_EQ(bad, 0);
        CHECK_EQ(f.draws, n);
        CHECK_EQ(f.faults, n);
    }
    {
        long bad = 0;
        for (int i = 0; i < 1000; i++) {           /* a 1-byte chunk can only lose everything */
            prefix = (size_t)-1;
            if (hook(&f, 1, &prefix) != 1 || prefix != 0)
                bad++;
        }
        CHECK_EQ(bad, 0);
    }
    {
        uint64_t draws = f.draws;                  /* no chunk -> no draw, even at p = 1 */
        CHECK_EQ(hook(&f, 0, &prefix), 0);
        CHECK_EQ(f.draws, draws);
    }

    /* ---- fault_init() resets the counters ---- */
    fault_init(&f, 1.0, 1, 42, 1);
    CHECK_EQ(f.draws, 0);
    CHECK_EQ(f.faults, 0);
    CHECK(f.p == 1.0);

    /* ---- p = 0.1: observed rate within 0.1 +- 0.005 (sd is about 0.00067) ---- */
    fault_init(&f, 0.1, 1, 12345, 7);
    {
        long faults = run(&f, RATE_DRAWS, FDS_CHUNK_SIZE);
        double rate = (double)faults / RATE_DRAWS;
        printf("p=0.1: %ld faults in %d draws, rate %.5f\n", faults, RATE_DRAWS, rate);
        CHECK(rate > 0.095 && rate < 0.105);
        CHECK_EQ(f.draws, RATE_DRAWS);
        CHECK_EQ(f.faults, faults);
    }

    /* ---- prefix roughly uniform over [0, chunk_len-1] ---- */
    fault_init(&f, 1.0, 1, 777, 3);
    {
        long hist[16] = { 0 }, bad = 0;
        double sum = 0.0, mean, want = (FDS_CHUNK_SIZE - 1) / 2.0;
        size_t lo = FDS_CHUNK_SIZE, hi = 0;

        for (int i = 0; i < UNIFORM_DRAWS; i++) {
            if (hook(&f, FDS_CHUNK_SIZE, &prefix) != 1 || prefix >= FDS_CHUNK_SIZE) {
                bad++;
                continue;
            }
            sum += (double)prefix;
            hist[prefix / (FDS_CHUNK_SIZE / 16)]++;
            lo = prefix < lo ? prefix : lo;
            hi = prefix > hi ? prefix : hi;
        }
        mean = sum / UNIFORM_DRAWS;
        printf("prefix: mean %.1f (uniform: %.1f), min %zu, max %zu\n", mean, want, lo, hi);
        CHECK_EQ(bad, 0);
        CHECK(mean > 0.95 * want && mean < 1.05 * want);
        CHECK(lo < FDS_CHUNK_SIZE / 100);              /* both ends of the range are reached */
        CHECK(hi > FDS_CHUNK_SIZE - FDS_CHUNK_SIZE / 100);
        for (int b = 0; b < 16; b++)                   /* 10000 expected per bucket, sd about 97 */
            CHECK(hist[b] > 9000 && hist[b] < 11000);
    }
    {
        long count[4] = { 0 };                         /* a tiny chunk: every legal prefix occurs */
        fault_init(&f, 1.0, 1, 778, 3);
        for (int i = 0; i < 3000; i++) {
            prefix = 3;
            hook(&f, 3, &prefix);
            count[prefix <= 3 ? prefix : 3]++;
        }
        CHECK(count[0] > 800 && count[1] > 800 && count[2] > 800);
        CHECK_EQ(count[3], 0);
    }

    /* ---- determinism: same seed and conn_id -> identical sequence ---- */
    fault_init(&f, 0.3, 1, 99, 5);
    fault_init(&g, 0.3, 1, 99, 5);
    CHECK(memcmp(&f.rng, &g.rng, sizeof(f.rng)) == 0);
    CHECK_EQ(differences(&f, &g, SEQ_DRAWS, FDS_CHUNK_SIZE), 0);
    CHECK_EQ(f.faults, g.faults);
    CHECK(f.faults > 0);                               /* the faulting path was compared too */

    /* ---- same seed, different conn_id -> different sequence ---- */
    fault_init(&f, 0.5, 1, 99, 5);
    fault_init(&g, 0.5, 1, 99, 6);
    CHECK(differences(&f, &g, 1000, FDS_CHUNK_SIZE) > 100);

    /* ---- different seed, same conn_id -> different sequence ---- */
    fault_init(&f, 0.5, 1, 99, 5);
    fault_init(&g, 0.5, 1, 100, 5);
    CHECK(differences(&f, &g, 1000, FDS_CHUNK_SIZE) > 100);

    /* ---- no seed: two inits (same thread, same conn_id) still differ ---- */
    fault_init(&f, 0.5, 0, 0, 1);
    fault_init(&g, 0.5, 0, 0, 1);
    CHECK(memcmp(&f.rng, &g.rng, sizeof(f.rng)) != 0);
    CHECK(differences(&f, &g, 1000, FDS_CHUNK_SIZE) > 100);
    CHECK_EQ(f.draws, 1000);

    /* ---- unseeded generators still give the right rate ---- */
    fault_init(&f, 0.1, 0, 0, 2);
    {
        double rate = (double)run(&f, RATE_DRAWS, FDS_CHUNK_SIZE) / RATE_DRAWS;
        CHECK(rate > 0.095 && rate < 0.105);
    }

    UNIT_DONE();
}
