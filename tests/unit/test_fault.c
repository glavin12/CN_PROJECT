/*
 * test_fault.c - unit tests for fault.c: one fault decision per connection
 * (design section 4, deviations 9, 20 and 28).
 *
 * Every test feeds chunks through a fault_hook_fn pointer, exactly as
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

#define RATE_CONNS    200000
#define UNIFORM_CONNS 160000

static const fault_hook_fn hook = fault_should_fail;

/*
 * One simulated connection: plan `bytes`, then offer the data in chunks of at
 * most `len` bytes until the plan is used up or a fault fires. Returns 1 and
 * the absolute cut byte in *cut on a fault, 0 if the data all went out.
 */
static int stream(fault_state_t *f, uint64_t bytes, size_t len, uint64_t *cut)
{
    uint64_t pos = 0;

    fault_plan(f, bytes);
    while (pos < bytes) {
        size_t n = (size_t)(bytes - pos < len ? bytes - pos : len);
        size_t prefix = (size_t)-1;

        if (hook(f, n, &prefix) == 1) {
            if (prefix >= n)
                return -1;                 /* contract broken: caught by the caller */
            *cut = pos + prefix;
            return 1;
        }
        pos += n;
    }
    return 0;
}

/* Connections c = 1..n with (p, seed): number that faulted. */
static long rate_run(double p, int have_seed, long seed, long n, uint64_t bytes)
{
    fault_state_t f;
    uint64_t cut;
    long faults = 0;

    for (long c = 1; c <= n; c++) {
        fault_init(&f, p, have_seed, seed, (uint64_t)c);
        faults += stream(&f, bytes, FDS_CHUNK_SIZE, &cut) == 1;
    }
    return faults;
}

/* Outcome of one connection as one number: -1 = no fault, else the cut byte. */
static int64_t outcome(double p, int have_seed, long seed, uint64_t conn, uint64_t bytes)
{
    fault_state_t f;
    uint64_t cut = 0;

    fault_init(&f, p, have_seed, seed, conn);
    return stream(&f, bytes, FDS_CHUNK_SIZE, &cut) == 1 ? (int64_t)cut : -1;
}

int main(void)
{
    fault_state_t f;
    struct drand48_data before;
    size_t prefix;
    uint64_t cut;

    /* ---- p = 0 (and p < 0, as Part I's p = -1): never faults, never draws ---- */
    fault_init(&f, 0.0, 1, 42, 1);
    before = f.rng;
    CHECK_EQ(stream(&f, 100ull * FDS_CHUNK_SIZE, FDS_CHUNK_SIZE, &cut), 0);
    CHECK_EQ(f.draws, 0);
    CHECK_EQ(f.faults, 0);
    CHECK(memcmp(&before, &f.rng, sizeof(before)) == 0);   /* generator untouched */
    fault_init(&f, -1.0, 1, 42, 1);
    CHECK_EQ(stream(&f, 1000, 7, &cut), 0);
    CHECK_EQ(f.draws, 0);

    /* ---- no plan, no fault (even at p = 1) ---- */
    fault_init(&f, 1.0, 1, 42, 1);
    CHECK_EQ(hook(&f, FDS_CHUNK_SIZE, &prefix), 0);
    CHECK_EQ(f.draws, 0);

    /* ---- a 0-byte request does not use up the decision ---- */
    fault_init(&f, 1.0, 1, 42, 1);
    fault_plan(&f, 0);
    CHECK_EQ(f.planned, 0);
    CHECK_EQ(f.draws, 0);
    CHECK_EQ(stream(&f, 10, 4, &cut), 1);                  /* the next request takes it */
    CHECK(cut < 10);

    /* ---- p = 1: every connection is cut, inside its data, exactly once ---- */
    {
        long bad = 0;

        for (uint64_t c = 1; c <= 2000; c++) {
            uint64_t bytes = 1 + c * 997;                  /* 998 B .. about 2 MB */
            size_t lens[] = { 1, 7, 1000, FDS_CHUNK_SIZE };

            fault_init(&f, 1.0, 1, 42, c);
            if (stream(&f, bytes, lens[c % 4], &cut) != 1 || cut >= bytes)
                bad++;
            if (f.draws != 1 || f.faults != 1)
                bad++;
            /* after the cut: never again on this connection */
            if (hook(&f, FDS_CHUNK_SIZE, &prefix) != 0 || f.faults != 1)
                bad++;
        }
        CHECK_EQ(bad, 0);
    }
    {
        long bad = 0;                                      /* a 1-byte request can only lose everything */
        for (uint64_t c = 1; c <= 1000; c++) {
            fault_init(&f, 1.0, 1, 7, c);
            if (stream(&f, 1, FDS_CHUNK_SIZE, &cut) != 1 || cut != 0)
                bad++;
        }
        CHECK_EQ(bad, 0);
    }

    /* ---- one decision per connection: a second plan changes nothing ---- */
    fault_init(&f, 0.5, 1, 42, 3);
    fault_plan(&f, 1000);
    {
        fault_state_t copy = f;

        fault_plan(&f, 5000000);
        CHECK(memcmp(&copy, &f, sizeof(f)) == 0);
        CHECK_EQ(f.draws, 1);
    }

    /* ---- fault_init() resets everything ---- */
    fault_init(&f, 1.0, 1, 42, 1);
    CHECK_EQ(f.draws, 0);
    CHECK_EQ(f.faults, 0);
    CHECK_EQ(f.planned, 0);
    CHECK_EQ(f.doomed, 0);
    CHECK(f.p == 1.0);

    /* ---- p = 0.1: fraction of connections cut within 0.1 +- 0.005 (sd about
     * 0.00067), and it does not depend on the size of the request ---- */
    {
        long small = rate_run(0.1, 1, 12345, RATE_CONNS, 1000);
        long large = rate_run(0.1, 1, 12346, RATE_CONNS / 10, 50ull * 1000 * 1000);
        double rs = (double)small / RATE_CONNS, rl = (double)large / (RATE_CONNS / 10);

        printf("p=0.1: %.5f of 1 KB connections cut, %.5f of 50 MB connections cut\n", rs, rl);
        CHECK(rs > 0.095 && rs < 0.105);
        CHECK(rl > 0.085 && rl < 0.115);                   /* 20000 conns: sd about 0.0021 */
    }

    /* ---- the cut byte is roughly uniform over [0, bytes-1], also across chunks ---- */
    {
        const uint64_t bytes = 16ull * FDS_CHUNK_SIZE;     /* 1 MiB: 16 chunks */
        long hist[16] = { 0 }, bad = 0;
        double sum = 0.0, mean, want = (bytes - 1) / 2.0;
        uint64_t lo = bytes, hi = 0;

        for (uint64_t c = 1; c <= UNIFORM_CONNS; c++) {
            fault_init(&f, 1.0, 1, 777, c);
            if (stream(&f, bytes, FDS_CHUNK_SIZE, &cut) != 1 || cut >= bytes) {
                bad++;
                continue;
            }
            sum += (double)cut;
            hist[cut / FDS_CHUNK_SIZE]++;                  /* which chunk was cut */
            lo = cut < lo ? cut : lo;
            hi = cut > hi ? cut : hi;
        }
        mean = sum / UNIFORM_CONNS;
        printf("cut byte: mean %.1f (uniform: %.1f), min %llu, max %llu\n", mean, want,
               (unsigned long long)lo, (unsigned long long)hi);
        CHECK_EQ(bad, 0);
        CHECK(mean > 0.98 * want && mean < 1.02 * want);
        CHECK(lo < bytes / 1000);                          /* both ends of the range are reached */
        CHECK(hi > bytes - bytes / 1000);
        for (int b = 0; b < 16; b++)                       /* 10000 expected per chunk, sd about 97 */
            CHECK(hist[b] > 9000 && hist[b] < 11000);
    }
    {
        long count[4] = { 0 };                             /* a tiny request: every legal cut occurs */
        for (uint64_t c = 1; c <= 3000; c++) {
            fault_init(&f, 1.0, 1, 778, c);
            cut = 3;
            stream(&f, 3, FDS_CHUNK_SIZE, &cut);
            count[cut <= 3 ? cut : 3]++;
        }
        CHECK(count[0] > 800 && count[1] > 800 && count[2] > 800);
        CHECK_EQ(count[3], 0);
    }

    /* ---- the chunking does not move the cut: same connection, any chunk size ---- */
    {
        long bad = 0;
        for (uint64_t c = 1; c <= 500; c++) {
            uint64_t c1 = 0, c2 = 0;
            fault_state_t g;
            int r1, r2;

            fault_init(&f, 0.7, 1, 5, c);
            fault_init(&g, 0.7, 1, 5, c);
            r1 = stream(&f, 300000, FDS_CHUNK_SIZE, &c1);
            r2 = stream(&g, 300000, 333, &c2);
            if (r1 != r2 || (r1 == 1 && c1 != c2))
                bad++;
        }
        CHECK_EQ(bad, 0);
    }

    /* ---- determinism: same seed and conn_id -> identical outcome ---- */
    {
        long diff = 0, faulted = 0;
        for (uint64_t c = 1; c <= 1000; c++) {
            int64_t a = outcome(0.3, 1, 99, c, 10000000), b = outcome(0.3, 1, 99, c, 10000000);
            diff += a != b;
            faulted += a >= 0;
        }
        CHECK_EQ(diff, 0);
        CHECK(faulted > 0);                                /* the faulting path was compared too */
    }

    /* ---- same seed, different conn_id / different seed, same conn_id -> differ ---- */
    {
        long d_conn = 0, d_seed = 0;
        for (uint64_t c = 1; c <= 1000; c++) {
            d_conn += outcome(1.0, 1, 99, c, 10000000) != outcome(1.0, 1, 99, c + 1000, 10000000);
            d_seed += outcome(1.0, 1, 99, c, 10000000) != outcome(1.0, 1, 100, c, 10000000);
        }
        CHECK(d_conn > 990);
        CHECK(d_seed > 990);
    }

    /* ---- no seed: two inits (same thread, same conn_id) still differ ---- */
    {
        fault_state_t g;
        fault_init(&f, 0.5, 0, 0, 1);
        fault_init(&g, 0.5, 0, 0, 1);
        CHECK(memcmp(&f.rng, &g.rng, sizeof(f.rng)) != 0);
    }

    /* ---- unseeded generators still give the right rate ---- */
    {
        double rate = (double)rate_run(0.1, 0, 0, RATE_CONNS, 1000) / RATE_CONNS;
        CHECK(rate > 0.095 && rate < 0.105);
    }

    /* ---- deviation 20: nearby seeds must not replay each other's sequences.
     * With the design's seed ^ conn_id, seed 1001 on conn c equals seed 1002
     * on conn c ^ 3, so 30 of the 32 per-connection generators of two
     * repetitions were identical. With hashed seeding, no generator of
     * repetition 2 may match any generator of repetition 1 (compared by the
     * cut byte of a 10 MB request at p = 1, which differs between any two
     * unrelated generators). ---- */
    {
        int same = 0;

        for (uint64_t c1 = 1; c1 <= 32; c1++)
            for (uint64_t c2 = 1; c2 <= 32; c2++)
                if (outcome(1.0, 1, 1001, c1, 10000000) == outcome(1.0, 1, 1002, c2, 10000000))
                    same++;
        CHECK_EQ(same, 0);
    }

    UNIT_DONE();
}
