/*
 * test_workq.c - unit tests for workq.c (design section 6.2).
 *
 * The CHECK macros of unit.h use plain counters, so worker threads never call
 * them; they record into atomics and main() checks afterwards.
 */
#include "unit.h"
#include "workq.h"
#include "util.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>

static void test_fifo_and_capacity(void)
{
    workq_t *q = workq_create(4);
    work_item_t it;

    CHECK(q != NULL);
    for (uint32_t i = 0; i < 4; i++)
        CHECK_EQ(workq_push(q, i, 100 * i), 0);
    CHECK_EQ(workq_push(q, 99, 0), -1);              /* full */
    CHECK_EQ(workq_outstanding(q), 4);
    for (uint32_t i = 0; i < 4; i++) {
        CHECK_EQ(workq_pop(q, &it), 1);
        CHECK_EQ(it.idx, i);                         /* FIFO */
        CHECK_EQ(it.off, 100 * i);
        CHECK_EQ(workq_outstanding(q), 4 - i);       /* popped items stay counted while busy */
        workq_done(q);
        CHECK_EQ(workq_outstanding(q), 3 - i);
    }
    /* Empty and nobody busy: complete, at once. */
    CHECK_EQ(workq_pop(q, &it), 0);
    CHECK_EQ(workq_pop(q, &it), 0);
    CHECK_EQ(workq_aborted(q), 0);

    /* The ring wraps around many times and stays FIFO; a re-queued item
     * goes to the back. */
    for (uint32_t i = 0; i < 3; i++)
        CHECK_EQ(workq_push(q, i, 0), 0);
    for (uint32_t round = 0; round < 20; round++) {
        CHECK_EQ(workq_pop(q, &it), 1);
        CHECK_EQ(it.idx, round);
        CHECK_EQ(workq_push(q, round + 3, round), 0);
        workq_done(q);
    }
    CHECK_EQ(workq_push(q, 1000, 0), 0);             /* 4th slot */
    CHECK_EQ(workq_push(q, 1001, 0), -1);
    for (uint32_t i = 20; i < 23; i++) {
        CHECK_EQ(workq_pop(q, &it), 1);
        CHECK_EQ(it.idx, i);
        workq_done(q);
    }
    CHECK_EQ(workq_pop(q, &it), 1);
    CHECK_EQ(it.idx, 1000);
    workq_done(q);
    CHECK_EQ(workq_outstanding(q), 0);
    /* An extra done() must not wrap the busy count. */
    workq_done(q);
    CHECK_EQ(workq_outstanding(q), 0);
    CHECK_EQ(workq_pop(q, &it), 0);
    workq_destroy(q);

    /* Capacity 0 (empty topic). */
    q = workq_create(0);
    CHECK(q != NULL);
    CHECK_EQ(workq_push(q, 0, 0), -1);
    CHECK_EQ(workq_outstanding(q), 0);
    CHECK_EQ(workq_trypop(q, &it), 0);
    CHECK_EQ(workq_pop(q, &it), 0);
    workq_destroy(q);
    workq_destroy(NULL);                             /* harmless */
}

static void test_trypop_and_outstanding(void)
{
    workq_t *q = workq_create(2);
    work_item_t it;

    CHECK_EQ(workq_trypop(q, &it), 0);               /* empty right now */
    CHECK_EQ(workq_push(q, 7, 70), 0);
    CHECK_EQ(workq_push(q, 8, 80), 0);
    CHECK_EQ(workq_trypop(q, &it), 1);
    CHECK_EQ(it.idx, 7);
    CHECK_EQ(it.off, 70);
    CHECK_EQ(workq_outstanding(q), 2);               /* 1 queued + 1 busy */
    CHECK_EQ(workq_trypop(q, &it), 1);
    CHECK_EQ(it.idx, 8);
    CHECK_EQ(workq_outstanding(q), 2);               /* 0 queued + 2 busy */
    CHECK_EQ(workq_trypop(q, &it), 0);               /* never blocks, even with busy > 0 */
    workq_done(q);
    CHECK_EQ(workq_outstanding(q), 1);
    /* The poll engine re-queues the same way: push, then done. */
    CHECK_EQ(workq_push(q, 8, 85), 0);
    workq_done(q);
    CHECK_EQ(workq_outstanding(q), 1);
    CHECK_EQ(workq_trypop(q, &it), 1);
    CHECK_EQ(it.idx, 8);
    CHECK_EQ(it.off, 85);
    workq_done(q);
    CHECK_EQ(workq_outstanding(q), 0);
    CHECK_EQ(workq_trypop(q, &it), 0);
    CHECK_EQ(workq_pop(q, &it), 0);

    /* After abort, trypop gives nothing even if items are queued. */
    CHECK_EQ(workq_push(q, 1, 0), 0);
    workq_abort(q);
    CHECK_EQ(workq_aborted(q), 1);
    CHECK_EQ(workq_trypop(q, &it), 0);
    CHECK_EQ(workq_pop(q, &it), 0);
    workq_destroy(q);
}

/* ---- Re-queue semantics with 4 workers ----------------------------------- */

#define RQ_ITEMS   64
#define RQ_WORKERS 4
#define RQ_SLOW    (RQ_ITEMS - 1)       /* the last item: fails once, slowly */

static struct {
    workq_t   *q;
    atomic_int attempts[RQ_ITEMS];
    atomic_int completed[RQ_ITEMS];
    atomic_int completed_total;
    atomic_int early_exits;             /* a worker saw "complete" too soon */
    atomic_int bad_offset;
} rq;

static void *rq_worker(void *arg)
{
    work_item_t it;

    (void)arg;
    while (workq_pop(rq.q, &it) == 1) {
        int attempt = atomic_fetch_add(&rq.attempts[it.idx], 1);

        if (attempt == 0 && (it.idx % 8 == 3 || it.idx == RQ_SLOW)) {
            /*
             * Simulated connection failure. The slow item is popped last
             * and holds on long enough that every other worker finds the
             * queue empty and blocks in pop(); they must stay there (not
             * exit), because this worker is about to re-queue.
             */
            if (it.idx == RQ_SLOW)
                sleep_ms(100);
            if (workq_push(rq.q, it.idx, 1000 + it.idx) != 0)
                atomic_fetch_add(&rq.bad_offset, 1);
            workq_done(rq.q);
            continue;
        }
        /* A retry must carry the re-queued offset. */
        if (attempt > 0 && it.off != 1000 + it.idx)
            atomic_fetch_add(&rq.bad_offset, 1);
        atomic_fetch_add(&rq.completed[it.idx], 1);
        atomic_fetch_add(&rq.completed_total, 1);
        workq_done(rq.q);
    }
    /* pop() returned 0: complete. Every item must really be done. */
    if (atomic_load(&rq.completed_total) != RQ_ITEMS || workq_outstanding(rq.q) != 0)
        atomic_fetch_add(&rq.early_exits, 1);
    return NULL;
}

static void test_requeue(void)
{
    pthread_t th[RQ_WORKERS];
    int exactly_once = 1, attempts_ok = 1;

    memset(&rq, 0, sizeof(rq));
    rq.q = workq_create(RQ_ITEMS);
    CHECK(rq.q != NULL);
    for (uint32_t i = 0; i < RQ_ITEMS; i++)
        CHECK_EQ(workq_push(rq.q, i, 0), 0);
    for (int t = 0; t < RQ_WORKERS; t++)
        CHECK_EQ(pthread_create(&th[t], NULL, rq_worker, NULL), 0);
    for (int t = 0; t < RQ_WORKERS; t++)
        CHECK_EQ(pthread_join(th[t], NULL), 0);

    for (int i = 0; i < RQ_ITEMS; i++) {
        int want = (i % 8 == 3 || i == RQ_SLOW) ? 2 : 1;
        if (atomic_load(&rq.completed[i]) != 1)
            exactly_once = 0;
        if (atomic_load(&rq.attempts[i]) != want)
            attempts_ok = 0;
    }
    CHECK(exactly_once);
    CHECK(attempts_ok);
    CHECK_EQ(atomic_load(&rq.completed_total), RQ_ITEMS);
    CHECK_EQ(atomic_load(&rq.early_exits), 0);
    CHECK_EQ(atomic_load(&rq.bad_offset), 0);
    CHECK_EQ(workq_outstanding(rq.q), 0);
    workq_destroy(rq.q);
}

/* ---- abort wakes blocked poppers -------------------------------------- */

static struct {
    workq_t   *q;
    atomic_int returned;
    atomic_int got_item;
} ab;

static void *ab_waiter(void *arg)
{
    work_item_t it;

    (void)arg;
    if (workq_pop(ab.q, &it) != 0)
        atomic_fetch_add(&ab.got_item, 1);
    atomic_fetch_add(&ab.returned, 1);
    return NULL;
}

static void test_abort(void)
{
    pthread_t th[3];
    work_item_t it;

    memset(&ab, 0, sizeof(ab));
    ab.q = workq_create(4);
    CHECK_EQ(workq_push(ab.q, 5, 0), 0);
    CHECK_EQ(workq_pop(ab.q, &it), 1);     /* main is now "busy": others must wait */
    for (int t = 0; t < 3; t++)
        CHECK_EQ(pthread_create(&th[t], NULL, ab_waiter, NULL), 0);
    sleep_ms(100);
    CHECK_EQ(atomic_load(&ab.returned), 0);  /* blocked, not exited */
    workq_abort(ab.q);
    for (int t = 0; t < 3; t++)
        CHECK_EQ(pthread_join(th[t], NULL), 0);
    CHECK_EQ(atomic_load(&ab.returned), 3);
    CHECK_EQ(atomic_load(&ab.got_item), 0);
    CHECK_EQ(workq_aborted(ab.q), 1);
    /* A re-queue after abort is stored but never handed out. */
    CHECK_EQ(workq_push(ab.q, 5, 10), 0);
    workq_done(ab.q);
    CHECK_EQ(workq_pop(ab.q, &it), 0);
    workq_destroy(ab.q);
}

/* ---- Stress: 16 threads, 10000 items (TSan) --------------------------- */

#define ST_ITEMS   10000
#define ST_THREADS 16

static struct {
    workq_t   *q;
    atomic_int completed[ST_ITEMS];
    atomic_int attempts[ST_ITEMS];
    atomic_int early_exits;
    atomic_int push_fail;
} st;

static void *st_worker(void *arg)
{
    long tid = (long)arg;
    work_item_t it;

    for (;;) {
        int got;

        /* Some threads behave like the poll engine and try first. */
        if ((tid & 3) == 0 && workq_trypop(st.q, &it) == 1)
            got = 1;
        else
            got = workq_pop(st.q, &it);
        if (!got)
            break;
        /* Every 7th item fails on its first two attempts. */
        if (it.idx % 7 == 0 && atomic_fetch_add(&st.attempts[it.idx], 1) < 2) {
            if (workq_push(st.q, it.idx, it.off + 1) != 0)
                atomic_fetch_add(&st.push_fail, 1);
        } else {
            atomic_fetch_add(&st.completed[it.idx], 1);
        }
        workq_done(st.q);
    }
    if (workq_outstanding(st.q) != 0)
        atomic_fetch_add(&st.early_exits, 1);
    return NULL;
}

static void test_stress(void)
{
    pthread_t th[ST_THREADS];
    int exactly_once = 1;

    memset(&st, 0, sizeof(st));
    st.q = workq_create(ST_ITEMS);
    CHECK(st.q != NULL);
    for (uint32_t i = 0; i < ST_ITEMS; i++)
        CHECK_EQ(workq_push(st.q, i, 0), 0);
    for (long t = 0; t < ST_THREADS; t++)
        CHECK_EQ(pthread_create(&th[t], NULL, st_worker, (void *)t), 0);
    for (int t = 0; t < ST_THREADS; t++)
        CHECK_EQ(pthread_join(th[t], NULL), 0);
    for (int i = 0; i < ST_ITEMS; i++)
        if (atomic_load(&st.completed[i]) != 1)
            exactly_once = 0;
    CHECK(exactly_once);
    CHECK_EQ(atomic_load(&st.early_exits), 0);
    CHECK_EQ(atomic_load(&st.push_fail), 0);
    CHECK_EQ(workq_outstanding(st.q), 0);
    workq_destroy(st.q);
}

int main(void)
{
    test_fifo_and_capacity();
    test_trypop_and_outstanding();
    test_requeue();
    test_abort();
    test_stress();
    UNIT_DONE();
}
