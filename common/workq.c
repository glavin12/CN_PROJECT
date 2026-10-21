/*
 * workq.c - the Enhanced client's shared work queue (design section 6.2).
 * The contract is in workq.h.
 *
 * A fixed ring buffer of (file index, offset) items plus a count of busy
 * workers, under one mutex and one condition variable.
 *
 * Why "busy" exists: an empty queue does not mean the topic is complete. A
 * worker holding an item may still lose its connection and push the item
 * back. If idle workers exited as soon as the queue was empty, that re-queued
 * file would be left with fewer (or no) connections to fetch it. So pop()
 * waits while the queue is empty but some worker is busy, and reports
 * "complete" only when the queue is empty AND nobody is busy: then no one is
 * left who could add work.
 *
 * Why the ring never overflows: count + busy never exceeds the number of
 * items pushed initially, because an item is re-queued only by the worker
 * that popped it (which is busy until workq_done()). With capacity = file
 * count, a re-queue always fits.
 */
#include "workq.h"

#include <pthread.h>
#include <stdlib.h>

struct workq {
    pthread_mutex_t mu;
    pthread_cond_t  cv;        /* "an item, completion or abort may have happened" */
    work_item_t    *items;     /* ring buffer of `cap` items (NULL if cap == 0) */
    uint32_t        cap;
    uint32_t        head;      /* index of the oldest item */
    uint32_t        count;     /* items queued */
    uint32_t        busy;      /* items popped and not yet workq_done() */
    int             aborted;
};

workq_t *workq_create(uint32_t capacity)
{
    workq_t *q = calloc(1, sizeof(*q));

    if (q == NULL)
        return NULL;
    /* capacity 0 is a valid empty topic: no buffer, pop() returns 0 at once. */
    if (capacity > 0) {
        q->items = calloc(capacity, sizeof(*q->items));
        if (q->items == NULL) {
            free(q);
            return NULL;
        }
    }
    q->cap = capacity;
    if (pthread_mutex_init(&q->mu, NULL) != 0) {
        free(q->items);
        free(q);
        return NULL;
    }
    if (pthread_cond_init(&q->cv, NULL) != 0) {
        pthread_mutex_destroy(&q->mu);
        free(q->items);
        free(q);
        return NULL;
    }
    return q;
}

/* Only after every thread using q has been joined. */
void workq_destroy(workq_t *q)
{
    if (q == NULL)
        return;
    pthread_cond_destroy(&q->cv);
    pthread_mutex_destroy(&q->mu);
    free(q->items);
    free(q);
}

int workq_push(workq_t *q, uint32_t idx, uint64_t off)
{
    uint32_t tail;

    pthread_mutex_lock(&q->mu);
    if (q->count == q->cap) {
        pthread_mutex_unlock(&q->mu);
        return -1;
    }
    /* 64-bit sum: head + count cannot wrap even for a huge capacity. */
    tail = (uint32_t)(((uint64_t)q->head + q->count) % q->cap);
    q->items[tail].idx = idx;
    q->items[tail].off = off;
    q->count++;
    /* One new item can satisfy one waiter, so signal (not broadcast). */
    pthread_cond_signal(&q->cv);
    pthread_mutex_unlock(&q->mu);
    return 0;
}

/* Remove the oldest item into *out and count the caller as busy. Caller
 * holds q->mu and has checked count > 0. */
static void take_locked(workq_t *q, work_item_t *out)
{
    *out = q->items[q->head];
    q->head = (q->head + 1) % q->cap;
    q->count--;
    q->busy++;
}

int workq_pop(workq_t *q, work_item_t *out)
{
    pthread_mutex_lock(&q->mu);
    /*
     * Wait while there is nothing to take but a busy worker might still
     * re-queue its item. The while loop re-checks after every wake-up:
     * condition variables may wake spuriously, and another worker may have
     * taken the item first.
     */
    while (q->count == 0 && q->busy > 0 && !q->aborted)
        pthread_cond_wait(&q->cv, &q->mu);
    if (q->aborted) {
        pthread_mutex_unlock(&q->mu);
        return 0;
    }
    if (q->count == 0) {
        /* Empty and nobody busy: the topic is complete. Wake the other
         * waiters too, so that every worker sees it and exits. */
        pthread_cond_broadcast(&q->cv);
        pthread_mutex_unlock(&q->mu);
        return 0;
    }
    take_locked(q, out);
    pthread_mutex_unlock(&q->mu);
    return 1;
}

int workq_trypop(workq_t *q, work_item_t *out)
{
    int got = 0;

    /* The poll() engine must never block here: it owns every socket. */
    pthread_mutex_lock(&q->mu);
    if (!q->aborted && q->count > 0) {
        take_locked(q, out);
        got = 1;
    }
    pthread_mutex_unlock(&q->mu);
    return got;
}

void workq_done(workq_t *q)
{
    pthread_mutex_lock(&q->mu);
    /* Guard against a done() without a pop(): an unsigned wrap would make
     * the queue look busy forever and hang every worker. */
    if (q->busy > 0)
        q->busy--;
    /*
     * Broadcast: if busy just reached 0 with an empty queue, EVERY waiting
     * worker must wake up and see completion; a single signal would leave
     * the others blocked forever.
     */
    pthread_cond_broadcast(&q->cv);
    pthread_mutex_unlock(&q->mu);
}

void workq_abort(workq_t *q)
{
    pthread_mutex_lock(&q->mu);
    q->aborted = 1;
    pthread_cond_broadcast(&q->cv);
    pthread_mutex_unlock(&q->mu);
}

int workq_aborted(workq_t *q)
{
    int a;

    pthread_mutex_lock(&q->mu);
    a = q->aborted;
    pthread_mutex_unlock(&q->mu);
    return a;
}

uint32_t workq_outstanding(workq_t *q)
{
    uint32_t n;

    pthread_mutex_lock(&q->mu);
    n = q->count + q->busy;
    pthread_mutex_unlock(&q->mu);
    return n;
}
