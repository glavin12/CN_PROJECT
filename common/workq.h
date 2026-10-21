/*
 * workq.h - the Enhanced client's shared work queue (design section 6.2).
 *
 * Holds (file index, start offset) items. K worker threads repeatedly pop an
 * item, FETCH it, and either finish it (workq_done) or, after a connection
 * failure, push it back with the new offset (workq_push) and then call
 * workq_done. A pop blocks while the queue is empty but some worker is still
 * busy (that worker might re-queue its item); it returns 0 only when the queue
 * is empty AND no worker is busy (topic complete) or after workq_abort().
 *
 * Mutex + condition variable inside. FIFO order, so files are fetched roughly
 * in index order and re-queued items go to the back.
 */
#ifndef FDS_WORKQ_H
#define FDS_WORKQ_H

#include <stdint.h>

typedef struct {
    uint32_t idx;      /* file index in the MANIFEST */
    uint64_t off;      /* start offset (size of the existing .part, else 0) */
} work_item_t;

typedef struct workq workq_t;

/* Queue with room for `capacity` items (normally the file count; re-queues
 * never exceed it because an item is only re-queued by the worker holding it). */
workq_t *workq_create(uint32_t capacity);
void     workq_destroy(workq_t *q);

/* Add an item (initial fill or re-queue). Returns 0, or -1 if full. */
int workq_push(workq_t *q, uint32_t idx, uint64_t off);

/* Blocking pop (threads). Returns 1 with *out filled and the caller counted as
 * busy; 0 when complete or aborted. */
int workq_pop(workq_t *q, work_item_t *out);

/* Non-blocking pop (poll client). Returns 1 with *out filled (caller counted as
 * busy), 0 if the queue is empty right now. */
int workq_trypop(workq_t *q, work_item_t *out);

/* The caller finished with the item it popped (after re-queueing it, if it
 * failed). Wakes waiters. */
void workq_done(workq_t *q);

/* Wake everyone; every later pop returns 0 (fatal error / give up). */
void workq_abort(workq_t *q);

/* 1 if aborted. */
int workq_aborted(workq_t *q);

/* Items queued + items busy (0 = topic complete). */
uint32_t workq_outstanding(workq_t *q);

#endif /* FDS_WORKQ_H */
