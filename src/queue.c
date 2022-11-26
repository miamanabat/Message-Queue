/* queue.c: Concurrent Queue of Requests */

#include "mq/queue.h"

/**
 * Create queue structure.
 * @return  Newly allocated queue structure.
 */
Queue * queue_create() {
    Queue *q = calloc(1, sizeof(Queue));
    if (q) {
        q->head = NULL;
        q->tail = NULL;
        q->size = 0; 
        // initialize produced semaphore
        sem_init(&q->produced, 0, 0);
        sem_init(&q->Lock, 0, 1);
    }
    return q;
}

/**
 * Delete queue structure.
 * @param   q       Queue structure.
 */
void queue_delete(Queue *q) {
    if (q) {
        Request *r = q->head;
        Request *r_next;
        while (r) {
            r_next = r->next;
            request_delete(r);
            r = r_next;
        } 
        free(q);
    }
}

/**
 * Push request to the back of queue.
 * @param   q       Queue structure.
 * @param   r       Request structure.
 */
void queue_push(Queue *q, Request *r) {
    sem_wait(&q->Lock);
    if (q->size == 0) {
        q->head = r;
        q->tail = r;
    } else {
        q->tail->next = r;
        q->tail = r;
    }
    r->next = NULL;
    q->size++;

    sem_post(&q->Lock);
    sem_post(&q->produced);
}

/**
 * Pop request to the front of queue (block until there is something to return).
 * @param   q       Queue structure.
 * @return  Request structure.
 */
Request * queue_pop(Queue *q) {
    sem_wait(&q->produced);
    sem_wait(&q->Lock);
    Request *r = q->head;
    if (r) {
        q->head = q->head->next;
    }
    q->size--;

    sem_post(&q->Lock);
    return r;
}

/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */
