/* client.c: Message Queue Client */

#include "mq/client.h"
#include "mq/logging.h"
#include "mq/socket.h"
#include "mq/string.h"

/* Internal Constants */

#define SENTINEL "SHUTDOWN"

/* Internal Prototypes */

void * mq_pusher(void *);
void * mq_puller(void *);

/* External Functions */

/**
 * Create Message Queue withs specified name, host, and port.
 * @param   name        Name of client's queue.
 * @param   host        Address of server.
 * @param   port        Port of server.
 * @return  Newly allocated Message Queue structure.
 */
MessageQueue * mq_create(const char *name, const char *host, const char *port) {
   MessageQueue *mq = calloc(1, sizeof(MessageQueue));
   if (mq) {
       strcpy(mq->name, name);
       strcpy(mq->host, host);
       strcpy(mq->port, port);
       mq->outgoing = queue_create(); 
       mq->incoming = queue_create();
       mq->shutdown = false; // Set mq_shutdown to false to prevent early stopping
       
       // Initialize semaphore lock
       sem_init(&mq->Lock, 0, 1);

   } 
    return mq;
}

/**
 * Delete Message Queue structure (and internal resources).
 * @param   mq      Message Queue structure.
 */
void mq_delete(MessageQueue *mq) {
    if (mq) {
        if (mq->outgoing) queue_delete(mq->outgoing);
        if (mq->incoming) queue_delete(mq->incoming);
        free(mq);
    }
}

/**
 * Publish one message to topic (by placing new Request in outgoing queue).
 * @param   mq      Message Queue structure.
 * @param   topic   Topic to publish to.
 * @param   body    Message body to publish.
 */
void mq_publish(MessageQueue *mq, const char *topic, const char *body) {
    const char *method = "PUT";
    char uri [BUFSIZ];
    sprintf(uri, "/topic/%s", topic);
    Request *r = request_create(method, uri, body);
    //if (r)
    queue_push(mq->outgoing, r);
}

/**
 * Retrieve one message (by taking Request from incoming queue).
 * @param   mq      Message Queue structure.
 * @return  Newly allocated message body (must be freed).
 */
char * mq_retrieve(MessageQueue *mq) {
    Request *r = queue_pop(mq->incoming);
    //if (!r) return NULL; 

    char *body = NULL;
    if (streq(SENTINEL, r->body)) {
        request_delete(r);
        return NULL;
    }
        
    body = strdup(r->body);
    request_delete(r);
    return body;
}

/**
 * Subscribe to specified topic.
 * @param   mq      Message Queue structure.
 * @param   topic   Topic string to subscribe to.
 **/
void mq_subscribe(MessageQueue *mq, const char *topic) {
    const char *method = "PUT";
    char uri [BUFSIZ];
    sprintf(uri, "/subscription/%s/%s", mq->name, topic);
    Request *r = request_create(method, uri, NULL);
    //if (r) 
    queue_push(mq->outgoing, r); 
}

/**
 * Unubscribe to specified topic.
 * @param   mq      Message Queue structure.
 * @param   topic   Topic string to unsubscribe from.
 **/
void mq_unsubscribe(MessageQueue *mq, const char *topic) {
    const char *method = "DELETE";
    char uri [BUFSIZ]; 
    sprintf(uri, "/subscription/%s/%s", mq->name, topic);
    Request *r = request_create(method, uri, NULL);
    //if (r) 
    queue_push(mq->outgoing, r);

}

/**
 * Start running the background threads:
 *  1. First thread should continuously send requests from outgoing queue.
 *  2. Second thread should continuously receive reqeusts to incoming queue.
 * @param   mq      Message Queue structure.
 */
void mq_start(MessageQueue *mq) {
    thread_create(&mq->thread1, NULL, mq_pusher, (void*)(mq));
    thread_create(&mq->thread2, NULL, mq_puller, (void*)(mq));

    // Subscribing to shutdown topic
    mq_subscribe(mq, SENTINEL);
}

/**
 * Stop the message queue client by setting shutdown attribute and sending
 * sentinel messages
 * @param   mq      Message Queue structure.
 */
void mq_stop(MessageQueue *mq) {
    mq_publish(mq, SENTINEL, SENTINEL);

    // Must use semaphore as a lock to prevent race condition between mq_stop changing mq->shutdown and pusher and puller reading it
    sem_wait(&mq->Lock);
    mq->shutdown = true;
    sem_post(&mq->Lock);
    thread_join(mq->thread1, NULL);
    thread_join(mq->thread2, NULL);
}

/**
 * Returns whether or not the message queue should be shutdown.
 * @param   mq      Message Queue structure.
 */
bool mq_shutdown(MessageQueue *mq) {
    // Must use semaphore again to prevent same race condition as in mq_stop
    sem_wait(&mq->Lock);
    bool temp = mq->shutdown;
    sem_post(&mq->Lock);
    return temp;
}

/* Internal Functions */

/**
 * Pusher thread takes messages from outgoing queue and sends them to server.
 **/
void * mq_pusher(void *arg) {
    MessageQueue *mq = (MessageQueue*)arg;
    while (!mq_shutdown(mq)) {

        // Pop request for outgoing
        Request *r = queue_pop(mq->outgoing); 
        //if (!r) continue;

        // Connect to the server --> socket connect or socket dial
        FILE *fs = socket_connect(mq->host, mq->port);
        if(!fs) continue;

        // Write the request
        request_write(r, fs);
        request_delete(r); 

        // Read the response
        char buffer[BUFSIZ];
        while (fgets(buffer, BUFSIZ, fs));
        fclose(fs);
    }
    return NULL;
}

/**
 * Puller thread requests new messages from server and then puts them in
 * incoming queue.
 **/
void * mq_puller(void *arg) {
    MessageQueue *mq = (MessageQueue*)arg;
    while (!mq_shutdown(mq)) {

        // Connect to the server
        FILE *fs = socket_connect(mq->host, mq->port);
        if(!fs) continue;

        // Create a request
        char uri [BUFSIZ]; 
        sprintf(uri, "/queue/%s", mq->name);
        Request *r = request_create("GET", uri, NULL);
        //if (!r) continue;

        // Write the request
        request_write(r, fs);
        request_delete(r); 

        // Read the response
        char *method = "GET";
        char buffer[BUFSIZ];
        if (!fgets(buffer, BUFSIZ, fs) || !strstr(buffer, "200 OK")) {
            fclose(fs);
            continue;
        }

        long length = 0;
        while (fgets(buffer, BUFSIZ, fs) && !streq(buffer, "\r\n")) {
            sscanf(buffer, "Content-Length: %ld", &length);
        }
        char body[BUFSIZ];
        fgets(body,BUFSIZ, fs);
        fclose(fs);

        // Push the response into the incoming queue
        Request *response = request_create(method, uri, body);
        //if (response) 
        queue_push(mq->incoming, response); 
    }
    return NULL;
}
/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */
