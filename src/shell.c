/* shell.c
 * Demonstration of multiplexing I/O with a background thread also printing.
 **/

#include "mq/thread.h"
#include "mq/client.h"

#include <ctype.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <termios.h>
#include <unistd.h>

/* Constants */

#define BACKSPACE   127

/* Functions
 * https://viewsourcecode.org/snaptoken/kilo/02.enteringRawMode.html
 */

void usage(const char *program) {
    fprintf(stderr, "Usage: %s [options]\n\n", program);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "    -n  NAME            Name of user\n");
    fprintf(stderr, "    -h  HOST            Host to connect to\n");
    fprintf(stderr, "    -p  PORT            Port to connect to\n");
    fprintf(stderr, "    -H                  Print this help message\n");
}



void toggle_raw_mode() {
    static struct termios OriginalTermios = {0};
    static bool enabled = false;

    if (enabled) {
    	tcsetattr(STDIN_FILENO, TCSAFLUSH, &OriginalTermios);
    } else {
	tcgetattr(STDIN_FILENO, &OriginalTermios);

	atexit(toggle_raw_mode);

	struct termios raw = OriginalTermios;
	raw.c_lflag &= ~(ECHO | ICANON | IEXTEN);
	raw.c_cc[VMIN] = 0;
	raw.c_cc[VTIME] = 1;

	tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw);

    	enabled = true;
    }
}

/* Threads */

void *background_thread(void *arg) {
    MessageQueue *mq = (MessageQueue*)arg;

    while (!mq_shutdown(mq)) {
    	sleep(1);
        char *message = mq_retrieve(mq);
        if (message) {
            printf("\r%-80s\n", message);
            free(message);
        }	
    }
    return NULL;
}

/* Main Execution */

int main(int argc, char *argv[]) {
    toggle_raw_mode();

    char *name = getenv("USER");
    char *host = "localhost";
    char *port = "9540";

    int argind = 1;
    while (argind < argc && strlen(argv[argind]) > 1 && argv[argind][0] == '-') {
        char *arg = argv[argind++];

        switch (arg[1]) {
            case 'n':
                if (argind < argc) name = argv[argind++];
                break;
            case 'h':
                if (argind < argc) host = argv[argind++];
                break;
            case 'p':
                if (argind < argc) port = argv[argind++];
                break;
            case 'H':
                usage(argv[0]);
                return false;
            default:
                usage(argv[0]);
                return false;
        }
    } 
    printf("%s connecting to %s:%s...\n", name, host, port);
    MessageQueue *mq = mq_create(name, host, port);

    // checks if mq_create worked
    if (!mq) return 1;

    // start mq
    mq_start(mq);

    printf("successfully connected!\n");

    /* Background Thread */
    Thread background;
    thread_create(&background, NULL, background_thread, (void*)mq);


    /* Foreground Thread */
    char   input_buffer[BUFSIZ] = "";
    size_t input_index = 0;
    char topic[BUFSIZ];
    strcpy(topic, "chat");

    while (true) {
	char input_char = 0;
    	read(STDIN_FILENO, &input_char, 1);
 
    	if (input_char == '\n') {				// Process commands
            char buffer[BUFSIZ];
            strcpy(buffer, input_buffer);

            char *first_word = strtok(input_buffer, " ");
            char *str_left = strtok(NULL, "");
            
            // exits the shell with "quit" or "exit"
    	    if (strcmp(input_buffer, "quit") == 0 || strcmp(input_buffer, "exit") == 0) {
                printf("\n");
                mq_stop(mq); 
    	    	break;
            
            // subscribes to a topic by "subscribe [TOPIC]"
            } else if (strcmp(first_word, "subscribe") == 0) {
                mq_subscribe(mq, str_left);
		        printf("\r%-80s\n", buffer);

            // unsubscribes to a topic by "unsubscribe [TOPIC]"
            } else if (strcmp(first_word, "unsubscribe") == 0) {
                mq_unsubscribe(mq, str_left);
                printf("\r%-80s\n", buffer);

            // changes the topic user is posting to by "topic [NEW_TOPIC]"
            } else if (strcmp(first_word, "topic") == 0) {
                strcpy(topic, str_left);
                printf("\r%-80s\n", buffer);

            // all other messages are published to the current topic
            } else {
		        printf("\r%-80s\n", buffer);
                mq_publish(mq, topic, buffer);
	        }

            // resets the input_buffer
    	    input_index = 0;
    	    input_buffer[0] = 0;
	    } else if (input_char == BACKSPACE && input_index) {	// Backspace
    	    input_buffer[--input_index] = 0;
	    } else if (!iscntrl(input_char) && input_index < BUFSIZ) {
	        input_buffer[input_index++] = input_char;
	        input_buffer[input_index]   = 0;
	    }

	    printf("\r%-80s", "");			// Erase line (hack!)
	    printf("\r%s", input_buffer);		// Write
	    fflush(stdout);
    }

    thread_join(background, NULL);
    mq_delete(mq);
    return 0;
}
