#include "db_interface.h"
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

static void write_callback(void *arg, enum write_err error) {
    printf("got back a callback, time %ld err %d\n", clock(), error);
    free(arg);
}

static void read_callback(void *arg, enum read_err error, db_data response) {
    printf("got read callback, time %ld err %d data is length %d, %s\n", clock(), error, response.length, (char *)response.data);
}

int main(int argc, char **argv) {
    void* db = create_db();
    if (db == 0) {
        printf("got err in create_db\n");
        return 1;
    }
    
    bool write_req = 1;
    char *buf = malloc(4096);
    while (1) {
        int length = read(0, buf, 4096);
        // printf("got buf %s", buf);
        if (buf[0] == 'S') {
            if (buf[1] == 'R') {
                write_req = 0;
            } else if (buf[1] == 'W') {
                write_req = 1;
            }

            printf("write_req set to %d\n", write_req);
            continue;
        }

        if (write_req) {
            db_data key = {.length = length, .data = buf};
            printf("key is %s, please input data:\n", buf);
            char *data_buf = malloc(4096);
            int data_length = read(0, data_buf, 4096);
            db_data value= {.length  = data_length, .data = data_buf};
            value.length -= 1; // cut out newline
            write_value_async(db, key, value, write_callback, data_buf);
        } else {
            db_data key = {.length = length, .data = buf};
            read_value_async(db, key, read_callback, NULL);
        }
        memset(buf, 0, 4096);
    }
}
