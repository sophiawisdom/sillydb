#include "db_interface.h"
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

void write_callback(void *opaque, int index) {
    printf("Got async write callback at clock %lu. index is %d\n", clock(), index);
}

void read_callback(void *opaque, struct read_response data) {
    printf("Got async read callback at clock %lu. data is length %d, \"%s\"\n", clock(), data.data.length, data.data.data);
}

void write_callback(void *arg, enum write_err error) {
    printf("got back a callback, time %lld err %d\n", clock(), err);
}

void read_callback(void *arg, enum read_err error, db_data response) {
    printf("got read callback, time %lld err %d data %s\n");
}

int main() {
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
            printf("key is %s, please input data:\n");
            char *data_buf = malloc(4096);
            int data_length = read(0, data_buf, 4096);
            db_data value= {.length  = data_length, .data = data_buf};
            data.length -= 1; // cut out newline
            write_value_async(db, key, value, write_callback, NULL);
        } else {
            db_data key = {.length = length, .data = buf};
            read_value_async(db, key, read_callback, NULL);
        }
        memset(buf, 0, 4096);
    }
}
