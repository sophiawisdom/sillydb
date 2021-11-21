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

int main() {
    void* db = create_db();
    if (db == 0) {
        printf("got err in create_db\n");
        return 1;
    }
    
    bool write_req = 1;
    bool async = 0;
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

            if (buf[2] == 'A') {
                async = 1;
            } else if (buf[2] == 'S') {
                async = 0;
            }

            printf("write_req set to %d, async set to %d\n", write_req, async);
            continue;
        }

        if (write_req) {
            db_data data = {.length = length, .data = buf};
            data.length -= 1; // cut out newline
            printf("appending of length %d\n", data.length);
            if (async) {
                printf("issuing async write at time %lu\n", clock());
                append_object_async(db, data, write_callback, NULL);
            } else {
                append_object_sync(db, data);
            }
        } else {
            int index = atoi(buf);
            printf("reading object at index %d\n", index);
            if (async) {
                printf("issuing async read at time %lu\n", clock());
                read_object_async(db, index, read_callback, NULL);
            } else {
                struct read_response resp = read_object_sync(db, index);
                printf("err is %d, length is %d, data is \"%s\"\n", resp.err, resp.data.length, resp.data.data);
            }
        }
        memset(buf, 0, 4096);
    }
}
