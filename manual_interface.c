#include "db_interface.h"
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

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
            } else {
                write_req = 1;
            }
            printf("write_req set to %d\n", write_req);
            continue;
        }

        if (write_req) {
            db_data data = {.length = length, .data = buf};
            data.length -= 1; // cut out newline
            printf("appending of length %d\n", data.length);
            append_object(db, data);
        } else {
            printf("reading object at index %d\n", atoi(buf));
            struct read_response resp = read_object_sync(db, atoi(buf));
            printf("err is %d, length is %d, data is \"%s\"\n", resp.err, resp.data.length, resp.data.data);
        }
        memset(buf, 0, 4096);
    }
}
