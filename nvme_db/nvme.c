#include "db_interface.h"
#include "nvme_internal.h"
#include "nvme_init.h"
#include "nvme_cleanup.h"
#include "nvme_write.h"
#include "nvme_read.h"

#include "spdk/stdinc.h"
#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/nvme_zns.h"
#include "spdk/env.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

void *create_db(void) {
    struct state *initial_state = malloc(sizeof(struct state));
    initial_state -> lock = 0;
    initial_state -> num_entries = 0;
    
    printf("about to initialize!\n");
    int ret = initialize(initial_state);
    printf("retval from initialize is %d\n", ret);
    if (ret == 2) {
        cleanup(initial_state);
    }
    if (ret > 0) {
        return NULL;
    }

    return initial_state;
}

void free_db(void *opaque) {
    struct state *db = opaque;
    cleanup(db);
    free(db);
}

int append_object_sync(void *opaque, db_data object) {
    if (object.length > 4092) { // sector size - 4 bytes for length.
        return -1;
    }
    struct state *db = opaque;
    while (db -> lock != 0) {}
    db -> lock = 1;

    int index = nvme_append(db, object.length, object.data); // writes are negative values, but just passed through.
    
    db -> lock = 0;
    return index;
}

void append_object_async(void *db, db_data object, write_cb callback, void *cb_arg) {
    int resp = nvme_issue_sector_write(db, object.length, object.data, write_db, cb_arg);
    if (resp < 0) { // encountered an error issuing the write
        callback(cb_arg, resp);
    }
}

struct read_response read_object_sync(void *opaque, int index) {
    struct state *db = opaque;
    while (db -> lock != 0) {}
    db -> lock = 1; // ACQ LOCK
    
    struct read_response resp;

    if (index > db -> num_entries) {
        resp.data = (db_data){.length = 0, .data = NULL};
        resp.err = 1;
        db -> lock = 0; // RELEASE LOCK
        printf("got request for index beyond capabilities: %d\n", index);
        return resp;
    }

    resp = nvme_sector_read_sync(db, index);
    printf("Got response from nvme_sector_read_sync: %d\n", resp.err);

    db -> lock = 0; // RELEASE LOCK
    return resp;
}

/*

void poll(void *db) {
    poll_for_writes(db);
    poll_for_reads(db);
}
 
 */
