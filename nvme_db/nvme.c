#include "db_interface.h"
#include "nvme_internal.h"
#include "nvme_init.h"

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
    
    int ret = initialize(initial_state);

    return initial_state;
}

void free_db(void *opaque) {
    struct state *db = opaque;
    free(db);
}

int append_object(void *opaque, db_data object) {
    struct state *db = opaque;
    while (db -> lock != 0) {}
    db -> lock = 1;
    
    int index = 5;
    
    db -> lock = 0;
    return index;
}

struct read_response read_object(void *opaque, int index) {
    struct state *db = opaque;
    while (db -> lock != 0) {}
    db -> lock = 1; // ACQ LOCK
    
    struct read_response resp;

    db -> lock = 0;
    return resp;
}
