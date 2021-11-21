#include "db_interface.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct state {
    _Atomic int lock;
    
    
}

void *create_db() {
    struct state *initial_state = malloc(sizeof(struct state));
    initial_state -> lock = 0;
    
    struct spdk_env_opts opts;

    /*
     * SPDK relies on an abstraction around the local environment
     * named env that handles memory allocation and PCI device operations.
     * This library must be initialized first.
     *
     */
    spdk_env_opts_init(&opts);
    opts.name = "hello_world";
    opts.shm_id = 0;
    if (spdk_env_init(&opts) < 0) {
        fprintf(stderr, "Unable to initialize SPDK env\n");
        return NULL;
    }
    
    if (spdk_vmd_init()) {
        fprintf(stderr, "Failed to initialize VMD."
            " Some NVMe devices can be unavailable.\n");
        return NULL;
    }

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
    
    
    
    db -> lock = 0;
    return index;
}

struct read_response read_object(void *opaque, int index) {
    struct state *db = opaque;
    while (db -> lock != 0) {}
    db -> lock = 1; // ACQ LOCK
    
    

    db -> lock = 0;
    return resp;
}
