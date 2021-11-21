#include "db_interface.h"
#include "nvme_init.h"

#include "spdk/stdinc.h"
#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/nvme_zns.h"
#include "spdk/env.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct ctrlr_entry {
    struct spdk_nvme_ctrlr        *ctrlr;
    TAILQ_ENTRY(ctrlr_entry)    link;
    char                name[1024];
};

struct ns_entry {
    struct spdk_nvme_ctrlr    *ctrlr;
    struct spdk_nvme_ns    *ns;
    TAILQ_ENTRY(ns_entry)    link;
    struct spdk_nvme_qpair    *qpair;
};

struct state {
    _Atomic int lock;
    
    TAILQ_ENTRY(ctrlr_entry) g_controllers;
    TAILQ_ENTRY(ns_entry) g_namespaces;
};

void *create_db() {
    struct state *initial_state = malloc(sizeof(struct state));
    initial_state -> lock = 0;
    
    initial_state -> g_namespaces = TAILQ_HEAD_INITIALIZER(initial_state -> g_namespaces);
    initial_state -> g_controllers = TAILQ_HEAD_INITIALIZER(initial_state -> g_controllers);
    
    initialize();

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
