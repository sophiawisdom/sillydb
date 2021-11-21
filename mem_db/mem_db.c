#include "db_interface.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define INITIAL_SIZE (1)

struct state {
    _Atomic int lock;

    int num_entries;
    int capacity;
    int *data_lengths;
    void **datas;
};

void *create_db() {
    struct state *initial_state = malloc(sizeof(struct state));
    initial_state -> num_entries = 0;
    initial_state -> capacity = INITIAL_SIZE;
    initial_state -> data_lengths = malloc(INITIAL_SIZE * sizeof(int));
    initial_state -> datas = malloc(INITIAL_SIZE * sizeof(void *));
    initial_state -> lock = 0;
    return initial_state;
}

void free_db(void *opaque) {
    struct state *db = opaque;
    while (db -> lock != 0) {}
    db -> lock = 1;

    for (int i = 0; i < db -> num_entries; i++) {
        free(db -> datas[i]);
    }
    
    free(db -> data_lengths);
    free(db -> datas);
}

int append_object(void *opaque, db_data object) {
    struct state *db = opaque;
    while (db -> lock != 0) {}
    db -> lock = 1;

    int index = db -> num_entries++;
    if (index >= db -> capacity) {
        int newcap = db -> capacity * 2;
        db -> capacity = newcap;
        db -> datas = realloc(db -> datas, newcap * sizeof(void *));
        db -> data_lengths = realloc(db -> data_lengths, newcap * sizeof(int));
        printf("RESOZE TO CAPACITY %d\n", newcap);
    }
    // printf("writing to index %d. length is %d\n", index, object.length);
    db -> datas[index] = malloc(object.length);
    memcpy(db -> datas[index], object.data, object.length);
    db -> data_lengths[index] = object.length;

    db -> lock = 0;
    return index;
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
        printf("GOT ERR WHEN READING\n");
        return resp;
    }
    
    void *result = malloc(db -> data_lengths[index]);
    memcpy(result, db -> datas[index], db -> data_lengths[index]);
    resp.data = (db_data){.length = db -> data_lengths[index], .data=result};
    resp.err = 0;

    db -> lock = 0;
    return resp;
}
