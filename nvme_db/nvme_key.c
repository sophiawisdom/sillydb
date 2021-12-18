//
//  nvme_key.c
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#include "nvme_key.h"
#include "nvme_key_init.h"
#include "nvme_read_key_async.h"
#include "nvme_write_key_async.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INITIAL_CAPACITY (100)

// HELPER FUNCTIONS

void acq_lock(struct db_state *db) {
    // C atomics have weird semantics. Here's what this compiles to on x86:
    /**
     acq_lock:                               # @acq_lock
             mov     ecx, 1
     .LBB0_1:                                # =>This Inner Loop Header: Depth=1
             xor     eax, eax
             lock            cmpxchg dword ptr [rdi], ecx
             jne     .LBB0_1
             ret
     */
    // So to start, we set ECX to 1 (the third parameter to atomic_compare_exchange_strong).
    // Then we enter our loop body. We set EAX to 0 (success) and then issue a `lock cmpxchg`.
    // https://www.felixcloutier.com/x86/cmpxchg
    // This compares EAX (0) with the destination operand ([rdi], which is db -> lock).
    // If the two values are equal (i.e. the lock is 0) load ecx (1) into the destination operand (the lock) atomically, and
    // set the zero flag. If they are not, clear the zero flag. `jne .LBB0_1` returns to the top of our loop
    // if the zero flag is not set. So we continually try to get the lock.
    //
    // This way there is no race between making sure the lock is 0 and then acquiring the lock.
    int success = 0;
    atomic_compare_exchange_strong(&db -> lock, &success, 1);
    while (success != 0) {
        success = 0;
        atomic_compare_exchange_strong(&db -> lock, &success, 1);
    }
}

void release_lock(struct db_state *db) {
    db -> lock = 0;
}

static unsigned int hash_key(db_data key) {
    unsigned int hash = 0x55555555; // 0b01010101
    unsigned int *int_data = key.data;
    for (int i = 0; i < key.length>>2; i++) {
        hash ^= int_data[i];
    }
    return hash;
}

// MUST HAVE LOCK TO CALL THIS FUNCTION
static bool search_for_key(struct db_state *db, db_data search_key, struct ram_stored_key *found_key, bool insert) {
    unsigned int key_hash = hash_key(search_key);

    if (db -> nodes[0].key_idx == -1) { // If there are no nodes, create the first node.
        if (insert) {
            db -> nodes[0] = (struct key_node){.key_idx=db -> num_key_entries, .left_idx = -1, .right_idx = -1};
        }
        return false;
    }

    int node_idx = 0;
    while (1) {
        struct node_key cur_node = db -> nodes[node_idx];

        // We use three different levels of comparison to try to reduce the odds of a memcmp().
        bool left = cur_key.key_hash < key_hash;
        if (cur_key.key_hash == key_hash) {
            struct ram_stored_key cur_key = db -> keys[cur_node.key_idx];
            left = cur_key.key_length < search_key.length;
            if (cur_key.key_length == search_key.length) {
                int resp = memcmp(search_key.data, db -> key_vla + cur_key.key_offset, cur_key.key_length);
                left = resp == -1
                if (resp == 0) {
                    // We've got a match
                    *found_key = cur_key;
                    return true;
                }
            }
        }

        // It's not equal, so we try to find a lower value on the tree.
        if (left) {
            if (cur_node.left_idx != -1) { // Recurse lower
                node_idx = cur_node.left_idx;
                continue;
            } else if (insert) { // Couldn't find it - if insert, this is where we inesrt.
                db -> nodes[node_idx].left_idx = db -> num_nodes;
                db -> nodes[db -> num_nodes++] = (struct key_node){.key_idx = db -> num_key_entries, .left_idx=-1, .right_idx=-1};
            }
        } else {
            if (cur_node.right_idx != -1) {
                node_idx = cur_node.right_idx;
                continue;
            } else if (insert) {
                db -> nodes[node_idx].right_idx = db -> num_nodes;
                db -> nodes[db -> num_nodes++] = (struct key_node){.key_idx = db -> num_key_entries, .left_idx=-1, .right_idx=-1};
            }
        }
        return false;
    }
}

static unsigned long long get_time_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 10000000) + tv.tv_usec;
}

unsigned long long callback_ssd_size(struct write_cb_state *write_callback) {
    return write_callback -> value.length + write_callback -> key.length + sizeof(struct ssd_header);
}

// MUST HAVE LOCK TO CALL THIS FUNCTION
unsigned long long calc_write_bytes_queued(struct db_state *db) {
    unsigned long long write_bytes_queued = 0;
    struct write_cb_state *write_callback;
    TAILQ_FOREACH(write_callback, &db -> write_callback_queue, link) {
        write_bytes_queued += callback_ssd_size(write_callback);
    }
    return write_bytes_queued;
}

int compar(struct ram_stored_key *first, struct ram_stored_key *second) {
    if (first -> key_hash < second -> key_hash) {
        return -1;
    } else if (first -> key_hash == second -> key_hash) {
        return 0;
    } else {
        return 1;
    }
}

// MUST HAVE LOCK TO CALL THIS FUNCTION
static bool should_flush_writes(struct db_state *db) {
    if (TAILQ_EMPTY(&db -> write_callback_queue) || db -> writes_in_flight || db -> flushes_in_flight) {
        return false;
    }

    // Check if there are enough bytes enqueued to fill a sector.
    // TODO: possibly store the current number of write bytes enqueued and update it when callbacks are enqueued.
    // Current behavior could get ~O(n^2) with hundreds of tiny callbacks.
    if (calc_write_bytes_queued(db) >= db -> sector_size) {
        return true;
    }
    
    // Check if oldest write in queue has been waiting more than 1ms.
    unsigned long long cur_t = get_time_us();
    struct write_cb_state *last = TAILQ_FIRST(&db -> write_callback_queue);
    unsigned long long elapsed_us = cur_t - last -> clock_time_enqueued;
    if (elapsed_us > 1000) { // more than 1ms
        return true;
    }

    return false;
}

static void print_key(struct db_state *db, struct ram_stored_key key) {
#ifdef DEBUG
            printf("Key has length %d, hash %d, vla offset %d, flags %d, data length %d data loc %llu\n",
        key.key_length, key.key_hash, key.key_offset, key.flags, key.data_length, key.data_loc);
        // printf("key itself is %s\n", db -> key_vla + key.key_offset); // todo print only till end of key
#endif
}

// PUBLIC API

void *create_db() {
    struct db_state *state = malloc(sizeof(struct db_state));

    state -> lock = 0;

    state -> num_key_entries = 0;
    state -> key_capacity = INITIAL_CAPACITY;
    state -> keys = calloc(sizeof(struct ram_stored_key), INITIAL_CAPACITY);
    state -> keys_at_last_sort = 0;

    state -> nodes = malloc(sizeof(struct key_node) * INITIAL_CAPACITY);
    state -> nodes[0].key_idx = 0;
    state -> num_nodes = 0;
    state -> node_capacity = INITIAL_CAPACITY;

    state -> key_vla_capacity = INITIAL_CAPACITY*20;
    state -> key_vla_length = 0;
    state -> key_vla = calloc(sizeof(char), state -> key_vla_capacity);

    state -> writes_in_flight = 0;
    state -> reads_in_flight = 0;
    state -> flushes_in_flight = 0;

    if (initialize(state) != 0) {
        free(state -> keys);
        free(state -> key_vla);
        free(state);
        return NULL;
    }

    state -> current_sector_ssd = 0;
    state -> current_sector_bytes = 0;
    state -> current_sector_data = calloc(1, state -> sector_size);

    TAILQ_INIT(&state -> write_callback_queue);

    write_zeroes(state, 0, 50000);
    
    return state;
}

void free_db(void *opaque) {
    struct db_state *db = opaque;
    acq_lock(db);

    free(db -> keys);
    free(db -> key_vla);
    free(db -> current_sector_data);
    free(TAILQ_FIRST(&db -> g_controllers));
    free(db);
    // TODO: TAILQ_FREE our tail queues
    // make sure all writes have persisted? this shouldn't really happen very much. mostly we expect the process to exit instead.
}

void write_value_async(void *opaque, db_data key, db_data value, key_write_cb callback, void *cb_arg) {
    struct db_state *db = opaque;
    acq_lock(db);

    enum write_err err = WRITE_SUCCESSFUL;
    if (key.length == 0) {
        err = KEY_TOO_SHORT_ERROR;
    } else if (key.length > (1<<16)) {
        err = KEY_TOO_LONG_ERROR;
    } else if (value.length == 0) {
        err = VALUE_TOO_SHORT_ERROR;
    } else if (value.length >= (1<<32)) {
        err = VALUE_TOO_LONG_ERROR;
    }
    if (err != WRITE_SUCCESSFUL) {
        release_lock(db);
        callback(cb_arg, err);
    }

    // Check if the key exists already, which requires special logic that's not yet implemented.
    struct ram_stored_key prev_key;
    if (db -> node_capacity <= db -> num_nodes) {
        db -> node_capacity *= 2;
        db -> nodes = realloc(db -> nodes, db -> node_capacity * sizeof(struct key_node));
        printf("resizing node area\n");
    }
    bool found = search_for_key(db, key, &prev_key, true); // insert key to nodes if not found
    if (found) {
        release_lock(db);
        callback(cb_arg, GENERIC_WRITE_ERROR); // in order to support this we would have to delete the previous key and do a bunch of other work, so not implemented yet.
        return;
    }

    // Get key index in list, possibly resizing db -> keys
    long long key_idx = db -> num_key_entries++;
    if (key_idx >= db -> key_capacity) {
        db -> key_capacity *= 2;
        db -> keys = realloc(db -> keys, db -> key_capacity * sizeof(struct ram_stored_key));
        printf("resizing key area\n");
    }

    // Write the key itself to the VLA, possibly resizing db -> key_vla
    long long current_key_vla_offset = db -> key_vla_length;
    if ((key.length + current_key_vla_offset) > db -> key_vla_capacity) { // resize VLA
        db -> key_vla_capacity *= 2;
        db -> key_vla = realloc(db -> key_vla, db -> key_vla_capacity);
        printf("resizing VLA to %lld\n", db -> key_vla_capacity);
    }
    memcpy(db -> key_vla + current_key_vla_offset, key.data, key.length);
    db -> key_vla_length += key.length;

    struct ram_stored_key ram_key;
    ram_key.key_length = key.length;
    ram_key.key_hash = hash_key(key);
    ram_key.key_offset = current_key_vla_offset;
    ram_key.data_length = value.length;
    ram_key.flags = DATA_FLAG_INCOMPLETE;
    ram_key.data_loc = -1;
    db -> keys[key_idx] = ram_key;

    struct write_cb_state *callback_arg = malloc(sizeof(struct write_cb_state)); // FREED BY THE WRITE CALLBACK
    callback_arg -> db = db;
    callback_arg -> callback = callback;
    callback_arg -> cb_arg = cb_arg;
    callback_arg -> key_index = key_idx;
    callback_arg -> key = key;
    callback_arg -> value = value;
    callback_arg -> bytes_written = 0;
    callback_arg -> flag = 0;
    callback_arg -> clock_time_enqueued = get_time_us();
    TAILQ_INSERT_TAIL(&db -> write_callback_queue, callback_arg, link); // Append the callback to a linked list of write callbacks

    if (should_flush_writes(db)) {
        flush_writes(db);
    }

    // print_keylist(db);
    release_lock(db);
}

void read_value_async(void *opaque, db_data read_key, key_read_cb callback, void *cb_arg) {
    struct db_state *db = opaque;
    acq_lock(db); // ACQUIRE LOCK

    struct ram_stored_key found_key;
    bool found = search_for_key(db, read_key, &found_key, false);
    if (!found) { // couldn't find key
        release_lock(db); // RELEASE LOCK
        callback(cb_arg, KEY_NOT_FOUND, (db_data){.data=NULL, .length=0});
        return;
    }
    if (found_key.flags & DATA_FLAG_INCOMPLETE) { // The key is in the process of being written, so it's effectively not there.
        release_lock(db);
        callback(cb_arg, KEY_NOT_FOUND, (db_data){.data=NULL, .length=0});
        printf("Returning can't found for key because data not yet written\n");
        return;
    }

#ifdef DEBUG
    printf("Trying to read key: %.16s at %llu\n", read_key.data, found_key.data_loc);
    print_key(db, found_key);
#endif

    db -> reads_in_flight++;
    issue_nvme_read(db, found_key, callback, cb_arg);
    release_lock(db);
}

void poll_db(void *opaque) {
    struct db_state *db = opaque;
    acq_lock(db); // ACQUIRE LOCK
    
    if (should_flush_writes(db)) {
#ifdef DEBUG
        printf("flushing writes\n");
#endif
        flush_writes(db);
    }

    release_lock(db); // have to release lock here so the callback can acquire lock
    spdk_nvme_qpair_process_completions(db->main_namespace->qpair, 0);
}

void print_keylist(struct db_state *db) {
    // acq_lock(db);

    for (int i = 0; i < db -> num_key_entries; i++) {
        struct ram_stored_key key = db -> keys[i];
        print_key(db, key);
    }

    // release_lock(db);
}

// gnu++ standard + a -f
