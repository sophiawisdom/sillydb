//
//  nvme_key.c
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#include "nvme_key.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <time.h>
#include <stdio.h>

#define INITIAL_CAPACITY (100)

// HELPER FUNCTIONS

static void acq_lock(struct db_state *db) {
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

static void release_lock(struct db_state *db) {
    db -> lock = 0;
}

bool search_for_key(struct db_state *db, db_data search_key, struct ram_stored_key *found_key) {
    bool found = false;
    struct ram_stored_key found_key;
    for (int i = 0; i < db -> num_key_entries; i++) {
        struct ram_stored_key key = keys[i];
        if (key.key_length != search_key.length || key.key_hash != key_hash) {
            continue;
        }

        void *key_str = db -> key_vla[key.key_offset];
        if (memcmp(search_key.data, key_str, key.key_length) == 0) {
            *found_key = key;
            return true;
        }
    }
    return false;
}

unsigned short hash_key(db_data key) {
    unsigned short hash = 0x5555; // 0b01010101
    unsigned short *short_data = key.data;
    for (int i = 0; i < key.length>>1; i++) {
        hash ^= short_data[i];
    }
    return hash;
}

unsigned long long get_time_µs() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 10000000) + tv.tv_usec;
}

struct flush_writes_state {
    TAILQ_HEAD(write_cb_head, write_cb_state) write_callback_queue;
}

void flush_writes_cb(void *arg, enum write_err err) {
    acq_lock(state -> db);

    struct flush_writes_state *callback_state = arg;
    struct write_cb_state *write_callback;
    TAILQ_FOREACH(write_callback, &callback_state -> write_callback_queue, link) {
        db -> writes_in_flight--;
        db -> keys[write_callback -> key_index].flags &= (255-DATA_FLAG_INCOMPLETE); // set incomplete flag to false
        db -> keys[write_callback -> key_index].data_loc = write_callback -> data_loc;
        
        if (write_callback -> callback != NULL) {
            write_callback -> callback(write_callback -> cb_arg, write_err);
        }
    }
    
    // TODO: figure out how to free both a) the callback queue and b) all the callbacks inside it.
    // free(write_callback);

    release_lock(db);
}

unsigned long long callback_ssd_size(struct write_cb_state *write_callback) {
    return write_callback -> value.length + write_callback -> key.length + sizeof(struct ssd_header);
}

// MUST HAVE LOCK TO CALL THIS FUNCTION
unsigned long long calc_write_bytes_queued(struct db_state *db) {
    unsigned long long write_bytes_queued = 0;
    struct write_cb_state *write_callback;
    TAILQ_FOREACH(write_callback, &state -> write_callback_queue, link) {
        write_bytes_queued += callback_ssd_size(write_callback);
    }
    return write_bytes_queued;
}

// MUST HAVE LOCK TO CALL THIS FUNCTION
void flush_writes(struct db_state *db) {
    unsigned long long write_bytes_queued = calc_write_bytes_queued(db);
    unsigned long long sectors_to_write = write_bytes_queued/db -> sector_size; // We have 10000 bytes enqueued with a sector length of 4096, so write 2 sectors
    unsigned long long current_sector = db -> current_sector_ssd;
    db -> current_sector_ssd += sectors_to_write;
    unsigned long long write_size = sectors_to_write * db -> sector_size;
    
    struct flush_writes_state *flush_writes_cb_state = malloc(sizeof(struct flush_writes_state));

    void *data = calloc(1, db -> sector_size * write_size); // what *specifically* we are writing in this write.
    // TODO: dma_alloc this ^ ? Would eliminate a needless copy. spdk_nvme_ctrlr_map_cmb or spdk_zmalloc
    struct write_cb_state *write_callback;
    unsigned long long data_bytes_written = 0;
    TAILQ_FOREACH(write_callback, &state -> write_callback_queue, link) {
        unsigned long long size = callback_ssd_size;
        // TODO: remove each callback as we encounter it, if we don't consume the whole buffer note that somehow (synthetic_buffer?) and re-enqueue it. Ideally, we would just `break` after that but otherwise just continue through.
        // TODO: if we don't consume the whole buffer and make a "synthetic" buffer, make sure to make a fake write_db so the data can be deallocated at the end.
        // Also, enqueue every callback in flush_writes_cb_state -> write_callback_queue and also write the ssd_loc of every callback out.
    }
    
    nvme_issue_write(db, current_sector_ssd, sectors_to_write, data, flush_writes_cb, flush_writes_cb_state);
}

// MUST HAVE LOCK TO CALL THIS FUNCTION
bool should_flush_writes(struct db_state *db) {
    // Check if there are enough bytes enqueued to fill a sector.
    // TODO: possibly store the current number of write bytes enqueued and update it when callbacks are enqueued.
    // Current behavior could get ~O(n^2) with hundreds of tiny callbacks.
    if (calc_write_bytes_queued(db) >= db -> sector_size) {
        return true;
    }
    
    // Check if oldest write in queue has been waiting more than 1ms.
    unsigned long long cur_t = get_time_µs();
    unsigned long long elapsed_µs = cur_t - TAILQ_LAST(&db -> write_callback_queue) -> clock_time_enqueued;
    if (elapsed_µs > 1000) { // more than 1ms
        return true;
    }

    return false;
}

// PUBLIC API

void *create_db() {
    struct db_state *state = malloc(sizeof(struct key_state));

    state -> lock = 0;

    state -> num_key_entries = 0;
    state -> key_capacity = INITIAL_CAPACITY;
    state -> keys = calloc(sizeof(struct ram_stored_key), INITIAL_CAPACITY);
    
    state -> key_vla_capacity = INITIAL_CAPACITY*20;
    state -> key_vla_length = 0;
    state -> key_vla = calloc(sizeof(char), state -> key_vla_capacity);

    if (initialize(state) != 0) {
        free(state -> keys);
        free(state -> key_vla);
        free(state);
        return NULL;
    }
    
    state -> write_cache_size = 4096; // guesstimate of sector size. One day we need to actually determine for this is.
    state -> write_cache = calloc(state -> write_cache_size, 1);
    
    TAILQ_INIT(&state -> write_callback_queue);
    
    return state;
}

void free_db(void *db) {
    acq_lock(db);
    // TODO: TAILQ_FREE our tail queues
    // make sure all writes have persisted? this shouldn't really happen very much. mostly we expect the process to exit instead.
}

void write_key_data_async(void *opaque, db_data key, db_data value, key_write_cb callback, void *cb_arg) {
    struct db_state *db = opaque;
    acq_lock(db);
    
    // Check if the key exists already, which requires special logic that's not yet implemented.
    struct ram_stored_key prev_key;
    bool found = search_for_key(db, key, &prev_key);
    if (found) {
        release_lock(db);
        if (state -> callback != NULL) {
            write_cb(cb_arg, GENERIC_WRITE_ERROR); // in order to support this we would have to delete the previous key and do a bunch of other work, so not implemented yet.
        }
        return;
    }
    
    // Get key index in list, possibly resizing db -> keys
    long long key_idx = db -> num_key_entries++;
    if (key_idx >= db -> key_capacity) {
        db -> key_capacity *= 2;
        db -> keys = realloc(db -> keys, db -> key_capacity);
        printf("resizing key area\n");
    }

    // Write the key itself to the VLA, possibly resizing db -> key_vla
    long long current_key_vla_offset = db -> key_vla_length;
    if (current_key_vla_offset > db -> key_vla_capacity) { // resize VLA
        db -> key_vla_capacity *= 2;
        db -> key_vla = realloc(db -> key_vla, db -> key_vla_capacity);
        printf("resizing VLA to %d\n", db -> key_vla_capacity);
    }
    memcpy(db -> key_vla + current_key_vla_offset, key.data, key.length);
    db -> key_vla_length += key.length;

    struct ram_stored_key ram_key;
    ram_key.key_length = key.key_length;
    ram_key.key_hash = hash_key(key);
    ram_key.key_offset = current_key_vla_offset;
    ram_key.data_length = value.length;
    ram_key.flags = DATA_FLAG_INCOMPLETE;
    ram_key.data_loc = -1;
    db -> keys[key_idx] = ram_key;

    db -> writes_in_flight++;

    struct write_cb_state *callback_arg = malloc(sizeof(struct write_db_state)); // FREED BY THE WRITE CALLBACK
    callback_arg -> db = db;
    callback_arg -> callback = callback;
    callback_arg -> cb_arg = cb_arg;
    callback_arg -> index = key_idx;
    callback_arg -> key = key;
    callback_arg -> value = value;
    callback_arg -> clock_time_enqueued = get_time_µs();
    TAILQ_INSERT_HEAD(&db -> write_callbacks, callback_arg, link); // Append the callback to a linked list of write callbacks

    if (should_flush_writes(db)) {
        flush_writes(db);
    }
    release_lock(db);
}

void read_key_data_async(void *opaque, db_data read_key, read_cb callback, void *cb_arg) {
    struct db_state *db = opaque;
    acq_lock(db); // ACQUIRE LOCK

    struct ram_stored_key found_key;
    bool found = search_for_key(db, read_key, &found_key);
    if (!found) { // couldn't find key
        release_lock(db); // RELEASE LOCK
        if (callback != NULL) {
            callback(cb_arg, KEY_NOT_FOUND, (db_data){.data=NULL, .length=0});
        }
        return;
    }
    if (found_key.flags & DATA_FLAG_INCOMPLETE) { // The key is in the process of being written, so it's effectively not there.
        release_lock(db);
        if (callback != NULL) {
            callback(cb_arg, KEY_NOT_FOUND, (db_data){.data=NULL, .length=0});
        }
        return;
    }
    
    db -> reads_in_flight++;
    // DO SOME CALLBACK
    release_lock(db);
}

void poll(void *opaque) {
    struct db_state *db = opaque;
    acq_lock(db); // ACQUIRE LOCK
    
    if (should_flush_writes(db)) {
        flush_writes(db);
    }
    
    unsigned int begin_clock = clock();
    while ((clock() - begin_clock) < 5000) { // poll for completions for up to 5ms
        spdk_nvme_qpair_process_completions(db->main_namespace->qpair, 0);
    }
    
    release_lock(db);
}

// gnu++ standard + a -f