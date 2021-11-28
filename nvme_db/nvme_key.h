//
//  nvme_key.h
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#ifndef nvme_key_h
#define nvme_key_h

#include "db_interface.h"
#include <stdio.h>
#include <stdatomic.h>
#include "spdk/env.h"

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

#define DATA_FLAG_ZSTD 1
#define DATA_FLAG_INCOMPLETE 2

__attribute__((packed))
struct ram_stored_key {
    unsigned short key_length;
    unsigned short key_hash; // for speed. we have a shitty architecture where you have to search through the entire list of keys to find any key.
    unsigned int key_offset;

    char flags; // contains flags, notably DATA_FLAG_INCOMPLETE which indicates whether the data is yet to be written to disk.
    unsigned int data_length;
    long long data_loc; // location within ssd.
};

// header for all nvme data
__attribute__((packed))
struct ssd_header {
    unsigned short key_length;
    unsigned int data_length;
    int flags; // followed by key_length bytes of key and data_length bytes of data.
    // TOCONSIDER: unsigned int padding_length?Can be used to not cross big block boundaries.
};

#define WRITE_CB_FLAG_PARTIALLY_WRITTEN 1
#define WRITE_CB_FLAG_PERSISTED 2

// 92 bytes as stands, and created for every write. Could it be smaller?
struct write_cb_state {
    struct db_state *db;
    
    void *cb_arg;
    key_write_cb callback;
    
    db_data key;
    db_data value;

    int key_index; // TODO: if we implement deletes this has to become more complicated. Perhaps deletes can't occur while a key is in flight?
    
    unsigned long long clock_time_enqueued; // clock() time at which this write was enqueued. After a certain amount of time, or when we have enough writes to fill a sector, this will be unqueued.
    
    unsigned long long ssd_loc; // written in flush_writes and read when the callback returns.

    int flag; // WRITE_CB_FLAG_PARTIALLY_WRITTEN
    unsigned long long bytes_written; // for partially written cbs.

    TAILQ_ENTRY(write_cb_state)    link;
};

struct db_state {
    _Atomic int lock;

    long long num_key_entries;
    long long key_capacity;
    struct ram_stored_key *keys;
    // Each key is stored here in fixed-width form for enumeration. But the keys themselves are variable-width, so we have key_vla to store the keys themselves. `key_offset` in `struct ram_stored_key` refers to an offset in `key_vla`.

    long long key_vla_length; // end point at which bytes should be written in key_vla
    long long key_vla_capacity; // capacity of key_vla
    void *key_vla;

    int writes_in_flight;
    int reads_in_flight;

    unsigned int sector_size; // https://spdk.io/doc/nvme_8h.html#a0d24c0b2b0b2a22b0c0af2ca2e157e04
    unsigned long long num_sectors; // https://spdk.io/doc/nvme_8h.html#a7c522609f730db26f66e7f5b6b3501e0
    unsigned int max_transfer_size; // https://spdk.io/doc/nvme_8h.html#ac2aac85501f13bff557d3a224d8ec156

    // Writes are queued until there are sufficiently many to write a whole sector, or else for a few ms.
    TAILQ_HEAD(write_cb_head, write_cb_state) write_callback_queue;
    
    unsigned long long current_sector_ssd; // how many sectors are we into the current SSD (i.e. where will the next value be stored).
    void *current_sector_data;
    unsigned short current_sector_bytes; // How many bytes are we into the current sector? Mostly this should be 0.

    TAILQ_HEAD(control_head, ctrlr_entry) g_controllers;
    TAILQ_HEAD(namespace_head, ns_entry) g_namespaces;
    struct ns_entry *main_namespace;
};


/*
// You can pass NULL for the callbacks if you wish
void read_key_data_async(void *db, db_data key, key_read_cb callback, void *cb_arg);

void write_key_data_async(void *db, db_data key, db_data object, key_write_cb callback, void *cb_arg);

void poll_db(void *opaque);
void free_db(void *db);
*/

// TODO: add debug assert in all functions that require lock to make sure they have the lock.
void acq_lock(struct db_state *db);
void release_lock(struct db_state *db);

unsigned long long calc_write_bytes_queued(struct db_state *db);

#endif /* nvme_key_h */
