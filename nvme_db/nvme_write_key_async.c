//
//  nvme_write_key_async.c
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#include "nvme_write_key_async.h"
#include "spdk/nvme.h"

struct flush_writes_state {
    TAILQ_HEAD(flush_writes_head, write_cb_state) write_callback_queue;
    struct db_state *db;
    void *buf; // buffer used to write data to SSD, must be freed on flush.
    struct ns_entry *ns_entry;
};

static void flush_writes_cb(void *arg, const struct spdk_nvme_cpl *completion) {
    struct flush_writes_state *callback_state = arg;
    struct db_state *db = callback_state -> db;
    acq_lock(callback_state -> db);

    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        // TODO: fix this. go through each write callback and return an error.
        spdk_nvme_qpair_print_completion(callback_state->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        // callback_state -> callback(callback_state -> cb_arg, WRITE_IO_ERROR);
        // free(callback_state);
        return;
    }

    struct write_cb_state *write_callback;
    TAILQ_FOREACH(write_callback, &callback_state -> write_callback_queue, link) {
        db -> writes_in_flight--;
        db -> keys[write_callback -> key_index].flags &= (255-DATA_FLAG_INCOMPLETE); // set incomplete flag to false
        db -> keys[write_callback -> key_index].data_loc = write_callback -> ssd_loc;
        
        write_callback -> callback(write_callback -> cb_arg, WRITE_SUCCESSFUL);
    }
    
    // TODO: figure out how to free both a) the callback queue and b) all the callbacks inside it.
    // free(write_callback);

    release_lock(db);

    spdk_free(callback_state -> buf);
}

// MUST HAVE LOCK TO CALL THIS FUNCTION
void flush_writes(struct db_state *db) {
    unsigned long long write_bytes_queued = calc_write_bytes_queued(db);
    printf("write bytes queued: %d\n", write_bytes_queued);
    unsigned long long sectors_to_write = write_bytes_queued/db -> sector_size; // e.g. We have 10000 bytes enqueued with a sector length of 4096, so write 2 sectors
    sectors_to_write = sectors_to_write == 0 ? 1 : sectors_to_write; // at min 1
    unsigned long long current_sector = db -> current_sector_ssd; // sector we're going to write to
    db -> current_sector_ssd += sectors_to_write;
    unsigned long long write_size = sectors_to_write * db -> sector_size;
    
    struct flush_writes_state *flush_writes_cb_state = malloc(sizeof(struct flush_writes_state));
    flush_writes_cb_state -> db = db;
    // transfer the callback queue to the callback, it will be written to when that's completed.
    flush_writes_cb_state -> buf = spdk_zmalloc(write_size, db -> sector_size, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    printf("buf is %p\n", flush_writes_cb_state -> buf);
    flush_writes_cb_state -> ns_entry = db -> main_namespace -> ns;
    TAILQ_INIT(&flush_writes_cb_state -> write_callback_queue);

    struct write_cb_state *cb;
    TAILQ_FOREACH(cb, &db -> write_callback_queue, link) {
        printf("cb is %p\n", cb);
    }

    // TAILQ_FOREACH_SAFE 

    unsigned long long buf_bytes_written = 0;
    while (!TAILQ_EMPTY(&db -> write_callback_queue)) {
        struct write_cb_state *write_callback = TAILQ_FIRST(&db -> write_callback_queue);
        unsigned long long size = callback_ssd_size(write_callback);
        unsigned long long bytes_to_skip = write_callback -> bytes_written;
        if (bytes_to_skip > size) {
            printf("GOT INVALID STATE, BYTES_TO_SKIP (%llu) > SIZE (%llu)", bytes_to_skip, size);
            return;
        }
        unsigned long long bytes_to_write = write_size - buf_bytes_written;

        // Two important non-regular scenarios here: 1) it's a 'synthetic' callback where
        // part of the data has already been written to the ssd. 2) we can't write all the
        // data because it would mean writing a partial sector. These can't both happen at
        // the same time.

        // Write header
        struct ssd_header header = (struct ssd_header){
            .key_length = write_callback -> key.length,
            .data_length = write_callback -> value.length,
            .flags = 0
        };
        if (bytes_to_skip < sizeof(header)) {
            void *head = (void *)&header;
            int bytes_to_write = sizeof(header)-bytes_to_skip;
            if (bytes_to_write + buf_bytes_written > write_size) { // max(buf_bytes_written+bytes_to_write, write_size)
                bytes_to_write = write_size - buf_bytes_written;
            }
            memcpy(&flush_writes_cb_state -> buf[buf_bytes_written], head+bytes_to_skip, bytes_to_write);
            buf_bytes_written += bytes_to_write;
            bytes_to_skip = 0;
        } else {
            bytes_to_skip -= sizeof(header);
        }

        // Write key
        if (bytes_to_skip < write_callback -> key.length && buf_bytes_written < write_size) {
            int bytes_to_write = write_callback -> key.length-bytes_to_skip;
            if (bytes_to_write + buf_bytes_written > write_size) { // max(buf_bytes_written+bytes_to_write, write_size)
                bytes_to_write = write_size - buf_bytes_written;
            }
            memcpy(&flush_writes_cb_state -> buf[buf_bytes_written], write_callback -> key.data+bytes_to_skip, bytes_to_write);
            buf_bytes_written += bytes_to_write;
            bytes_to_skip = 0;
        } else {
            bytes_to_skip -= write_callback -> key.length;
        }

        // Write data. Not necessary to not write if bytes_to_skip is too high
        // because if it was too high it would skip the whole thing.
        if (buf_bytes_written < write_size) {
            int bytes_to_write = write_callback -> value.length - bytes_to_skip;
            memcpy(&flush_writes_cb_state -> buf[buf_bytes_written], write_callback -> value.data+bytes_to_skip, bytes_to_write);
            buf_bytes_written += bytes_to_write;
        }

        if (buf_bytes_written == write_size) {
            // make a synthetic callback and then break
            break;
        }
        printf("removing callback %p from %p to %p\n", write_callback, &db -> write_callback_queue, &flush_writes_cb_state -> write_callback_queue);
        TAILQ_INSERT_TAIL(&flush_writes_cb_state -> write_callback_queue, write_callback, link);
        TAILQ_REMOVE(&db -> write_callback_queue, write_callback, link);
    }

    if (buf_bytes_written < write_size) {
        memset(&flush_writes_cb_state -> buf[buf_bytes_written], 0, write_size-buf_bytes_written);
    }


    printf("Writing %d sectors of data tp sector %d %s\n", sectors_to_write, current_sector, flush_writes_cb_state -> buf);

    spdk_nvme_ns_cmd_write(
        db -> main_namespace -> ns,
        db -> main_namespace -> qpair,
        flush_writes_cb_state -> buf,
        current_sector, // LBA start
        sectors_to_write, // number of LBAs
        flush_writes_cb,
        flush_writes_cb_state,
        0 // flags. Worth considering implementing at some point: streams directive for big writes.
    );
    return;
}