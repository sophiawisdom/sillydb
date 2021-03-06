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
    // Lock is acquired by the caller of spdk_nvme_qpair_process_completions.

    enum write_err error = WRITE_SUCCESSFUL;

    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        // TODO: fix this. go through each write callback and return an error.
        spdk_nvme_qpair_print_completion(callback_state->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        error = WRITE_IO_ERROR;
    }

#ifdef DEBUG
    printf("Got write callback\n");
#endif

    struct write_cb_state *prev_callback = NULL;
    struct write_cb_state *write_callback;
    TAILQ_FOREACH(write_callback, &callback_state -> write_callback_queue, link) {
        free(prev_callback);
        prev_callback = write_callback;
        if (error != WRITE_SUCCESSFUL) {
            printf("Not setting incomplete false due to IO error\n");
            // TODO: what to do here when we get an IO error? remove the key is the only thing.
        } else {
            db -> keys[write_callback -> key_index].flags &= (255-DATA_FLAG_INCOMPLETE); // set incomplete flag to false
#ifdef DEBUG
            printf("Setting complete for key %.16s\n", (char *)db -> key_vla+db -> keys[write_callback -> key_index].key_offset);
#endif
        }
        write_callback -> callback(write_callback -> cb_arg, error);
    }
    free(prev_callback);

    db -> writes_in_flight--;

    TAILQ_INIT(&callback_state -> write_callback_queue); // believe this frees it? unclear...

    release_lock(db);
    spdk_free(callback_state -> buf);
    free(callback_state);
}

// MUST HAVE LOCK TO CALL THIS FUNCTION
void flush_writes(struct db_state *db) {
    unsigned long long write_bytes_queued = calc_write_bytes_queued(db);
    // TODO: fail with error if there's not enough space on the SSD.
#ifdef DEBUG
    printf("write bytes queued: %d. current_sector_bytes is %d\n", write_bytes_queued, db -> current_sector_bytes);
#endif
    double bytes_to_write = db -> current_sector_bytes + write_bytes_queued;
    unsigned long long current_sector = db -> current_sector_ssd; // sector we're going to write to
    db -> current_sector_ssd += (db -> current_sector_bytes + write_bytes_queued)/db -> sector_size;
#ifdef DEBUG
    printf("current_sector_bytes is %lld, write_bytes_queued %lld, increasing current sector by %d to %d\n",
    db -> current_sector_bytes, write_bytes_queued, (db -> current_sector_bytes + write_bytes_queued)/db -> sector_size, db -> current_sector_ssd);
#endif
    unsigned long long sectors_to_write = ceil(bytes_to_write/((double)db -> sector_size)); // e.g. We have 10000 bytes enqueued with a sector length of 4096, so write 3 sectors with 1 partially written
    sectors_to_write = sectors_to_write == 0 ? 1 : sectors_to_write; // at min 1
    unsigned long long write_size = sectors_to_write * db -> sector_size;

    struct flush_writes_state *flush_writes_cb_state = malloc(sizeof(struct flush_writes_state));
    flush_writes_cb_state -> db = db;
    // transfer the callback queue to the callback, it will be written to when that's completed.
    flush_writes_cb_state -> buf = spdk_zmalloc(write_size, db -> sector_size, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    flush_writes_cb_state -> ns_entry = db -> main_namespace;
    TAILQ_INIT(&flush_writes_cb_state -> write_callback_queue);

    unsigned long long buf_bytes_written = db -> current_sector_bytes;
    if (db -> current_sector_bytes) {
        memcpy(flush_writes_cb_state -> buf, db -> current_sector_data, db -> current_sector_bytes);
#ifdef DEBUG
        printf("Copying first %lld bytes into flush writes cb state: %.64s\n", db -> current_sector_bytes, db -> current_sector_data);
#endif
    }
    while (!TAILQ_EMPTY(&db -> write_callback_queue)) {
        struct write_cb_state *write_callback = TAILQ_FIRST(&db -> write_callback_queue);

        db -> keys[write_callback -> key_index].data_loc = buf_bytes_written + current_sector * db -> sector_size;
#ifdef DEBUG
        printf("Flushing key %.16s to %lld\n", (char *)db -> key_vla+db -> keys[write_callback -> key_index].key_offset, db -> keys[write_callback -> key_index].data_loc);
#endif

        // Write header
        struct ssd_header header = (struct ssd_header){
            .key_length = write_callback -> key.length,
            .data_length = write_callback -> value.length,
            .flags = 0
        };
        memcpy(flush_writes_cb_state -> buf + buf_bytes_written, &header, sizeof(header));
        buf_bytes_written += sizeof(header);

        // Write key
        memcpy(flush_writes_cb_state -> buf + buf_bytes_written, write_callback -> key.data, write_callback -> key.length);
        buf_bytes_written += write_callback -> key.length;

        // Write data
        memcpy(flush_writes_cb_state -> buf + buf_bytes_written, write_callback -> value.data, write_callback -> value.length);
        buf_bytes_written += write_callback -> value.length;

#ifdef DEBUG
        unsigned long long bytes_written = sizeof(header) + write_callback -> key.length + write_callback -> value.length;
        unsigned long long original_sector = db -> keys[write_callback -> key_index].data_loc / db -> sector_size;
        unsigned long long original_sector_bytes = db -> keys[write_callback -> key_index].data_loc % db -> sector_size;
        unsigned long long end_sector = (db -> keys[write_callback -> key_index].data_loc + bytes_written)/db -> sector_size;
        unsigned long long end_sector_bytes = (db -> keys[write_callback -> key_index].data_loc + bytes_written)%db -> sector_size;
        printf("Wrote %lld bytes from sector %lld byte %lld to sector %lld byte %lld for key %.16s\n",
        bytes_written, original_sector, original_sector_bytes, end_sector, end_sector_bytes, (char *)write_callback -> key.data);
#endif

        TAILQ_REMOVE(&db -> write_callback_queue, write_callback, link);
        TAILQ_INSERT_TAIL(&flush_writes_cb_state -> write_callback_queue, write_callback, link);
    }

    if (buf_bytes_written < write_size) { // We're writing to 9.5 sectors, so fill out the last .5 with 0s and store the first .5
        // if write_size is 10000 bytes and we end up writing 9400 bytes, we want to 0 out the last 600 and store the first 400.
#ifdef DEBUG
        printf("setting %d bytes from %lld to 'a'\n", write_size - buf_bytes_written, buf_bytes_written + (current_sector*db -> sector_size));
#endif
        db -> current_sector_bytes = db -> sector_size - (write_size - buf_bytes_written);
        memset(flush_writes_cb_state -> buf + buf_bytes_written, 0, write_size-buf_bytes_written);
        memcpy(db -> current_sector_data, flush_writes_cb_state -> buf + (write_size - db -> sector_size), db -> current_sector_bytes);
    } else {
        db -> current_sector_bytes = 0;
        memset(db -> current_sector_data, 'b', db -> sector_size);
    }

    db -> current_sector_ssd += 1;
    db -> current_sector_bytes = 0;

    db -> writes_in_flight++;

#ifdef DEBUG
    printf("Wrote %lld bytes. Set current_sector_bytes to %lld\n", buf_bytes_written, db -> current_sector_bytes);
#endif

#ifdef DEBUG
    printf("Writing %d sectors of data to sector %d\n", sectors_to_write, current_sector);
#endif
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

static void write_zeroes_cb(void *arg, const struct spdk_nvme_cpl *completion) {
    spdk_free(arg);
    if (spdk_nvme_cpl_is_error(completion)) {
        printf("got error while writing zeroes!\n");
    } else {
        printf("successfully wrote zeroes\n");
    }
}


void write_zeroes(struct db_state *db, int start_block, int num_blocks) {
    // In theory we could use e.g. write_uncorrectable, or write_zeroes, but the SSD i've been testing on doesn't support those,
    // so instead just actually write zeroes. This is useful for testing.
    void *buf = spdk_zmalloc(db -> sector_size * num_blocks, db -> sector_size, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    memset(buf, 'c', db -> sector_size * num_blocks);
    spdk_nvme_ns_cmd_write(
        db -> main_namespace -> ns,
        db -> main_namespace -> qpair,
        buf,
        start_block,
        num_blocks,
        write_zeroes_cb,
        buf,
        0
    );
}

static void flush_cb(void *arg,  const struct spdk_nvme_cpl *completion) {
    struct db_state *db = arg;
    db -> flushes_in_flight--;
    if (spdk_nvme_cpl_is_error(completion)) {
        // TODO: fix this. go through each write callback and return an error.
        // spdk_nvme_qpair_print_completion(callback_state->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "flush failed: I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        printf("completed flush with error\n");
    } else {
#ifdef DEBUG
        printf("completed flush successfully\n");
#endif
    }
}

void flush_commands(void *opaque) {
    struct db_state *db = opaque;
    db -> flushes_in_flight++;
    spdk_nvme_ns_cmd_flush(
        db -> main_namespace -> ns,
        db -> main_namespace -> qpair,
        flush_cb,
        db
    );
}

void wait_for_zero_writes(void *opaque) {
    struct db_state *db = opaque;
    while (db -> writes_in_flight) {
        usleep(1000);
        poll_db(db);
    }
}
