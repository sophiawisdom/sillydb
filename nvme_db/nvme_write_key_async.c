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
    printf("flush_writes_cb called\n");
    acq_lock(callback_state -> db);
    printf("Acquired lock\n");

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
        // db -> keys[write_callback -> key_index].data_loc = write_callback -> ssd_loc;
        
        write_callback -> callback(write_callback -> cb_arg, WRITE_SUCCESSFUL);
    }
    
    // TODO: figure out how to free both a) the callback queue and b) all the callbacks inside it.
    // free(write_callback);

    release_lock(db);

    spdk_free(callback_state -> buf);
}

static short halfbyte(char halfbyte) {
    if (halfbyte < 10) {
        return 48 + halfbyte;
    }
    return 97 + halfbyte-10;
}

short byte_to_hex(unsigned char byte) {
    short firstletter = halfbyte(byte & 15);
    short secondletter = halfbyte((byte & 240)<<4);
    return (secondletter<<8) + firstletter;
}

// MUST HAVE LOCK TO CALL THIS FUNCTION
void flush_writes(struct db_state *db) {
    unsigned long long write_bytes_queued = calc_write_bytes_queued(db);
    printf("write bytes queued: %d\n", write_bytes_queued);
    unsigned long long data_write_begin = db -> current_sector_bytes + (db -> sector_size * db -> current_sector_ssd);
    double bytes_to_write = db -> current_sector_bytes + write_bytes_queued;
    unsigned long long sectors_to_write = ceil(bytes_to_write/((double)db -> sector_size)); // e.g. We have 10000 bytes enqueued with a sector length of 4096, so write 3 sectors with 1 partially written
    unsigned long long current_sector = db -> current_sector_ssd; // sector we're going to write to
    db -> current_sector_ssd += sectors_to_write;
    sectors_to_write = sectors_to_write == 0 ? 1 : sectors_to_write; // at min 1
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

    unsigned long long buf_bytes_written = db -> current_sector_bytes;
    if (db -> current_sector_bytes) {
        memcpy(&flush_writes_cb_state -> buf, db -> current_sector_data, db -> current_sector_bytes);
    }
    while (!TAILQ_EMPTY(&db -> write_callback_queue)) {
        struct write_cb_state *write_callback = TAILQ_FIRST(&db -> write_callback_queue);
        unsigned long long size = callback_ssd_size(write_callback);

        db -> keys[write_callback -> key_index].data_loc = data_write_begin + buf_bytes_written;
        printf("Writing data to %lld\n", db -> keys[write_callback -> key_index].data_loc);

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
        memcpy(&flush_writes_cb_state -> buf[buf_bytes_written], &header, sizeof(header));
        buf_bytes_written += sizeof(header);

        // Write key
        memcpy(&flush_writes_cb_state -> buf[buf_bytes_written], write_callback -> key.data, write_callback -> key.length);
        buf_bytes_written += write_callback -> key.length;

        // Write data
        memcpy(&flush_writes_cb_state -> buf[buf_bytes_written], write_callback -> value.data, write_callback -> value.length);
        buf_bytes_written += write_callback -> value.length;

        printf("removing callback %p from %p to %p\n", write_callback, &db -> write_callback_queue, &flush_writes_cb_state -> write_callback_queue);
        TAILQ_REMOVE(&db -> write_callback_queue, write_callback, link);
        TAILQ_INSERT_TAIL(&flush_writes_cb_state -> write_callback_queue, write_callback, link);
    }

    if (buf_bytes_written < write_size) { // We're writing to 9.5 sectors, so fill out the last .5 with 0s and store the first .5
        // if write_size is 10000 bytes and we end up writing 9400 bytes, we want to 0 out the last 600 and store the first 400.
        db -> current_sector_bytes = db -> sector_size - (write_size - buf_bytes_written);
        memcpy(db -> current_sector_data, &flush_writes_cb_state -> buf[write_size - db -> sector_size], db -> current_sector_bytes);
        memset(&flush_writes_cb_state -> buf[buf_bytes_written], 0, write_size-buf_bytes_written);
    }

    printf("Wrote %lld bytes. buf is \"%s\"\n", buf_bytes_written, flush_writes_cb_state -> buf);
    short *d = calloc(buf_bytes_written+1, 2);
    char *char_buf = flush_writes_cb_state -> buf;
    for (int i = 0; i < buf_bytes_written; i++) {
        d[i] = byte_to_hex(char_buf[i]);
    }
    d[buf_bytes_written] = 0;
    printf("buf: %s\n", (char *)d);

    print_keylist(db);

    printf("Writing %d sectors of data to sector %d %p %s\n", sectors_to_write, current_sector, flush_writes_cb_state -> buf, flush_writes_cb_state -> buf);

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
    void *buf = spdk_zmalloc(db -> sector_size * num_blocks, db -> sector_size, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    memset(buf, 0, db -> sector_size * num_blocks);
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
