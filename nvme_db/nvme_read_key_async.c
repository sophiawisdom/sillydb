//
//  nvme_read_key_async.c
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#include "nvme_read_key_async.h"
#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/nvme_zns.h"
#include "spdk/env.h"

struct read_cb_state {
    struct db_state *db;
    void *data;

    unsigned long long key_header_offset; // offset from beginning of buf to ssd_header
    unsigned long long data_length;

    key_read_cb callback;
    void *cb_arg;
};

static void
read_complete(struct read_cb_state *arg, const struct spdk_nvme_cpl *completion)
{
    arg -> db -> reads_in_flight--; // don't need to lock here because this key doesn't need a lock
#ifdef DEBUG
    printf("read has completed! data_length is %d\n", arg -> data_length);
#endif

    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        // acq_lock(arg -> db);
        spdk_nvme_qpair_print_completion(arg->db->main_namespace->qpair, (struct spdk_nvme_cpl *)completion);
        // release_lock(arg -> db);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        fprintf(stderr, "Read I/O failed, aborting run\n");
        arg -> callback(arg -> cb_arg, READ_IO_ERROR, (db_data){.length=0, .data=NULL});
        goto end;
    }

    arg -> callback(arg -> cb_arg, READ_SUCCESSFUL, (db_data){.length=arg -> data_length, .data=arg -> data + arg -> key_header_offset});

end:
    spdk_free(arg -> data);
    free(arg);
    return;
}

void issue_nvme_read(struct db_state *db, struct ram_stored_key key, key_read_cb callback, void *cb_arg) {
    unsigned long long data_beginning = key.data_loc + sizeof(struct ssd_header) + key.key_length;
#ifdef DEBUG
    printf("data_beginning is %llu, data_loc is %llu\n", data_beginning, key.data_loc);
#endif
    unsigned long long key_sector = data_beginning/db -> sector_size;
    unsigned long long bytes_within_sector = data_beginning - (key_sector * db -> sector_size);
    unsigned long long bytes_to_read = key.data_length;
    unsigned long long sectors_to_read = ceil(((double) bytes_to_read + bytes_within_sector) / ((double) db -> sector_size));
    struct read_cb_state *read_cb = calloc(sizeof(struct read_cb_state), 1);
    read_cb -> db = db;
    read_cb -> callback = callback;
    read_cb -> cb_arg = cb_arg;
    read_cb -> data_length = key.data_length;
    read_cb -> key_header_offset = bytes_within_sector;
    read_cb -> data = spdk_zmalloc(db -> sector_size * sectors_to_read, db -> sector_size, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

    unsigned long long end_sector_bytes = (data_beginning + key.data_length)%db -> sector_size;
#ifdef DEBUG
    printf("reading %lld bytes from sector %lld byte %lld to sector %lld byte %lld for key %.16s\n",
    bytes_to_read, key_sector, bytes_within_sector, key_sector + sectors_to_read - 1, end_sector_bytes, &db -> key_vla[key.key_offset]);
#endif
    spdk_nvme_ns_cmd_read(
        db -> main_namespace -> ns,
        db -> main_namespace -> qpair,
        read_cb -> data,
        key_sector,
        sectors_to_read,
        read_complete, // callback
        read_cb, // callback arg
        0
    );
    return 0;
}

struct dump_cb {
    int fd;
    void *buf;
    int len;
};

static void sector_read_cb(struct dump_cb *state, const struct spdk_nvme_cpl *completion) {
    write(state -> fd, state -> buf, state -> len);
    spdk_free(state -> buf);
    free(state);
    printf("Completed dump successfully\n");
}

void dump_sectors_to_file(void *opaque, int start_lba, int num_lbas) {
    struct db_state *db = opaque;
    char *filename = calloc(500, 1);
    sprintf(filename, "/home/sophiawisdom/dump_%d", time(NULL));
    printf("Writing filename %s\n", filename);
    int fd = open(filename, O_CREAT | O_WRONLY, 0x777);
    free(filename);
    struct dump_cb *dump_state = malloc(sizeof(struct dump_cb));
    dump_state -> fd = fd;
    dump_state -> buf = spdk_zmalloc(num_lbas * db -> sector_size, db -> sector_size, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    dump_state -> len = db -> sector_size * num_lbas;

    spdk_nvme_ns_cmd_read(
        db -> main_namespace -> ns,
        db -> main_namespace -> qpair,
        dump_state -> buf,
        start_lba,
        num_lbas,
        sector_read_cb, // callback
        dump_state, // callback arg
        0
    );
}
