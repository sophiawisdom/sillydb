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

    key_read_cb callback;
    void *cb_arg;
};

static void
read_complete(struct read_cb_state *arg, const struct spdk_nvme_cpl *completion)
{
    arg -> db -> reads_in_flight--; // don't need to lock here because this key doesn't need a lock
    printf("read has completed! buf: %s\n", (char *) arg -> data);

    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        acq_lock(arg -> db);
        spdk_nvme_qpair_print_completion(sequence->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        release_lock(arg -> db);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        fprintf(stderr, "Read I/O failed, aborting run\n");
        arg -> callback(arg -> cb_arg, READ_IO_ERROR, NULL);
        goto end;
    }

    arg -> callback(arg -> cb_arg, READ_SUCCESSFUL, arg -> data + arg -> key_header_offset);

end:
    spdk_free(arg -> data);
    free(arg);
    return;
}

void issue_nvme_read(struct db_state *db, struct ram_stored_key key, key_read_cb callback, void *cb_arg) {
    unsigned long long key_sector = key.data_loc/db -> sector_size;
    unsigned long long bytes_to_read = key.data_length + key.key_length + sizeof(struct ssd_header);
    unsigned long long sectors_to_read = ceil(((double) bytes_to_read) / ((double) db -> sector_size));
    struct read_cb_state *read_cb = calloc(sizeof(struct read_cb_state), 1);
    read_cb -> db = db;
    read_cb -> callback = callback;
    read_cb -> cb_arg = cb_arg;
    read_cb -> key_header_offset = key.data_loc - (key_sector * db -> sector_size);
    read_cb -> data = spdk_zmalloc(db -> sector_size * sectors_to_read, db -> sector_size, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

    spdk_nvme_ns_cmd_read(
        db -> main_namespace -> ns,
        db -> main_namespace -> qpair,
        read_cb -> data,
        key_sector,
        sectors_to_read,
        read_complete,
        read_cb,
    )
    return 0;
}
