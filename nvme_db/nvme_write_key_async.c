//
//  nvme_write_key_async.c
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#include "nvme_write_key_async.h"
#include "spdk/nvme.h"

struct nvme_write_cb_state {
    nvme_write_cb callback;
    void *cb_arg;
    struct ns_entry *ns_entry;
};

static void write_complete(void *arg, const struct spdk_nvme_cpl *completion) {
    struct nvme_write_cb_state *cb_state = arg;
    struct ns_entry *ns_entry = cb_state->ns_entry;

    printf("write_complete called\n");

    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        spdk_nvme_qpair_print_completion(cb_state->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        cb_state -> callback(cb_state -> cb_arg, WRITE_IO_ERROR);
        free(cb_state);
        return;
    }

    cb_state -> callback(cb_state -> cb_arg, WRITE_SUCCESSFUL);
    free(cb_state);
}

void nvme_issue_write(struct db_state *db, unsigned long long sector, int sectors_to_write, void *data, nvme_write_cb callback, void *cb_arg) {

    struct nvme_write_cb_state *write_state = malloc(sizeof(struct nvme_write_cb_state));
    write_state -> callback = callback;
    write_state -> cb_arg = cb_arg;
    write_state -> ns_entry = db -> main_namespace -> ns;

    int rc = spdk_nvme_ns_cmd_write(
                    db->main_namespace->ns,
                    db->main_namespace->qpair,
                    data, // data to write
                    sector, // LBA start
                    sectors_to_write, // number of LBAs
                    write_complete, // callback
                    write_state, // callback arg
                    0
    );
}
