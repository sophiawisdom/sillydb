//
//  nvme_write_async.c
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#include "nvme_write_async.h"

void poll_for_writes(struct state* state) {
    
}

int nvme_issue_write(struct state *state, int data_length, void *data, write_cb callback, void *cb_arg) {
    int index = state -> num_entries++;

    size_t  sz;
    struct write_sequence *sequence = malloc(sizeof(struct write_sequence));
    sequence -> using_cmb_io = 1;
    printf("about to DMA allocate some data\n");
    sequence -> buf = spdk_nvme_ctrlr_map_cmb(state->main_namespace->ctrlr, &sz);
    printf("sz is %ld\n", sz);
    if (sequence -> buf == NULL || sz < 0x1000) {
        sequence -> using_cmb_io = 0;
        sequence -> buf = spdk_zmalloc(0x1000, 0x1000, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    }
    if (sequence -> buf == NULL) {
        printf("ERROR: write buffer allocation failed\n");
        return -1;
    }
    if (sequence -> using_cmb_io) {
        printf("INFO: using controller memory buffer for IO\n");
    } else {
        printf("INFO: using host memory buffer for IO\n");
    }
    sequence -> is_completed = 0;
    sequence -> ns_entry = state->main_namespace;
    
    printf("about to check if reset zone\n");
    /*
     * If the namespace is a Zoned Namespace, rather than a regular
     * NVM namespace, we need to reset the first zone, before we
     * write to it. This not needed for regular NVM namespaces.
     */
    if (spdk_nvme_ns_get_csi(state->main_namespace->ns) == SPDK_NVME_CSI_ZNS) {
        printf("about to reset the first zone\n");
        reset_zone_and_wait_for_completion(sequence);
    }
    
    
    printf("about to write to sequence buf\n");
    *((int *)sequence -> buf) = data_length;
    memcpy(sequence -> buf+4, data, data_length);
    
    printf("about to issue write\n");
    int rc = spdk_nvme_ns_cmd_write(state->main_namespace->ns, state->main_namespace->qpair, sequence -> buf,
                    index, /* LBA start */
                    1, /* number of LBAs */
                    write_complete, sequence, 0);
    if (rc != 0) {
        fprintf(stderr, "starting write I/O failed\n");
        return -1;
    }
}
