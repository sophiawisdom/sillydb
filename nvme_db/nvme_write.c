//
//  nvme_write.c
//  
//
//  Created by Sophia Wisdom on 11/20/21.
//

#include "nvme_write.h"
#include "nvme_internal.h"

#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/nvme_zns.h"
#include "spdk/env.h"

struct write_sequence {
    struct ns_entry    *ns_entry;
    void        *buf;
    unsigned        using_cmb_io;
    struct state *state;
    
    int is_completed; // 1: success. 2: err
};

static void
write_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
    struct write_sequence    *sequence = arg;
    struct ns_entry            *ns_entry = sequence->ns_entry;

    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        spdk_nvme_qpair_print_completion(sequence->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        fprintf(stderr, "Write I/O failed, aborting run\n");
        sequence->is_completed = 2;
        return;
    }
    /*
     * The write I/O has completed.  Free the buffer associated with
     *  the write I/O and allocate a new zeroed buffer for reading
     *  the data back from the NVMe namespace.
     */
    if (sequence->using_cmb_io) {
        spdk_nvme_ctrlr_unmap_cmb(ns_entry->ctrlr);
    } else {
        spdk_free(sequence->buf);
    }
    sequence->is_completed = 1;
}

static void
reset_zone_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
    struct write_sequence *sequence = arg;

    /* Assume the I/O was successful */
    sequence->is_completed = 1;
    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        spdk_nvme_qpair_print_completion(sequence->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        fprintf(stderr, "Reset zone I/O failed, aborting run\n");
        sequence->is_completed = 2;
        exit(1);
    }
}

static void
reset_zone_and_wait_for_completion(struct write_sequence *sequence)
{
    if (spdk_nvme_zns_reset_zone(sequence->ns_entry->ns, sequence->ns_entry->qpair,
                     0, /* starting LBA of the zone to reset */
                     false, /* don't reset all zones */
                     reset_zone_complete,
                     sequence)) {
        fprintf(stderr, "starting reset zone I/O failed\n");
        exit(1);
    }
    while (!sequence->is_completed) {
        spdk_nvme_qpair_process_completions(sequence->ns_entry->qpair, 0);
    }
    sequence->is_completed = 0;
}

int nvme_append(struct state *state, int data_length, void *data) {
    int index = state -> num_entries++;

    /*
     * Use spdk_dma_zmalloc to allocate a 4KB zeroed buffer.  This memory
     * will be pinned, which is required for data buffers used for SPDK NVMe
     * I/O operations.
     */
    size_t  sz;
    struct write_sequence *sequence = malloc(sizeof(struct write_sequence));
    sequence -> using_cmb_io = 1;
    sequence -> buf = spdk_nvme_ctrlr_map_cmb(state->main_namespace->ctrlr, &sz);
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

    /*
     * Print "Hello world!" to sequence -> buf.  We will write this data to LBA
     *  0 on the namespace, and then later read it back into a separate buffer
     *  to demonstrate the full I/O path.
     */
    printf("about to write to sequence buf\n");
    *((int *)sequence -> buf) = data_length;
    memcpy(sequence -> buf+4, data, data_length);

    /*
     * Write the data buffer to LBA 0 of this namespace.  "write_complete" and
     *  "&sequence" are specified as the completion callback function and
     *  argument respectively.  write_complete() will be called with the
     *  value of &sequence as a parameter when the write I/O is completed.
     *  This allows users to potentially specify different completion
     *  callback routines for each I/O, as well as pass a unique handle
     *  as an argument so the application knows which I/O has completed.
     *
     * Note that the SPDK NVMe driver will only check for completions
     *  when the application calls spdk_nvme_qpair_process_completions().
     *  It is the responsibility of the application to trigger the polling
     *  process.
     */
    printf("about to issue write\n");
    int rc = spdk_nvme_ns_cmd_write(state->main_namespace->ns, state->main_namespace->qpair, sequence -> buf,
                    index, /* LBA start */
                    1, /* number of LBAs */
                    write_complete, sequence, 0);
    if (rc != 0) {
        fprintf(stderr, "starting write I/O failed\n");
        return -1;
    }

    /*
     * Poll for completions.  0 here means process all available completions.
     *  In certain usage models, the caller may specify a positive integer
     *  instead of 0 to signify the maximum number of completions it should
     *  process.  This function will never block - if there are no
     *  completions pending on the specified qpair, it will return immediately.
     *
     * When the write I/O completes, write_complete() will submit a new I/O
     *  to read LBA 0 into a separate buffer, specifying read_complete() as its
     *  completion routine.  When the read I/O completes, read_complete() will
     *  print the buffer contents and set sequence -> is_completed = 1.  That will
     *  break this loop and then exit the program.
     */
    printf("starting looping is_completed\n");
    while (!sequence -> is_completed) {
        spdk_nvme_qpair_process_completions(state->main_namespace->qpair, 0);
    }
    
    if (sequence -> is_completed != 2) {
        return -1;
    }

    return index;
}
