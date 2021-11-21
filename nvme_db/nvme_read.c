//
//  nvme_read.c
//  
//
//  Created by Sophia Wisdom on 11/20/21.
//

#include "nvme_read.h"
#include "nvme_internal.h"
#include "db_interface.h"

#include "spdk/nvme.h"

struct read_sequence {
    struct ns_entry    *ns_entry;
    unsigned        using_cmb_io;
    struct state *state;
    
    int is_completed;
    int length;
    void *data;
};

static void
read_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
    struct read_sequence *sequence = arg;

    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        spdk_nvme_qpair_print_completion(sequence->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        fprintf(stderr, "Read I/O failed, aborting run\n");
        sequence->is_completed = 2;
        return;
    }

    /*
     * The read I/O has completed.  Print the contents of the
     *  buffer, free the buffer, then mark the sequence as
     *  completed.  This will trigger the hello_world() function
     *  to exit its polling loop.
     */
    sequence->is_completed = 1;
}

struct read_response nvme_sector_read_sync(struct state *state, int sector) {
    struct read_sequence sequence = malloc(sizeof(read_sequence));
    sequence -> data = spdk_zmalloc(0x1000, 0x1000, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

    int rc = spdk_nvme_ns_cmd_read(
        state->main_namespace->ns,
        state->main_namespace->qpair,
        sequence -> data,
        0, /* LBA start */
        1, /* number of LBAs */
        read_complete,
        (void *)sequence,
        0
    );

    if (rc != 0) {
        fprintf(stderr, "starting read I/O failed\n");
        free(sequence);
        return (struct read_response){.err=rc, .data=( db_data){.length=0, .data=NULL}};
    }
    
    while (!sequence.is_completed) {
        spdk_nvme_qpair_process_completions(state->main_namespace->qpair, 0);
    }

    free(sequence);
    return (struct read_response){.err=0, .data=(db_data){.data=sequence -> data, .length=0x1000}};
}
