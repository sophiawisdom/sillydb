//
//  nvme_read_async.c
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#include "nvme_read_async.h"

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
