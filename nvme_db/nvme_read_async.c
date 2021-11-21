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
