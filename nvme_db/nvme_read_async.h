//
//  nvme_read_async.h
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#ifndef nvme_read_async_h
#define nvme_read_async_h

#include <stdio.h>

typedef void (*read_cb)(void *, void *, int);

int issue_nvme_sector_read(struct state *state, int sector);

void poll_for_reads(struct state* state);

#endif /* nvme_read_async_h */
