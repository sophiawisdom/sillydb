//
//  nvmd_write_async.h
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#ifndef nvmd_write_async_h
#define nvmd_write_async_h

#include <stdio.h>

int nvme_issue_sector_write(struct state *state, int data_length, void *data, write_cb callback, void *cb_arg);

void poll_for_writes(struct state* state);

#endif /* nvmd_write_async_h */
