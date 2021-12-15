//
//  nvme_write_key_async.h
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#ifndef nvme_write_key_async_h
#define nvme_write_key_async_h

#include <stdio.h>
#include "nvme_key.h"

typedef void (*nvme_write_cb)(void *, enum write_err);

void flush_writes(struct db_state *db);

void write_zeroes(struct db_state *db, int start_block, int num_blocks);

#endif /* nvme_write_key_async_h */
