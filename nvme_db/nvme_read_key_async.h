//
//  nvme_read_key_async.h
//  
//
//  Created by Sophia Wisdom on 11/21/21.
//

#ifndef nvme_read_key_async_h
#define nvme_read_key_async_h

#include <stdio.h>
#include "nvme_key.h"

void issue_nvme_read(struct db_state *db, struct ram_stored_key key, key_read_cb callback, void *cb_arg);

// TODO: batch read_keys if we think it could improve performance.

#endif /* nvme_read_key_async_h */
