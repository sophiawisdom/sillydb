//
//  nvme_read.h
//  
//
//  Created by Sophia Wisdom on 11/20/21.
//

#ifndef nvme_read_h
#define nvme_read_h

#include "nvme_internal.h"

struct read_response nvme_sector_read_sync(struct state *state, int sector);

#endif /* nvme_read_h */
