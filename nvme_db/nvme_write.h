//
//  nvme_write.h
//  
//
//  Created by Sophia Wisdom on 11/20/21.
//

#ifndef nvme_write_h
#define nvme_write_h

#include <stdio.h>
#include "nvme_internal.h"

int nvme_write(struct state *state, int data_length, void *data);

#endif /* nvme_write_h */
