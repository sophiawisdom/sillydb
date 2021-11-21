//
//  nvme_cleanup.c
//  
//
//  Created by Sophia Wisdom on 11/20/21.
//

#include "nvme_cleanup.h"
#include "nvme_internal.h"

#include "spdk/nvme.h"

void cleanup(struct state *state)
{
    struct ns_entry *ns_entry, *tmp_ns_entry;
    struct ctrlr_entry *ctrlr_entry, *tmp_ctrlr_entry;
    struct spdk_nvme_detach_ctx *detach_ctx = NULL;

    TAILQ_FOREACH_SAFE(ns_entry, &state -> g_namespaces, link, tmp_ns_entry) {
        TAILQ_REMOVE(&state -> g_namespaces, ns_entry, link);
        free(ns_entry);
    }

    TAILQ_FOREACH_SAFE(ctrlr_entry, &state -> g_controllers, link, tmp_ctrlr_entry) {
        TAILQ_REMOVE(&state -> g_controllers, ctrlr_entry, link);
        spdk_nvme_detach_async(ctrlr_entry->ctrlr, &detach_ctx);
        free(ctrlr_entry);
    }

    if (detach_ctx) {
        spdk_nvme_detach_poll(detach_ctx);
    }
}
