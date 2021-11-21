//
//  nvme_internal.h
//  
//
//  Created by Sophia Wisdom on 11/20/21.
//

#ifndef nvme_internal_h
#define nvme_internal_h

#include "spdk/env.h"

struct ctrlr_entry {
    struct spdk_nvme_ctrlr        *ctrlr;
    TAILQ_ENTRY(ctrlr_entry)    link;
    char                name[1024];
};

struct ns_entry {
    struct spdk_nvme_ctrlr    *ctrlr;
    struct spdk_nvme_ns    *ns;
    TAILQ_ENTRY(ns_entry)    link;
    struct spdk_nvme_qpair    *qpair;
};

struct state {
    _Atomic int lock;
    
    int num_entries;
    
    TAILQ_HEAD(control_head, ctrlr_entry) g_controllers;
    TAILQ_HEAD(namespace_head, ns_entry) g_namespaces;
    ns_entry *main_namespace;
};

#endif /* nvme_internal_h */
