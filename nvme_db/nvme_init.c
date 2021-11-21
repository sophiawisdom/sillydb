//
//  nvme_init.c
//  
//
//  Created by Sophia Wisdom on 11/20/21.
//

#include "nvme_init.h"
#include "nvme_internal.h"
#include "spdk/stdinc.h"
#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/nvme_zns.h"
#include "spdk/env.h"

// This is all single-threaded, because it happens only once, in initialization.

static void
register_ns(struct state *state, struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns *ns)
{
    struct ns_entry *entry;

    if (!spdk_nvme_ns_is_active(ns)) {
        return;
    }

    entry = malloc(sizeof(struct ns_entry));
    if (entry == NULL) {
        perror("ns_entry malloc");
        exit(1);
    }

    entry->ctrlr = ctrlr;
    entry->ns = ns;
    TAILQ_INSERT_TAIL(&state -> g_namespaces, entry, link);

    printf("  Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
           spdk_nvme_ns_get_size(ns) / 1000000000);
}

static bool
probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
     struct spdk_nvme_ctrlr_opts *opts)
{
    printf("Attaching to %s\n", trid->traddr);

    return true;
}

static void
attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
      struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
    int nsid;
    struct ctrlr_entry *entry;
    struct spdk_nvme_ns *ns;
    const struct spdk_nvme_ctrlr_data *cdata;
    struct state *state = cb_ctx;

    entry = malloc(sizeof(struct ctrlr_entry));
    if (entry == NULL) {
        perror("ctrlr_entry malloc");
        exit(1);
    }

    printf("Attached to %s\n", trid->traddr);

    /*
     * spdk_nvme_ctrlr is the logical abstraction in SPDK for an NVMe
     *  controller.  During initialization, the IDENTIFY data for the
     *  controller is read using an NVMe admin command, and that data
     *  can be retrieved using spdk_nvme_ctrlr_get_data() to get
     *  detailed information on the controller.  Refer to the NVMe
     *  specification for more details on IDENTIFY for NVMe controllers.
     */
    cdata = spdk_nvme_ctrlr_get_data(ctrlr);
    
    printf("about to get data\n");

    snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

    printf("snprintf'd name. \n");

    entry->ctrlr = ctrlr;
    TAILQ_INSERT_TAIL(&state -> g_controllers, entry, link);
    
    printf("inserted tail, about to spdk_nvme_ctrlr_get_first_active_ns\n");
    /*
     * Each controller has one or more namespaces.  An NVMe namespace is basically
     *  equivalent to a SCSI LUN.  The controller's IDENTIFY data tells us how
     *  many namespaces exist on the controller.  For Intel(R) P3X00 controllers,
     *  it will just be one namespace.
     *
     * Note that in NVMe, namespace IDs start at 1, not 0.
     */
    for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
         nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
        ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
        if (ns == NULL) {
            continue;
        }
        register_ns(state, ctrlr, ns);
    }
}

int initialize(struct state *state) {
    TAILQ_INIT(&state -> g_namespaces);
    TAILQ_INIT(&state -> g_controllers);

    struct spdk_env_opts opts;

    /*
     * SPDK relies on an abstraction around the local environment
     * named env that handles memory allocation and PCI device operations.
     * This library must be initialized first.
     *
     */
    spdk_env_opts_init(&opts);
    opts.name = "hello_world";
    opts.shm_id = 0;
    if (spdk_env_init(&opts) < 0) {
        fprintf(stderr, "Unable to initialize SPDK env\n");
        return 1;
    }
    
    printf("about to init\n");
    
    if (spdk_vmd_init()) {
        fprintf(stderr, "Failed to initialize VMD."
            " Some NVMe devices can be unavailable.\n");
        return 1;
    }
    
    printf("about to probe\n");
    
    /*
     * Start the SPDK NVMe enumeration process.  probe_cb will be called
     *  for each NVMe controller found, giving our application a choice on
     *  whether to attach to each controller.  attach_cb will then be
     *  called for each controller after the SPDK NVMe driver has completed
     *  initializing the controller we chose to attach.
     */
    int rc = spdk_nvme_probe(NULL, state, probe_cb, attach_cb, NULL);
    if (rc != 0) {
        fprintf(stderr, "spdk_nvme_probe() failed\n");
        cleanup();
        return 2;
    }
    
    printf("about to tailq empty\n");

    if (TAILQ_EMPTY(&state -> g_controllers)) {
        fprintf(stderr, "no NVMe controllers found\n");
        cleanup();
        return 2;
    }

    printf("Initialization complete.\n");
    return 0;
}
