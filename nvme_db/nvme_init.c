//
//  nvme_init.c
//  
//
//  Created by Sophia Wisdom on 11/20/21.
//

#include "nvme_init.h"

void initialize() {
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
        return NULL;
    }
    
    if (spdk_vmd_init()) {
        fprintf(stderr, "Failed to initialize VMD."
            " Some NVMe devices can be unavailable.\n");
        return NULL;
    }
    
    /*
     * Start the SPDK NVMe enumeration process.  probe_cb will be called
     *  for each NVMe controller found, giving our application a choice on
     *  whether to attach to each controller.  attach_cb will then be
     *  called for each controller after the SPDK NVMe driver has completed
     *  initializing the controller we chose to attach.
     */
    rc = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
    if (rc != 0) {
        fprintf(stderr, "spdk_nvme_probe() failed\n");
        cleanup();
        return 1;
    }

    if (TAILQ_EMPTY(&g_controllers)) {
        fprintf(stderr, "no NVMe controllers found\n");
        cleanup();
        return 1;
    }

    printf("Initialization complete.\n");
}
