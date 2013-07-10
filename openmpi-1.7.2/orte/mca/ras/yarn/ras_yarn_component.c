/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/base/base.h"
#include "opal/util/net.h"
#include "opal/opal_socket_errno.h"

#include "orte/util/name_fns.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/ras/base/ras_private.h"
#include "ras_yarn.h"


/*
 * Local functions
 */
static int ras_yarn_open(void);
static int ras_yarn_close(void);
static int orte_ras_yarn_component_query(mca_base_module_t **module, int *priority);


orte_ras_yarn_component_t mca_ras_yarn_component = {
    {
        /* First, the mca_base_component_t struct containing meta
           information about the component itself */

        {
            ORTE_RAS_BASE_VERSION_2_0_0,
        
            /* Component name and version */
            "yarn",
            ORTE_MAJOR_VERSION,
            ORTE_MINOR_VERSION,
            ORTE_RELEASE_VERSION,
        
            /* Component open and close functions */
            ras_yarn_open,
            ras_yarn_close,
            orte_ras_yarn_component_query
        },
        {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        }
    }
};

static int ras_yarn_open(void)
{
    return ORTE_SUCCESS;
}

static int ras_yarn_close(void)
{
    return ORTE_SUCCESS;
}

static int orte_ras_yarn_component_query(mca_base_module_t **module, int *priority)
{
    char *directive;
    
    /* determine if we were specified */
    directive = getenv("OMPI_MCA_ras");
    
    if (NULL == directive || 0 != strcmp("yarn", directive)) {
        *priority = 0;
        *module = NULL;
        return ORTE_ERROR;
    }

    OPAL_OUTPUT_VERBOSE((2, orte_ras_base.ras_output,
                         "%s ras:yarn: available for selection",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    *priority = 0;
    *module = (mca_base_module_t *) &orte_ras_yarn_module;
    return ORTE_SUCCESS;
}
