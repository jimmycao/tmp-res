/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"

#include "orte/util/proc_info.h"
#include "orte/mca/errmgr/errmgr.h"

#include "orte/mca/iof/iof.h"
#include "orte/mca/iof/base/base.h"

/**
 * Call the query function on all available components to find out if
 * they want to run.  Select the single component with the highest 
 * priority.
 */
int orte_iof_base_select(void)
{
    orte_iof_base_component_t *best_component = NULL;
    orte_iof_base_module_t *best_module = NULL;
    int rc;
    
    /*
     * Select the best component
     */
    if( OPAL_SUCCESS != mca_base_select("iof", orte_iof_base.iof_output,
                                        &orte_iof_base.iof_components_opened,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component) ) {
        /* it is okay to not find a module if we are a CM process */
        if (ORTE_PROC_IS_CM) {
            return ORTE_SUCCESS;
        }
        /* otherwise, this is a problem */
        return ORTE_ERR_NOT_FOUND;
    }
    
    /* Save the winner */
    orte_iof = *best_module;
    /* init it */
    if (NULL != orte_iof.init) {
        if (ORTE_SUCCESS != (rc = orte_iof.init())) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    }

    return ORTE_SUCCESS;
}
