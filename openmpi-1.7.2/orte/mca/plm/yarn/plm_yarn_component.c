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
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics.  Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire components just to query their version and parameters.
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/base/mca_base_param.h"

#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/base/plm_private.h"
#include "plm_yarn.h"


/*
 * Public string showing the plm ompi_yarn component version number
 */
const char *mca_plm_yarn_component_version_string =
  "Open MPI yarn plm MCA component version " ORTE_VERSION;


/*
 * Local functions
 */
static int plm_yarn_open(void);
static int plm_yarn_close(void);
static int orte_plm_yarn_component_query(mca_base_module_t **module, int *priority);


/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

orte_plm_yarn_component_t mca_plm_yarn_component = {

    {
        /* First, the mca_component_t struct containing meta
           information about the component itself */

        {
            ORTE_PLM_BASE_VERSION_2_0_0,
            
            /* Component name and version */
            "yarn",
            ORTE_MAJOR_VERSION,
            ORTE_MINOR_VERSION,
            ORTE_RELEASE_VERSION,
            
            /* Component open and close functions */
            plm_yarn_open,
            plm_yarn_close,
            orte_plm_yarn_component_query
        },
        {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        }
    }

    /* Other orte_plm_yarn_component_t items -- left uninitialized
       here; will be initialized in plm_yarn_open() */
};

static int plm_yarn_open(void)
{
    return ORTE_SUCCESS;
}

static int orte_plm_yarn_component_query(mca_base_module_t **module, int *priority)
{
	*priority = 0;

	OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
					"%s plm:yarn: available for selection",
					ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

	*module = (mca_base_module_t *) &orte_plm_yarn_module;
	return ORTE_SUCCESS;
}


static int plm_yarn_close(void)
{
    return ORTE_SUCCESS;
}
