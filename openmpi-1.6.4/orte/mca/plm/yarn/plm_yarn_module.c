/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Los Alamos National Security, LLC.  All rights
 *                         reserved. 
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

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <signal.h>
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include "opal/mca/installdirs/installdirs.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/opal_environ.h"
#include "opal/util/path.h"
#include "opal/util/basename.h"
#include "opal/mca/base/mca_base_param.h"

#include "orte/constants.h"
#include "orte/types.h"
#include "orte/util/show_help.h"
#include "orte/util/name_fns.h"
#include "orte/util/regex.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/rmaps.h"

#include "orte/orted/orted.h"

#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/base/plm_private.h"
#include "plm_yarn.h"

#include "orte/mca/hdclient/hdclient.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/grpcomm/grpcomm.h"

#define ORTE_RML_TAG_YARN_SYNC_REQUEST      97
#define ORTE_RML_TAG_YARN_SYNC_RESPONSE     98

extern char** environ;
static bool appmaster_finished = false;

typedef struct {
    int num_sync_daemons = 0;
    int num_completed_daemon_procs = 0;
    int num_completed_jdata_procs = 0;
    orte_job_t* jdata = NULL;
} yarn_plm_job_t;

/*
 * Local functions
 */
static int plm_yarn_init(void);
static int plm_yarn_launch_job(orte_job_t *jdata);
static int plm_yarn_terminate_orteds(void);
static int plm_yarn_signal_job(orte_jobid_t jobid, int32_t signal);
static int plm_yarn_terminate_job(orte_jobid_t jobid);
static int plm_yarn_finalize(void);


/*
 * Local intermediate functions
 */

static int launch_daemons(orte_job_t* jdata, int new_added_daemon_num);
static int setup_daemon_proc_env_and_argv(orte_proc_t* proc, char ***pargv,
        int *argc, char ***penv);

static void yarn_hnp_sync_recv(int status, orte_process_name_t* sender,
                           opal_buffer_t* buffer, orte_rml_tag_t tag,
                           void* cbdata);

static int plm_yarn_actual_launch_procs(orte_job_t* jdata);
static int setup_proc_env_and_argv(orte_job_t* jdata, orte_app_context_t* app,
        orte_proc_t* proc, char ***pargv, char ***penv);

static void heartbeat_with_am_cb(int fd, short event, void *data);
static void finish_app_master(bool succeed);
static void process_state_monitor_cb(int fd, short args, void *cbdata);

static int common_launch_process(orte_job_t *jdata, bool launch_daemon, int new_added_daemon_num, int *launched_proc_num);



/*
 * Global variable
 */
orte_plm_base_module_1_0_0_t orte_plm_yarn_module = {
    plm_yarn_init,
    orte_plm_base_set_hnp_name,
    plm_yarn_launch_job,
    NULL,
    plm_yarn_terminate_job,
    plm_yarn_terminate_orteds,
    orte_plm_base_orted_kill_local_procs,
    plm_yarn_signal_job,
    plm_yarn_finalize
};

/*
 * Local variables
 */
static pid_t primary_srun_pid = 0;
static bool primary_pid_set = false;
static orte_jobid_t active_job = ORTE_JOBID_INVALID;
static bool launching_daemons;
static bool local_launch_available = false;

/**
* Init the module
 */
static int plm_yarn_init(void)
{
    int rc;

    if (ORTE_SUCCESS != (rc = orte_plm_base_comm_start())) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/* setup argv for daemon process */
static int setup_daemon_proc_env_and_argv(orte_proc_t* proc, char ***pargv,
        int *argc, char ***penv)
{
    orte_job_t* daemons;
    int rc;
    char* param;
     
    /* get daemon job object */
    daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);

    *penv = opal_argv_copy(orte_launch_environ);

    /* prepend orted to argv */
    opal_argv_append(argc, pargv, "orted");

    /* ess */
    opal_argv_append(argc, pargv, "-mca");
    opal_argv_append(argc, pargv, "ess");
    opal_argv_append(argc, pargv, "env");

    /* jobid */
    opal_argv_append(argc, pargv, "-mca");
    opal_argv_append(argc, pargv, "orte_ess_jobid");
    if (ORTE_SUCCESS != (rc = orte_util_convert_jobid_to_string(&param, ORTE_PROC_MY_NAME->jobid))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    opal_argv_append(argc, pargv, param);
    free(param);

    /* vpid */
    opal_argv_append(argc, pargv, "-mca");
    opal_argv_append(argc, pargv, "orte_ess_vpid");
    if (ORTE_SUCCESS != (rc = orte_util_convert_vpid_to_string(&param, proc->name.vpid))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    opal_argv_append(argc, pargv, param);
    free(param);

    /* num processes */
    opal_argv_append(argc, pargv, "-mca");
    opal_argv_append(argc, pargv, "orte_ess_num_procs");
    asprintf(&param, "%lu", daemons->num_procs);
    opal_argv_append(argc, pargv, param);
    free(param);

    /* pass the uri of the hnp */
    asprintf(&param, "\\\"%s\\\"", orte_rml.get_contact_info());
    opal_argv_append(argc, pargv, "--hnp-uri");
    opal_argv_append(argc, pargv, param);
    free(param);

    /* oob */
    opal_argv_append(argc, pargv, "-mca");
    opal_argv_append(argc, pargv, "oob");
    opal_argv_append(argc, pargv, "tcp");

    /* odls */
    opal_argv_append(argc, pargv, "-mca");
    opal_argv_append(argc, pargv, "odls");
    opal_argv_append(argc, pargv, "yarn");

    /* add stdout, stderr to orted */
    opal_argv_append_nosize(pargv, "1><LOG_DIR>/stdout");
    opal_argv_append_nosize(pargv, "2><LOG_DIR>/stderr");

    /* print launch commandline and env when this env is specified */
    if (getenv("HAMSTER_VERBOSE")) {
        char* join_argv = opal_argv_join(*pargv, ' ');
        OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output, "%s plm:yarn launch_daemon argv=%s",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), join_argv));
        if (join_argv) {
            free(join_argv);
        }
    }
    return 0;
}

static int common_launch_process(orte_job_t *jdata, bool launch_daemon, int new_added_daemon_num, int *launched_proc_num)
{
	int i, rc;
	orte_proc_t* proc = NULL;
	char **argv;
	int argc;
	char **env;
	bool error_flag = false;
	int launched_num = 0;

	/* 1. create launch message */
	/*
	 message LaunchRequestProto {
	 repeated LaunchContextProto launch_contexts = 1;
	 }

	 message LaunchContextProto {
	 repeated string envars = 1;
	 optional string args = 2;
	 optional string host_name = 3;
	 optional ProcessNameProto name = 4;
	 }

	 message ProcessNameProto {
	 optional int32 jobid = 1;
	 optional int32 vpid = 2;
	 }
	 */
	struct pbc_wmessage* request_msg = pbc_wmessage_new(orte_hdclient_pb_env, "LaunchRequestProto");
	if (!request_msg) {
		opal_output(0, "%s plm:yarn:common_process_launch: failed to create AllocateRequestProto",
				ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
		return ORTE_ERROR;
	}

	/* when launch_daemon, start from 1 because we don't need launch HNP process */
	i = launch_daemon ? 1 : 0;
    if (launch_daemon) {
        i = jdata->num->procs - new_added_daemon_num;
        if (i < 0) {
            opal_output(0, "%s plm:yarn:common_launch_process: failed to launch daemon,  \
                because new_added_daemon_num larger than num proc in the daemon job",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            return ORTE_ERROR;
        }
    } else {
        i = 0;
    }

    OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output, "%s plm:yarn:common_launch_process: we will launch [%d] procs for the job [%s]",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), jdata->num_procs, ORTE_JOBID_PRINT(jdata->jobid)));

	for (; i < jdata->num_procs; i++) {
		argv = NULL;
		argc = 0;
		env = NULL;
		/* setup env/argv  */
		proc = opal_pointer_array_get_item(jdata->procs, i);
		if (!proc) {
			opal_output(0, "%s plm:yarn:common_launch_process: proc[%d] is NULL",
					ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i);
			ORTE_ERROR_LOG(ORTE_ERROR_DEFAULT_EXIT_CODE);
		}

		if (launch_daemon) {
			rc = setup_daemon_proc_env_and_argv(proc, &argv, &argc, &env);
		} else {
			orte_app_context_t* app = (orte_app_context_t*) opal_pointer_array_get_item(jdata->apps, proc->app_idx);
			rc = setup_proc_env_and_argv(jdata, app, proc, &argv, &env);
		}
		if (0 != rc) {
			opal_output(0,
					"%s plm:yarn:common_launch_process: failed to setup env/argv of proc[%d]",
					ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i);
			ORTE_ERROR_LOG(ORTE_ERROR_DEFAULT_EXIT_CODE);
			error_flag = true;
			goto cleanup;
		}

		 /* print launch commandline and env when this env is specified */
		if (getenv("HAMSTER_VERBOSE")) {

			char* join_argv = opal_argv_join(argv, ' ');

			OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output, "%s plm:yarn:common_launch_process: launch argv=%s",
							ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), join_argv));
			if (join_argv) {
				free(join_argv);
			}
		}

		OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output,
									"%s plm:yarn:common_launch_process: after setup env and argv for proc=%d.",
									ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i));

		/* now start packing request_msg */
		struct pbc_wmessage *launch_contexts_msg = pbc_wmessage_message(request_msg, "launch_contexts");
		if (!launch_contexts_msg) {
			opal_output(0,
					"%s plm:yarn:common_process_launch: create launch_contexts_msg failed",
					ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
			error_flag = true;
			goto cleanup;
		}

		char **tmp_env = env;
		while (*tmp_env) {
			pbc_wmessage_string(launch_contexts_msg, "envars", *tmp_env, strlen(*tmp_env));
			tmp_env++;
		}

		char* join_argv = opal_argv_join(argv, ' ');
		pbc_wmessage_string(launch_contexts_msg, "args", join_argv, strlen(join_argv));

		pbc_wmessage_string(launch_contexts_msg, "host_name", proc->node->name, strlen(proc->node->name));

		struct pbc_wmessage *proccess_name_msg = pbc_wmessage_message(launch_contexts_msg, "name");
		if (!proccess_name_msg) {
			opal_output(0,
					"%s plm:yarn:common_process_launch: create proccess_name_msg failed",
					ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
			error_flag = true;
			goto cleanup;
		}

		rc = pbc_wmessage_integer(proccess_name_msg, "jobid", ORTE_LOCAL_JOBID(proc->name.jobid), 0);
		if (0 != rc) {
			opal_output(0,
					"%s plm:yarn:common_process_launch: pack jobid in proccess_name_msg failed",
					ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
			error_flag = true;
			goto cleanup;
		}

		rc = pbc_wmessage_integer(proccess_name_msg, "vpid", proc->name.vpid,
				0);
		if (0 != rc) {
			opal_output(0,
					"%s plm:yarn:common_process_launch: pack vpid in proccess_name_msg failed",
					ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
			error_flag = true;
			goto cleanup;
		}

cleanup:
		/* free argv and env for this proc */
		if (argv) {
			opal_argv_free(argv);
		}
		if (env) {
			opal_argv_free(env);
		}
		if (join_argv) {
			free(join_argv);
		}
		if (error_flag) {
			pbc_wmessage_delete(request_msg);
			ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
			return ORTE_ERROR;
		}
	}

	/* 2. send launch deamon procs request msg */
	rc = orte_hdclient_send_message_and_delete(request_msg, HAMSTER_MSG_LAUNCH);
	if (rc != 0) {
		opal_output(0,
				"%s plm:yarn:common_process_launch: error happened when send launch proc request to AM",
				ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
		if (request_msg) {
			pbc_wmessage_delete(request_msg);	
		}
		return ORTE_ERROR;
	}

	/* 3. recv response and parse the msg*/
	/*
	 message LaunchResponseProto {
	 repeated LaunchResultProto results = 1;
	 }

	 message LaunchResultProto {
	 optional ProcessNameProto name = 1;
	 optional bool success = 2;
	 }

	 message ProcessNameProto {
	 optional int32 jobid = 1;
	 optional int32 vpid = 2;
	 }
	 */
	struct pbc_rmessage* response_msg = NULL;
	response_msg = orte_hdclient_recv_message("LaunchResponseProto");
	if (!response_msg) {
		opal_output(0,
				"%s plm:yarn:common_process_launch: error happened when recv launch response msg from AM",
				ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
		goto launch_failed;
	}

	int n = pbc_rmessage_size(response_msg, "results");
	if (n < 0) {
		opal_output(0,
				"%s plm:yarn:common_process_launch: got n(=%d) < 0, please check",
				ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), n);
		goto launch_failed;
	}

	for (i = 0; i < n; i++) {
		struct pbc_rmessage* results_msg = pbc_rmessage_message(response_msg, "results", i);
		if (!results_msg) {
			opal_output(0,
					"%s plm:yarn:launch_daemons: error when parse returned launch results from AM",
					ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
			goto launch_failed;
		}

		struct pbc_rmessage* proc_name_msg = pbc_rmessage_message(results_msg, "name", 0);
		if (!proc_name_msg) {
			opal_output(0,
					"%s plm:yarn:common_process_launch: error when parse returned proc_name_msg from AM",
					ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
			goto launch_failed;
		}

		orte_jobid_t local_jobid = pbc_rmessage_integer(proc_name_msg, "jobid", 0, NULL);
		orte_vpid_t vpid = pbc_rmessage_integer(proc_name_msg, "vpid", 0, NULL);

		bool success = pbc_rmessage_integer(results_msg, "success", 0, NULL);

		orte_proc_t* proc = (orte_proc_t*) opal_pointer_array_get_item(jdata->procs, vpid);
		if (success) {
			proc->state = ORTE_PROC_STATE_RUNNING;
			launched_num++;
		} else {
			opal_output(0,
					"%s plm:yarn:common_process_launch: launch proc failed when jobid = %u, vpid = %u",
					ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), local_jobid, vpid);
			proc->state = ORTE_PROC_STATE_FAILED_TO_START;
			jdata->state = ORTE_JOB_STATE_FAILED_TO_START;
			goto launch_failed;
		}
	}

	/* to return back */
	*launched_proc_num = launched_num;
	return ORTE_SUCCESS;

launch_failed:
	    if (response_msg) {
	        pbc_rmessage_delete(response_msg);
	    }
	    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
	    return ORTE_ERROR;
}

static int launch_daemons(orte_job_t* jdata, int new_added_daemon_num)
{
    int rc;
    int launched_proc_num = 0;

    orte_job_t* daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);

    rc = common_launch_process(daemons, true, new_added_daemon_num, &launched_proc_num);

    if (rc != ORTE_SUCCESS) {
    	return rc;
    }

    /* if all daemon procs are launched successfully, then modify the job's state */
    if (launched_proc_num == (daemons->num_procs-1)) {
        jdata->state = ORTE_JOB_STATE_LAUNCHED;
        daemons->state = ORTE_JOB_STATE_LAUNCHED;
        OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output,
                        "%s plm:yarn:launch_daemons: launch daemon proc successfully with AM",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
    }
    return ORTE_SUCCESS;
}


static void heartbeat_with_am_cb(int fd, short event, void *data)
{
    int i, rc;
    orte_job_t *jdata = (orte_job_t*)data;
    orte_job_t* daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);

    /* 1. create heartbeat request msg */
    /*
    message HeartbeatRequestProto {
    }
    */
    struct pbc_wmessage* request_msg = pbc_wmessage_new(orte_hdclient_pb_env, "HeartbeatRequestProto");
    if (!request_msg) {
        opal_output(0, "%s plm:yarn:heartbeat_with_AM_cb: failed to create request_msg",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        ORTE_ERROR_LOG(ORTE_ERROR_DEFAULT_EXIT_CODE);
        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
        orte_trigger_event(&orte_exit);
        return;
    }

    /* 2. send heartbeat request msg */
    rc = orte_hdclient_send_message_and_delete(request_msg, HAMSTER_MSG_HEARTBEAT);
    if (rc != 0) {
        opal_output(0,
                "%s plm:yarn:heartbeat_with_AM_cb: error happened when send request_msg to AM",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        ORTE_ERROR_LOG(ORTE_ERROR_DEFAULT_EXIT_CODE);
        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
        orte_trigger_event(&orte_exit);
        return;
    }

    /* 3. recv response and parse the msg*/
    /*
     message HeartbeatResponseProto {
         repeated ProcessStatusProto completed_processes = 1;
     }

     message ProcessStatusProto {
         optional ProcessNameProto name = 1;
         optional ProcessStateProto state = 2;
         optional int32 exit_value = 3;
     }

     enum ProcessStateProto {
         RUNNING = 1;
         COMPLETED = 2;
     }

     message ProcessNameProto {
         optional int32 jobid = 1;
         optional int32 vpid = 2;
     }
     */

    struct pbc_rmessage* response_msg = orte_hdclient_recv_message("HeartbeatResponseProto");
    if (!response_msg) {
        opal_output(0,
                "%s plm:yarn:heartbeat_with_AM_cb: error happened when recv HeartbeatResponseProto msg from AM",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        goto cleanup;
    }

    int n = pbc_rmessage_size(response_msg, "completed_processes");
    if (n < 0) {
        opal_output(0,
                "%s plm:yarn:heartbeat_with_AM_cb: got n(=%d) < 0, please check",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), n);
        goto cleanup;
    }

    for (i = 0; i < n; i++) {
        struct pbc_rmessage* completed_procs_msg = pbc_rmessage_message(response_msg, "completed_processes", i);
        if (!completed_procs_msg) {
            opal_output(0,
                    "%s plm:yarn:heartbeat_with_AM_cb: error when parse returned completed_procs_msg from AM",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            goto cleanup;
        }

        struct pbc_rmessage* proc_name_msg = pbc_rmessage_message(completed_procs_msg, "name", 0);
        if (!proc_name_msg) {
            opal_output(0,
                    "%s plm:yarn:heartbeat_with_AM_cb: error when parse proc_name_msg",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            goto cleanup;
        }

        uint32_t local_jobid = pbc_rmessage_integer(proc_name_msg, "jobid", 0, NULL);
        uint32_t vpid = pbc_rmessage_integer(proc_name_msg, "vpid", 0, NULL);

        uint32_t exit_value = pbc_rmessage_integer(completed_procs_msg, "exit_value", 0, NULL);

        /* next, we will modify proc's state */
        orte_job_t* tmp_jdata = (orte_job_t*) opal_pointer_array_get_item(orte_job_data, local_jobid);
        orte_proc_t* proc = (orte_proc_t*) opal_pointer_array_get_item(tmp_jdata->procs, vpid);

        /* if this process is already terminated, just skip over */
        if (proc->state >= ORTE_PROC_STATE_TERMINATED) {
            continue;
        }

        if (exit_value == -1000 || exit_value == -100 || exit_value == -101) {
            opal_output(0, "%s plm:yarn:heartbeat proc failed to start", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            ORTE_ERROR_LOG(ORTE_ERROR);
            proc->state = ORTE_PROC_STATE_FAILED_TO_START;
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
            orte_trigger_event(&orte_exit);
        } else {
            /* here, means currently the proc's state < ORTE_PROC_STATE_TERMINATED,
             * however, from AM, we got the proc's container is terminated,
             * to solve this dilemma , we set a timer event to confirm this proc's state,
             */
            opal_event_t *ev = NULL;
            ev = (opal_event_t*) malloc(sizeof(opal_event_t));

            struct timeval delay;
            delay.tv_sec = 15;
            delay.tv_usec = 0;
            opal_evtimer_set(ev, process_state_monitor_cb, proc);
            opal_evtimer_add(ev, &delay);
        }

        if (tmp_jdata->jobid == jdata->jobid) {
            num_completed_jdata_procs++;
        }
    }

cleanup:
    if (response_msg) {
        pbc_rmessage_delete(response_msg);
    }

    if (num_completed_jdata_procs == jdata->num_procs) {
        /*
         * all procs are completed, send finish request to AM,
         * modify job state to ORTE_JOB_STATE_TERMINATED
         */
        jdata->state = ORTE_JOB_STATE_TERMINATED;
        return;
    } else {
        /* next heartbeat */
        opal_event_t *ev = NULL;
        ev = (opal_event_t*) malloc(sizeof(opal_event_t));

        struct timeval delay;
        delay.tv_sec = 1;
        delay.tv_usec = 0;
        opal_evtimer_set(ev, heartbeat_with_am_cb, jdata);
        opal_evtimer_add(ev, &delay);
    }
}

static void process_state_monitor_cb(int fd, short args, void *cbdata)
{
    orte_proc_t *proc = (orte_proc_t*)cbdata;
    if(proc->state >= ORTE_PROC_STATE_TERMINATED) { 
        return;
    }
    
    proc->state = ORTE_PROC_STATE_ABORTED;
    opal_output(0, "%s recved aborted proc from AM, proc:%d", 
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
            proc->name.vpid);
    ORTE_ERROR_LOG(ORTE_ERROR_DEFAULT_EXIT_CODE);
    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
    orte_trigger_event(&orte_exit);
}

static void finish_app_master(bool succeed)
{
    int rc;
    int i, j;
    char *diag_msg = "finish_app_master";

    // we need double check if any proc failed
    if (succeed) {
        for (i = 1; i < orte_job_data->size; i++) {
            orte_job_t* job = opal_pointer_array_get_item(orte_job_data, i);
            if (!job) {
                continue;
            }
            for (j = 0; j < job->procs->size; j++) {
                orte_proc_t* proc = opal_pointer_array_get_item(job->procs, j);
                if (!proc) {
                    continue;
                }
                // if any process is non-terminated, we will consider it's error
                if (proc->state != ORTE_PROC_STATE_TERMINATED) {
                    succeed = false;
                    break;
                }
            }
            if (!succeed) {
                break;
            }
        }
    }

    /* 1. create launch message */
    /*
    message FinishRequestProto {
        optional bool succeed = 1;
        optional string diagnostics = 2;
    }
    */
    struct pbc_wmessage* request_msg = pbc_wmessage_new(orte_hdclient_pb_env, "FinishRequestProto");

    if (!request_msg) {
        opal_output(0, "%s plm:yarn:finish_app_master: failed to create request_msg",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        goto cleanup;
    }

    rc = pbc_wmessage_integer(request_msg, "succeed", succeed, 0);
    if (0 != rc) {
        opal_output(0,
                "%s plm:yarn:finish_app_master: pack succeed in request_msg failed",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        goto cleanup;
    }

    rc = pbc_wmessage_string(request_msg, "diagnostics", diag_msg, strlen(diag_msg));
    if (0 != rc) {
        opal_output(0,
                "%s plm:yarn:finish_app_master: pack diagnostics in request_msg failed",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        goto cleanup;
    }

    /* 2. send launch procs request msg */
    rc = orte_hdclient_send_message_and_delete(request_msg, HAMSTER_MSG_FINISH);
    if (rc != 0) {
        opal_output(0,
                "%s plm:yarn:finish_app_master: error happened when send request_msg to AM",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        goto cleanup;
    }

    /* 3. recv response and parse the msg*/
    /*
     message FinishResponseProto {
     }
     */
    struct pbc_rmessage* response_msg = orte_hdclient_recv_message("FinishResponseProto");
    if (!response_msg) {
        opal_output(0,
                "%s plm:yarn:finish_app_master: error happened when recv response_msg from AM",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        goto cleanup;
    }

    appmaster_finished = true;

cleanup:
    if (response_msg) {
        pbc_rmessage_delete(response_msg);
    }
}

/* When working in this function, ALWAYS jump to "cleanup" if
 * you encounter an error so that orterun will be woken up and
 * the job can cleanly terminate
 */
static int plm_yarn_launch_job(orte_job_t *jdata)
{
    orte_app_context_t **apps;
    orte_node_t **nodes;
    orte_std_cntr_t n;
    orte_job_map_t *map;
    char *jobid_string = NULL;
    char *param;
    char **argv = NULL;
    int argc;
    int rc;
    char *tmp;
    char** env = NULL;
    char* var;
    char *nodelist_flat;
    char **nodelist_argv;
    char *name_string;
    char **custom_strings;
    int num_args, i;
    char *cur_prefix;
    struct timeval launchstart, launchstop;
    int proc_vpid_index;
    orte_jobid_t failed_job;
    bool failed_launch=true;
    bool using_regexp=false;

    orte_proc_t* proc = NULL;

    //============heartbeat with AM======
    opal_event_t *ev = NULL;
    ev = (opal_event_t*) malloc(sizeof(opal_event_t));

    struct timeval delay;
    delay.tv_sec = 1;
    delay.tv_usec = 0;

    yarn_plm_job_t* yarn_job = (yarn_plm_job_t*)malloc(sizeof(yarn_plm_job_t));
    yarn_plm->jdata = jdata;

    opal_evtimer_set(ev, heartbeat_with_am_cb, yarn_job);
    opal_evtimer_add(ev, &delay);
    //===================================

    if (NULL == jdata) {
        /* just launching debugger daemons */
        active_job = ORTE_JOBID_INVALID;
        goto launch_apps;
    }

    if (jdata->controls & ORTE_JOB_CONTROL_DEBUGGER_DAEMON) {
        /* debugger daemons */
        active_job = jdata->jobid;
        goto launch_apps;
    }

    if (jdata->controls & ORTE_JOB_CONTROL_LOCAL_SLAVE) {
        /* if this is a request to launch a local slave,
         * then we will not be launching an orted - we will
         * directly ssh the slave process itself. No mapping
         * is performed to support this - the caller must
         * provide all the info required to launch the job,
         * including the target hosts
         */
        if (!local_launch_available) {
            /* if we can't support this, then abort */
            orte_show_help("help-plm-yarn.txt", "no-local-slave-support", true);
            return ORTE_ERR_FAILED_TO_START;
        }
        return orte_plm_base_local_slave_launch(jdata);
    }
    
    /* if we are timing, record the start time */
    if (orte_timing) {
        gettimeofday(&orte_plm_globals.daemonlaunchstart, NULL);
    }
    
    /* flag the daemons as failing by default */
    failed_job = ORTE_PROC_MY_NAME->jobid;
    
    if (orte_timing) {
        if (0 != gettimeofday(&launchstart, NULL)) {
            opal_output(0, "plm_yarn: could not obtain job start time");
            launchstart.tv_sec = 0;
            launchstart.tv_usec = 0;
        }        
    }
    
    /* indicate the state of the launch */
    launching_daemons = true;
    
    /* setup the job */
    if (ORTE_SUCCESS != (rc = orte_plm_base_setup_job(jdata))) {
        ORTE_ERROR_LOG(rc);
        goto cleanup;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                         "%s plm:yarn: launching job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));
    
    /* set the active jobid */
    active_job = jdata->jobid;
    
    /* Get the map for this job */
    if (NULL == (map = orte_rmaps.get_job_map(active_job))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        rc = ORTE_ERR_NOT_FOUND;
        goto cleanup;
    }
    apps = (orte_app_context_t**)jdata->apps->addr;
    nodes = (orte_node_t**)map->nodes->addr;
        
    if (0 == map->num_new_daemons) {
        /* no new daemons required - just launch apps */
        OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                             "%s plm:yarn: no new daemons to launch",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        goto launch_apps;
    }

    /* launch daemons */
    if (ORTE_SUCCESS != (rc = launch_daemons(jdata, map->num_new_daemons))) {
        opal_output(0, "%s plm:yarn:plm_yarn_launch_job: launch deamon failed",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        goto cleanup;
    }
    
    /* wait for daemons to callback */
    if (ORTE_SUCCESS
            != (rc = orte_plm_base_daemon_callback(map->num_new_daemons))) {
        OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                        "%s plm:yarn:plm_yarn_launch_job: daemon launch failed for job %s on error %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_JOBID_PRINT(active_job), ORTE_ERROR_NAME(rc)));
        goto cleanup;
    }

launch_apps:
    /* get here if daemons launch okay - any failures now by apps */
    launching_daemons = false;
    failed_job = active_job;

    /* register recv callback for daemons sync request */
    if (ORTE_SUCCESS != (rc = orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                    ORTE_RML_TAG_YARN_SYNC_REQUEST, ORTE_RML_PERSISTENT,
                    yarn_hnp_sync_recv, yarn_job))) {
        ORTE_ERROR_LOG(rc);
        goto cleanup;
    }

    if (ORTE_SUCCESS != (rc = orte_plm_base_launch_apps(active_job))) {
        OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                        "%s plm:slurm:plm_yarn_launch_job: launch of apps failed for job %s on error %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_JOBID_PRINT(active_job), ORTE_ERROR_NAME(rc)));
        goto cleanup;
    }

    /* declare the launch a success */
    failed_launch = false;
    
    if (orte_timing) {
        if (0 != gettimeofday(&launchstop, NULL)) {
            opal_output(0,
                    "%s plm:yarn:plm_yarn_launch_job: could not obtain stop time",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        } else {
            opal_output(0,
                    "%s plm:yarn:plm_yarn_launch_job: total job launch time is %ld usec",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    (launchstop.tv_sec - launchstart.tv_sec) * 1000000
                            + (launchstop.tv_usec - launchstart.tv_usec));
        }
    }

    if (ORTE_SUCCESS != rc) {
        opal_output(0,
                "%s plm:yarn:plm_yarn_launch_job: start_procs returned error %d",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), rc);
        goto cleanup;
    }

cleanup:
    /* check for failed launch - if so, force terminate */
    if (failed_launch) {
        orte_plm_base_launch_failed(failed_job, -1, ORTE_ERROR_DEFAULT_EXIT_CODE, ORTE_JOB_STATE_FAILED_TO_START);
    }
    
    return rc;
}

static void yarn_hnp_sync_recv(int status, orte_process_name_t* sender,
                           opal_buffer_t* buffer, orte_rml_tag_t tag,
                           void* cbdata)
{
    /* get daemon job object */
    orte_job_t* daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);

    yarn_plm_job_t* yarn_job = (yarn_plm_job_t*)cbdata;
    /* get user's job object */
    orte_job_t* jdata = yarn_job->jdata;
    opal_buffer_t *msg;
    int rc;

    yarn_job->num_sync_daemons++;

    /* we got all daemons synced */
    if (daemons->num_procs == yarn_job->num_sync_daemons) {
        OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output,
                        "%s plm:yarn:yarn_hnp_sync_recv: we got all daemons sync, will launch proc in NM",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

        msg = OBJ_NEW(opal_buffer_t);
        if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(ORTE_PROC_MY_NAME->jobid, msg, ORTE_RML_TAG_YARN_SYNC_RESPONSE))) { //ORTE_RML_TAG_DAEMON
            ORTE_ERROR_LOG(rc);
            opal_output(0, "%s plm:yarn:yarn_hnp_sync_recv: failed to send sync response to daemon processes.",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            OBJ_RELEASE(msg);
        }
        OBJ_RELEASE(msg);

        /* here, we actually launch procs */
        rc = plm_yarn_actual_launch_procs(jdata);

        if (ORTE_SUCCESS != rc) {
            orte_plm_base_launch_failed(jdata->jobid, -1, ORTE_ERROR_DEFAULT_EXIT_CODE, ORTE_JOB_STATE_FAILED_TO_START);
        }
    }

    OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output,
                "%s plm:yarn:yarn_hnp_sync_recv: we got [%d/%d] daemons yarn sync request",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), yarn_job->num_sync_daemons, daemons->num_procs));
}


static int plm_yarn_actual_launch_procs(orte_job_t* jdata)
{
    int rc;
    orte_vpid_t idx, i;
    orte_app_context_t* app = NULL;
    orte_proc_t *proc = NULL;

    char **argv;
    char **env;
    bool error_flag = false;
    int launched_proc_num = 0;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output,
                    "%s plm:yarn:launch_apps for job %s",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    ORTE_JOBID_PRINT(jdata->jobid)));


    rc = common_launch_process(jdata, false, 0, &launched_proc_num);

	if (rc != ORTE_SUCCESS) {
		return rc;
	}

	/* if all jdata procs are launched successfully, then modify the job's state */
	if (launched_proc_num == jdata->num_procs) {
		jdata->state = ORTE_JOB_STATE_RUNNING;
		OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output,
						"%s plm:yarn:plm_yarn_actual_launch_procs: launch jdata procs successfully with AM",
						ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
	}

	return ORTE_SUCCESS;
}

static int prepend_nosize(char ***argv, const char *arg)
{
    int argc;
    int i;

    /* Create new argv. */

    if (NULL == *argv) {
        *argv = (char**) malloc(2 * sizeof(char *));
        if (NULL == *argv) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        (*argv)[0] = strdup(arg);
        (*argv)[1] = NULL;
    } else {
        /* count how many entries currently exist */
        argc = opal_argv_count(*argv);
        
        *argv = (char**) realloc(*argv, (argc + 2) * sizeof(char *));
        if (NULL == *argv) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        (*argv)[argc+1] = NULL;

        /* shift all existing elements down 1 */
        for (i=argc; 0 < i; i--) {
            (*argv)[i] = (*argv)[i-1];
        }
        (*argv)[0] = strdup(arg);
    }

    return OPAL_SUCCESS;
}

/*
 * setup env and argv for specified process
 */
static int setup_proc_env_and_argv(orte_job_t* jdata, orte_app_context_t* app,
        orte_proc_t* proc, char ***pargv, char ***penv)
{
    char* param;
    char* param2;
    char* value;
    char* vp_id_str;
    char* job_id_str;
    int rc;
    int i, num_nodes;

    /* obtain app->argv */
    if (!(app->argv)) {
        opal_output(0, "%s plm::yarn::setup_proc_env_and_argv: app->argv is null",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }
    *pargv = opal_argv_copy(app->argv);

    if (ORTE_SUCCESS != orte_util_convert_jobid_to_string(&job_id_str, jdata->jobid)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    if (ORTE_SUCCESS != orte_util_convert_vpid_to_string(&vp_id_str, proc->name.vpid)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    // add stdout, stderr to app
    opal_argv_append_nosize(pargv, "1><LOG_DIR>/stdout");
    opal_argv_append_nosize(pargv, "2><LOG_DIR>/stderr");

    // add java executor to app
    prepend_nosize(pargv, vp_id_str);
    prepend_nosize(pargv, job_id_str);
    prepend_nosize(pargv, "com.pivotal.hamster.yarnexecutor.YarnExecutor");
    prepend_nosize(pargv, "hamster-core.jar");
    prepend_nosize(pargv, "-cp");
    prepend_nosize(pargv, getenv("HAMSTER_JAVA_OPT")==NULL ? "-Xmx32M -Xms8M" : getenv("HAMSTER_JAVA_OPT"));
    prepend_nosize(pargv, "$JAVA_HOME/bin/java");

    /* obtain app->env */
    *penv = opal_environ_merge(environ, app->env);

    if (!proc->node) {
        opal_output(0, "%s plm::yarn::setup_proc_env_and_argv: node of proc[%d] is NULL",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), proc->name.vpid);
        return ORTE_ERROR;
    }

    if (!proc->node->daemon) {
        opal_output(0, "%s plm::yarn::setup_proc_env_and_argv: daemon of node[%s] is NULL",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), proc->node->name);
        return ORTE_ERROR;
    }

    /* set the app_context number into the environment */
    param = mca_base_param_environ_variable("orte","app","num");
    asprintf(&param2, "%ld", (long)app->idx);
    opal_setenv(param, param2, true, penv);
    free(param);
    free(param2);

    // pass the daemon's name
    param = mca_base_param_environ_variable("orte","local_daemon","uri");
    opal_setenv(param, proc->node->daemon->rml_uri, true, penv);
    free(param);

    /* pass my contact info */
    param = mca_base_param_environ_variable("orte","hnp","uri");
    opal_setenv(param, orte_process_info.my_hnp_uri, true, penv);
    free(param);

    /* pass the jobid */
    param = mca_base_param_environ_variable("orte","ess","jobid");
    opal_setenv(param, job_id_str, true, penv);
    free(param);
    free(job_id_str);

    /* pass the rank */
    param = mca_base_param_environ_variable("orte","ess","vpid");
    opal_setenv(param, vp_id_str, true, penv);
    free(param);
    opal_setenv("OMPI_COMM_WORLD_RANK", vp_id_str, true, penv);
    free(vp_id_str);  /* done with this now */

    /* pass local rank */
    asprintf(&value, "%lu", (unsigned long) proc->local_rank);
    opal_setenv("OMPI_COMM_WORLD_LOCAL_RANK", value, true, penv);
    free(value);

    /* pass node rank */
    asprintf(&value, "%lu", (unsigned long) proc->node_rank);
    opal_setenv("OMPI_COMM_WORLD_NODE_RANK", value, true, penv);

    /* set an mca param for it too */
    param = mca_base_param_environ_variable("orte","ess","node_rank");
    opal_setenv(param, value, true, penv);
    free(param);
    free(value);

    /* pass a param telling the child what model of cpu we are on,
     * if we know it
     */
    if (NULL != orte_local_cpu_type) {
        param = mca_base_param_environ_variable("orte","cpu","type");
        /* do not overwrite what the user may have provided */
        opal_setenv(param, orte_local_cpu_type, false, penv);
        free(param);
    }
    if (NULL != orte_local_cpu_model) {
        param = mca_base_param_environ_variable("orte","cpu","model");
        /* do not overwrite what the user may have provided */
        opal_setenv(param, orte_local_cpu_model, false, penv);
        free(param);
    }

    /* pass the number of nodes involved in this job */
    param = mca_base_param_environ_variable("orte","num","nodes");

    /* we have to count the number of nodes as the size of orte_node_pool
     * is only guaranteed to be equal or larger than that number - i.e.,
     * the pointer_array increases the size by a block each time, so some
     * of the locations are left empty
     */
    num_nodes = 0;
    for (i = 0; i < orte_node_pool->size; i++) {
        if (NULL != opal_pointer_array_get_item(orte_node_pool, i)) {
            num_nodes++;
        }
    }
    asprintf(&value, "%d", num_nodes);
    opal_setenv(param, value, true, penv);
    free(param);
    free(value);

    /* setup yield schedule */
    param = mca_base_param_environ_variable("mpi", NULL, "yield_when_idle");
    opal_setenv(param, "0", false, penv);
    free(param);

    /* set MPI universe envar */
    orte_ess_env_put(jdata->num_procs, proc->node->num_procs, penv);
    
    asprintf(&value, "%ld", (long) jdata->num_procs);
    opal_setenv("OMPI_UNIVERSE_SIZE", value, true, penv);
    free(value);

    /* finally, we will set/unset some mca param to select modules */
    opal_unsetenv("OMPI_MCA_plm", penv);
    opal_unsetenv("OMPI_MCA_ras", penv);
    opal_unsetenv("OMPI_MCA_ess", penv);
    opal_unsetenv("OMPI_MCA_errmgr", penv);
    return 0;
}

/**
* Terminate the orteds for a given job
 */
static int plm_yarn_terminate_orteds(void)
{
    int rc;
    orte_job_t *jdata;

    finish_app_master(0 == orte_exit_status);

    /* tell them to die without sending a reply - we will rely on the
     * waitpid to tell us when they have exited!
     */
    if (ORTE_SUCCESS != (rc = orte_plm_base_orted_exit(ORTE_DAEMON_EXIT_CMD))) {
        ORTE_ERROR_LOG(rc);
    }
    
    /* check to see if the primary pid is set. If not, this indicates
     * that we never launched any additional daemons, so we cannot
     * not wait for a waitpid to fire and tell us it's okay to
     * exit. Instead, we simply trigger an exit for ourselves
     */
    if (!primary_pid_set) {
        OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                             "%s plm:slurm: primary daemons complete!",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        jdata = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
        jdata->state = ORTE_JOB_STATE_TERMINATED;
        /*  need to set the #terminated value to avoid an incorrect error msg */
        jdata->num_terminated = jdata->num_procs;
        orte_trigger_event(&orteds_exit);
    }
    
    return rc;
}

static int plm_yarn_terminate_job(orte_jobid_t jobid) {
    finish_app_master(0 == orte_exit_status);
    return orte_plm_base_orted_terminate_job(jobid);
}

/**
 * Signal all the processes in the child srun by sending the signal directly to it
 */
static int plm_yarn_signal_job(orte_jobid_t jobid, int32_t signal)
{
    return ORTE_SUCCESS;
}


static int plm_yarn_finalize(void)
{
    int rc;
    
    /* cleanup any pending recvs */
    if (ORTE_SUCCESS != (rc = orte_plm_base_comm_stop())) {
        ORTE_ERROR_LOG(rc);
    }
    
    return ORTE_SUCCESS;
}
