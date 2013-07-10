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
 * Copyright (c) 2007-2012 Los Alamos National Security, LLC.  All rights
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
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/runtime/orte_quit.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/rmaps.h"
#include "orte/mca/state/state.h"

#include "orte/orted/orted.h"

#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/base/plm_private.h"
#include "plm_yarn.h"


#define ORTE_RML_TAG_YARN_SYNC_REQUEST      97
#define ORTE_RML_TAG_YARN_SYNC_RESPONSE     98

/*
 * Local functions for orte_plm_slurm_module
 */
static int plm_yarn_init(void);
static int plm_yarn_launch_job(orte_job_t *jdata);
static int plm_yarn_terminate_orteds(void);
static int plm_yarn_signal_job(orte_jobid_t jobid, int32_t signal);
static int plm_yarn_finalize(void);


static int plm_slurm_start_proc(int argc, char **argv, char **env,
                                char *prefix);
/*
 * Local utility functions
 */
static void heartbeat_with_AM_cb(int fd, short event, void *data);
static void finish_app_master(bool succeed);

/*
 * Local functions for added job state's called in plm_yarn_init
 */
static int setup_daemon_proc_env_and_argv(orte_proc_t* proc, char ***pargv,
        int *argc, char ***penv);
static int common_launch_process(orte_job_t *jdata, bool launch_daemon, int *launched_proc_num);
static void launch_daemons(int fd, short args, void* cbdata);


static int setup_proc_env_and_argv(orte_job_t* jdata, orte_app_context_t* app,
        orte_proc_t* proc, char ***pargv, char ***penv);
static int plm_yarn_actual_launch_procs(orte_job_t* jdata);
static void yarn_hnp_sync_recv(int status, orte_process_name_t* sender,
                           opal_buffer_t* buffer, orte_rml_tag_t tag,
                           void* cbdata);
static void plm_yarn_launch_apps(int fd, short args, void *cbdata);


static void plm_yarn_cleanup_job(int fd, short args, void *cbdata);
static void plm_yarn_quit(int fd, short args, void* cbdata);

/*
 * Global variable
 */
orte_plm_base_module_1_0_0_t orte_plm_slurm_module = {
	plm_yarn_init,
    orte_plm_base_set_hnp_name,
    plm_yarn_launch_job,
    NULL,
    orte_plm_base_orted_terminate_job,
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
static bool launching_daemons;


extern char** environ;
static int num_sync_daemons = 0;
static int num_completed_daemon_procs = 0;
static int num_completed_jdata_procs = 0;
static bool appmaster_finished = false;
/**
* Init the module
 */
static int plm_yarn_init(void)
{
    int rc;
    
    if (ORTE_SUCCESS != (rc = orte_plm_base_comm_start())) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    /* point to our launch command */
    if (ORTE_SUCCESS != (rc = orte_state.add_job_state(ORTE_JOB_STATE_LAUNCH_DAEMONS,
                                                       launch_daemons, ORTE_SYS_PRI))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    /* point to our launch apps callback */
	if (ORTE_SUCCESS
			!= (rc = orte_state.add_job_state(ORTE_JOB_STATE_LAUNCH_APPS,
					plm_yarn_launch_apps, ORTE_SYS_PRI))) {
		ORTE_ERROR_LOG(rc);
		return rc;
	}

	/* set quit callback */
	if (ORTE_SUCCESS
			!= (rc = orte_state.set_job_state_callback(
					ORTE_JOB_STATE_ALL_JOBS_COMPLETE, plm_yarn_quit))) {
		ORTE_ERROR_LOG(rc);
		opal_output(0,
				"%s plm:yarn:plm_yarn_init: set job state callback with ORTE_JOB_STATE_ALL_JOBS_COMPLETE failed.",
				ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
		exit(rc);
	}

	if (ORTE_SUCCESS
			!= (rc = orte_state.set_job_state_callback(
					ORTE_JOB_STATE_DAEMONS_TERMINATED, plm_yarn_quit))) {
		ORTE_ERROR_LOG(rc);
		opal_output(0,
				"%s plm:yarn:plm_yarn_init: set job state callback with ORTE_JOB_STATE_DAEMONS_TERMINATED failed.",
				ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
		exit(rc);
	}

	if (ORTE_SUCCESS
			!= (rc = orte_state.set_job_state_callback(
					ORTE_JOB_STATE_NOTIFY_COMPLETED, plm_yarn_cleanup_job))) {
		ORTE_ERROR_LOG(rc);
		opal_output(0,
				"%s plm:yarn:init: set job state callback with ORTE_JOB_STATE_NOTIFY_COMPLETED failed",
				ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
		exit(rc);
	}

    return rc;
}


/* When working in this function, ALWAYS jump to "cleanup" if
 * you encounter an error so that orterun will be woken up and
 * the job can cleanly terminate
 */
static int plm_yarn_launch_job(orte_job_t *jdata)
{
	//============heartbeat with AM======
	opal_event_t *ev = NULL;
	ev = (opal_event_t*) malloc(sizeof(opal_event_t));

	struct timeval delay;
	delay.tv_sec = 1;
	delay.tv_usec = 0;

	opal_evtimer_set(ev, heartbeat_with_AM_cb, jdata);
	opal_evtimer_add(ev, &delay);
	//===================================

    if (ORTE_JOB_CONTROL_RESTART & jdata->controls) {
        /* this is a restart situation - skip to the mapping stage */
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP);
    } else {
        /* new job - set it up */
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_INIT);
    }
    return ORTE_SUCCESS;
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
    opal_argv_append_nosize(pargv, vp_id_str);
    opal_argv_append_nosize(pargv, job_id_str);
    opal_argv_append_nosize(pargv, "com.pivotal.hamster.yarnexecutor.YarnExecutor");
    opal_argv_append_nosize(pargv, "hamster-core.jar");
    opal_argv_append_nosize(pargv, "-cp");
    opal_argv_append_nosize(pargv, getenv("HAMSTER_JAVA_OPT")==NULL ? "-Xmx32M -Xms8M" : getenv("HAMSTER_JAVA_OPT"));
    opal_argv_append_nosize(pargv, "$JAVA_HOME/bin/java");

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

    /* set the app_context number into the environment */ //??
    param = mca_base_param_env_var("orte_app_num");
    asprintf(&param2, "%ld", (long)app->idx);
    opal_setenv(param, param2, true, penv);
    free(param);
    free(param2);

    // pass the daemon's name
    param = mca_base_param_env_var("orte_local_daemon_uri");
    opal_setenv(param, proc->node->daemon->rml_uri, true, penv);
    free(param);

    /* pass my contact info */
    param = mca_base_param_env_var("orte_hnp_uri");
    opal_setenv(param, orte_process_info.my_hnp_uri, true, penv);
    free(param);

    /* pass the jobid */
    param = mca_base_param_env_var("orte_ess_jobid");
    opal_setenv(param, job_id_str, true, penv);
    free(param);
    free(job_id_str);

    /* pass the rank */
    param = mca_base_param_environ_variable("orte","ess","vpid");
    param = mca_base_param_env_var("orte_ess_vpid");
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
    param = mca_base_param_env_var("orte_ess_node_rank");
    opal_setenv(param, value, true, penv);
    free(param);
    free(value);

    /* pass a param telling the child what model of cpu we are on,
     * if we know it
     */
    if (NULL != orte_local_cpu_type) {
        param = mca_base_param_env_var("orte_cpu_type");
        /* do not overwrite what the user may have provided */
        opal_setenv(param, orte_local_cpu_type, false, penv);
        free(param);
    }
    if (NULL != orte_local_cpu_model) {
        param = mca_base_param_env_var("orte_cpu_model");
        /* do not overwrite what the user may have provided */
        opal_setenv(param, orte_local_cpu_model, false, penv);
        free(param);
    }

    /* pass the number of nodes involved in this job */
    param = mca_base_param_env_var("orte_num_nodes");

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
    param = mca_base_param_env_var("mpi_yield_when_idle");
    opal_setenv(param, "0", false, penv);
    free(param);

    /* set MPI universe envar */
    orte_ess_env_put(jdata->num_procs, proc->node->num_procs, penv);

    asprintf(&value, "%ld", (long) jdata->num_procs);
    opal_setenv("OMPI_UNIVERSE_SIZE", value, true, penv);
    free(value);

	/* pass collective ids for the std MPI operations */
	param = mca_base_param_env_var("orte_peer_modex_id");
	asprintf(&value, "%d", jdata->peer_modex);
	opal_setenv(param, value, true, penv);
	free(param);
	free(value);

	param = mca_base_param_env_var("orte_peer_init_barrier_id");
	asprintf(&value, "%d", jdata->peer_init_barrier);
	opal_setenv(param, value, true, penv);
	free(param);
	free(value);

	param = mca_base_param_env_var("orte_peer_fini_barrier_id");
	asprintf(&value, "%d", jdata->peer_fini_barrier);
	opal_setenv(param, value, true, penv);
	free(param);
	free(value);

    /* finally, we will set/unset some mca param to select modules */
    opal_unsetenv("OMPI_MCA_plm", penv);
    opal_unsetenv("OMPI_MCA_ras", penv);
    opal_unsetenv("OMPI_MCA_ess", penv);
    opal_unsetenv("OMPI_MCA_state", penv);
    opal_unsetenv("OMPI_MCA_errmgr", penv);
    return 0;
}

static int common_launch_process(orte_job_t *jdata, bool launch_daemon, int *launched_proc_num)
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
	opal_argv_append(argc, pargv, "-mca");
	opal_argv_append(argc, pargv, "orte_hnp_uri");
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


    /* orted */
	opal_argv_append(argc, pargv, "-mca");
	opal_argv_append(argc, pargv, "state");
	opal_argv_append(argc, pargv, "orted");

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

static void launch_daemons(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *state = (orte_state_caddy_t*)cbdata;
    orte_job_map_t *map;
    int rc;
    bool failed_launch = true;
    orte_job_t *daemons;
    int launched_proc_num = 0;

    OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                         "%s plm:yarn:launch_daemons: LAUNCH DAEMONS CALLED",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* if we are launching debugger daemons, then just go
     * do it - no new daemons will be launched
     */
    if (ORTE_JOB_CONTROL_DEBUGGER_DAEMON & state->jdata->controls) {
        state->jdata->state = ORTE_JOB_STATE_DAEMONS_LAUNCHED;
        ORTE_ACTIVATE_JOB_STATE(state->jdata, ORTE_JOB_STATE_DAEMONS_REPORTED);
        OBJ_RELEASE(state);
        return;
    }

    /* start by setting up the virtual machine */
    daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    if (ORTE_SUCCESS != (rc = orte_plm_base_setup_virtual_machine(state->jdata))) {
        ORTE_ERROR_LOG(rc);
        goto cleanup;
    }

   /* if we don't want to launch, then don't attempt to
     * launch the daemons - the user really wants to just
     * look at the proposed process map
     */
    if (orte_do_not_launch) {
        /* set the state to indicate the daemons reported - this
         * will trigger the daemons_reported event and cause the
         * job to move to the following step
         */
        state->jdata->state = ORTE_JOB_STATE_DAEMONS_LAUNCHED;
        ORTE_ACTIVATE_JOB_STATE(state->jdata, ORTE_JOB_STATE_DAEMONS_REPORTED);
        OBJ_RELEASE(state);
        return;
    }

    /* Get the map for this job */
    if (NULL == (map = daemons->map)) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        rc = ORTE_ERR_NOT_FOUND;
        goto cleanup;
    }
        
    if (0 == map->num_new_daemons) {
        /* set the state to indicate the daemons reported - this
         * will trigger the daemons_reported event and cause the
         * job to move to the following step
         */
        OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                             "%s plm:yarn:launch_daemons: no new daemons to launch",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        state->jdata->state = ORTE_JOB_STATE_DAEMONS_LAUNCHED;
        if (ORTE_JOB_STATE_DAEMONS_REPORTED == daemons->state) {
            ORTE_ACTIVATE_JOB_STATE(state->jdata, ORTE_JOB_STATE_DAEMONS_REPORTED);
        }
        OBJ_RELEASE(state);
        return;
    }

	rc = common_launch_process(daemons, true, &launched_proc_num);
	if (rc != ORTE_SUCCESS) {
		ORTE_ERROR_LOG(rc);
		goto cleanup;
	}

	/* if all daemon procs are launched successfully, then modify the job's state */
	if (launched_proc_num == (daemons->num_procs - 1)) {
		/* indicate that the daemons for this job were launched */
		state->jdata->state = ORTE_JOB_STATE_DAEMONS_LAUNCHED;
		daemons->state = ORTE_JOB_STATE_DAEMONS_LAUNCHED;
		OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output,
						"%s plm:yarn:launch_daemons: launch daemon proc successfully with AM",
						ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
	}

    /* flag that launch was successful, so far as we currently know */
    failed_launch = false;

cleanup:
    /* cleanup the caddy */
    OBJ_RELEASE(state);

    /* check for failed launch - if so, force terminate */
    if (failed_launch) {
        ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
    }
}

static int plm_yarn_actual_launch_procs(orte_job_t* jdata)
{
    int rc;

    int launched_proc_num = 0;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output,
                    "%s plm:yarn:launch_apps for job %s",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    ORTE_JOBID_PRINT(jdata->jobid)));


    rc = common_launch_process(jdata, false, &launched_proc_num);

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

static void yarn_hnp_sync_recv(int status, orte_process_name_t* sender,
                           opal_buffer_t* buffer, orte_rml_tag_t tag,
                           void* cbdata)
{
    /* get daemon job object */
    orte_job_t* daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    /* get user's job object */
    orte_job_t* jdata = (orte_job_t*)cbdata;
    opal_buffer_t *msg;
    int rc;

    num_sync_daemons++;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output,
                "%s plm:yarn:yarn_hnp_sync_recv: we got [%d/%d] daemons yarn sync request",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), num_sync_daemons, daemons->num_procs));

    /* we got all daemons synced */
    if (daemons->num_procs == num_sync_daemons) {
        OPAL_OUTPUT_VERBOSE((5, orte_plm_globals.output,
                        "%s plm:yarn:yarn_hnp_sync_recv: we got all daemons sync, will launch proc in NM",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

        msg = OBJ_NEW(opal_buffer_t);
        if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(ORTE_PROC_MY_NAME->jobid, msg, ORTE_RML_TAG_YARN_SYNC_RESPONSE))) {
            ORTE_ERROR_LOG(rc);
            opal_output(0, "%s plm:yarn:yarn_hnp_sync_recv: failed to send sync response to daemon processes.",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            jdata->state = ORTE_JOB_STATE_FAILED_TO_LAUNCH;
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_FAILED_TO_LAUNCH);
            OBJ_RELEASE(msg);
        }
        OBJ_RELEASE(msg);

        /* here, we actually launch procs */
        plm_yarn_actual_launch_procs(jdata);
    }
}

static void plm_yarn_launch_apps(int fd, short args, void *cbdata) {
    int rc;
    orte_job_t *jdata;
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;

    /* convenience */
    jdata = caddy->jdata;

    if (ORTE_JOB_STATE_LAUNCH_APPS != caddy->job_state) {
        ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        return;
    }

    /* update job state */
    jdata->state = caddy->job_state;

    /* register recv callback for daemons sync request */
    if (ORTE_SUCCESS != (rc = orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                                                      ORTE_RML_TAG_YARN_SYNC_REQUEST,
                                                      ORTE_RML_PERSISTENT,
                                                      yarn_hnp_sync_recv, jdata))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    orte_plm_base_launch_apps(fd, args, cbdata);
}


static void heartbeat_with_AM_cb(int fd, short event, void *data)
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
        opal_evtimer_set(ev, heartbeat_with_AM_cb, jdata);
        opal_evtimer_add(ev, &delay);
    }
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

/**
* Terminate the orteds for a given job
 */
static int plm_yarn_terminate_orteds(void) {
	finish_app_master(0 == orte_exit_status);
    orte_job_t* jdata = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    if (ORTE_JOB_STATE_DAEMONS_TERMINATED != jdata->state) {
		/* need to set the #terminated value to avoid an incorrect error msg */
		jdata->num_terminated = jdata->num_procs;
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_DAEMONS_TERMINATED);
    }
    return ORTE_SUCCESS;
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
