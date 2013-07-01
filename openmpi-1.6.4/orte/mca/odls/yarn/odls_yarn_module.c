/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2010 Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2007      Evergrid, Inc. All rights reserved.
 * Copyright (c) 2008-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <errno.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif
#include <signal.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif  /* HAVE_SYS_STAT_H */
#ifdef HAVE_STDARG_H
#include <stdarg.h>
#endif
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include <dirent.h>
#include <sys/stat.h>
#include <stdio.h>

#include "opal/mca/hwloc/hwloc.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/util/opal_environ.h"
#include "opal/util/show_help.h"
#include "opal/util/fd.h"
#include "orte/mca/rml/rml.h"
#include "orte/util/show_help.h"
#include "orte/runtime/orte_wait.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/plm/base/base.h"
#include "orte/mca/iof/base/iof_base_setup.h"
#include "orte/mca/plm/plm.h"
#include "orte/util/name_fns.h"
#include "orte/mca/iof/iof.h"

#include "orte/mca/odls/base/base.h"
#include "orte/mca/odls/base/odls_private.h"
#include "orte/mca/odls/yarn/odls_yarn.h"

#define ORTE_RML_TAG_YARN_SYNC_REQUEST      97
#define ORTE_RML_TAG_YARN_SYNC_RESPONSE     98

static const int WAIT_TIME_MICRO_SEC = 100 * 1000;            // 0.1 s
static const int MAX_WAIT_TIME_MICRO_SEC = 120 * 1000 * 1000; // 120 s

/*
 * Module functions (function pointers used in a struct)
 */
static int orte_odls_yarn_launch_local_procs(opal_buffer_t *data);

/*
 * Local Functions
 */
static void local_process_state_monitor_cb(int fd, short args, void *cbdata);

static int orte_odls_yarn_kill_local_procs(opal_pointer_array_t *procs, bool set_state);

static int orte_odls_yarn_signal_local_procs(const orte_process_name_t *proc, int32_t signal);

static int orte_odls_yarn_deliver_message(orte_jobid_t job, opal_buffer_t *buffer, orte_rml_tag_t tag);

static void wait_process_completed(int fd, short event, void* cbdata);

static void monitor_local_launch(int fd, short event, void* cbdata);

static void handle_proc_exit(orte_odls_child_t* child, int32_t status);

static void orte_base_check_proc_complete(orte_odls_child_t *child);

static orte_vpid_t get_vpid_from_err_file(const char* filename);

static orte_vpid_t get_vpid_from_normal_file(const char* filename);

static int pack_state_update(opal_buffer_t *alert, bool include_startup_info, orte_odls_job_t *jobdat);

static bool any_live_children(orte_jobid_t job);

typedef struct {
    int detected_proc_num;
    int total_wait_time;
    orte_jobid_t jobid;
    orte_odls_job_t* jobdat;
} yarn_local_monitor_t;

/*
 * Module
 */
orte_odls_base_module_t orte_odls_yarn_module = {
        orte_odls_base_default_get_add_procs_data,
        orte_odls_yarn_launch_local_procs, 
        orte_odls_yarn_kill_local_procs, //kill_local_procs,
        orte_odls_yarn_signal_local_procs, //signal_local_procs,
        orte_odls_yarn_deliver_message,
        orte_odls_base_default_require_sync,
};

int orte_odls_yarn_deliver_message(orte_jobid_t job, opal_buffer_t *buffer, orte_rml_tag_t tag)
{
    int rc;
    opal_list_item_t *item;
    orte_odls_child_t *child;
    
    /* protect operations involving the global list of children */
    OPAL_THREAD_LOCK(&orte_odls_globals.mutex);
    
    for (item = opal_list_get_first(&orte_local_children);
         item != opal_list_get_end(&orte_local_children);
         item = opal_list_get_next(item)) {
        child = (orte_odls_child_t*)item;
        
        /* do we have a child from the specified job. Because the
         *  job could be given as a WILDCARD value, we must use
         *  the dss.compare function to check for equality.
         */
        if (OPAL_EQUAL != opal_dss.compare(&job, &(child->name->jobid), ORTE_JOBID)) {
            continue;
        }
        
        OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                             "%s odls: sending message to tag %lu on child %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             (unsigned long)tag, ORTE_NAME_PRINT(child->name)));
        
        /* if so, send the message */
        rc = orte_rml.send_buffer(child->name, buffer, tag, 0);
        if (rc < 0 && rc != ORTE_ERR_ADDRESSEE_UNKNOWN) {
            /* ignore if the addressee is unknown as a race condition could
             * have allowed the child to exit before we send it a barrier
             * due to the vagaries of the event library
             */
            ORTE_ERROR_LOG(rc);
        }
    }
    
    opal_condition_signal(&orte_odls_globals.cond);
    OPAL_THREAD_UNLOCK(&orte_odls_globals.mutex);
    return ORTE_SUCCESS;
}

static int orte_odls_yarn_kill_local_procs(opal_pointer_array_t *procs, bool set_state)
{
    // do nothing
    return ORTE_SUCCESS;
}

static int orte_odls_yarn_signal_local_procs(const orte_process_name_t *proc, int32_t signal)
{
    // do nothing
    return ORTE_SUCCESS;
}


static void yarn_daemon_sync_recv(int status, orte_process_name_t* sender,
                           opal_buffer_t* buffer, orte_rml_tag_t tag,
                           void* cbdata)
{
    OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                     "%s odls:yarn:yarn_daemon_sync_recv: recved sync response from hnp, start tracking proc state",
                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /*
     * since local procs are launched by YARN NM, we here just to check if procs are REALLY launched 
     */
    monitor_local_launch(0, 0, cbdata);
}


static orte_odls_child_t* get_local_child_by_id(orte_jobid_t jobid, orte_vpid_t vpid) {
    opal_list_item_t *item;
    orte_odls_child_t* child;
    for (item = opal_list_get_first(&orte_local_children); 
        item != opal_list_get_end(&orte_local_children);
        item = opal_list_get_next(item)) {
        child = (orte_odls_child_t*)item;
        if ((child->name->jobid == jobid) && (child->name->vpid == vpid)) {
            return child;
        }
    }
    return NULL;
}

static int get_num_proc_in_local_job(orte_jobid_t jobid) {
    opal_list_item_t *item;
    orte_odls_child_t* child;
    int num_local_procs = 0;
    for (item = opal_list_get_first(&orte_local_children); 
        item != opal_list_get_end(&orte_local_children);
        item = opal_list_get_next(item)) {
        child = (orte_odls_child_t*)item;
        if (child->name->jobid == jobid) {
            num_local_procs++;
        }
    }
    return num_local_procs;
}

static void monitor_local_launch(int fd, short event, void* cbdata) {
    yarn_local_monitor_t* mon = (yarn_local_monitor_t*)cbdata;
    bool other_error = false; // other error (like cannot find proc correctly) will cause launch failed
    int rc;
    opal_list_item_t *item;
    orte_odls_child_t* child;
    orte_vpid_t vpid;
    int total_local_children_num = get_num_proc_in_local_job(mon->jobid);
    struct dirent *ent;

    OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                 "%s odls:yarn: enter monitor_local_launch, we need [%d] local children, now we have [%d] launched",
                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                 total_local_children_num,
                 mon->detected_proc_num));

    // do monitoring logic
    // get processes pid path
    char* pid_root = getenv("HAMSTER_PID_ROOT");
    if (!pid_root) {
        pid_root = "/tmp/hamster-pid";
    }
    char pid_dir[1024];
    strcpy(pid_dir, pid_root);
    int path_len = strlen(pid_dir);
    sprintf(pid_dir + path_len, "/%u", mon->jobid);

    // see files in pid path
    DIR* dirp = opendir(pid_dir);
    int new_detected_proc = 0;
    
    if (dirp) {
        while ((ent = readdir(dirp)) != NULL) {
            if ((ent->d_name) && (strlen(ent->d_name) > 0)) {
                // if ent->d_name[0] not a valid number, skip it
                if (ent->d_name[0] > '9' || ent->d_name[0] < '0') {
                    continue;
                }

                // error when fork child process
                if (strstr(ent->d_name, "_err")) {
                    vpid = get_vpid_from_err_file(ent->d_name);
                    child = get_local_child_by_id(mon->jobid, vpid);
                    if (!child) {
                        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                        other_error = true;
                        goto MOVEON;
                    }
                    if (child->state < ORTE_PROC_STATE_UNTERMINATED) {
                        child->alive = false;
                        child->state = ORTE_PROC_STATE_FAILED_TO_START;
                        mon->detected_proc_num++;
                        new_detected_proc++;
                    }
                    continue;
                }

                // not error, get filename/length
                vpid = get_vpid_from_normal_file(ent->d_name);
                child = get_local_child_by_id(mon->jobid, vpid);
                if (!child) {
                    ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                    other_error = true;
                    goto MOVEON;
                }

                // skip if the proc is already launched
                if (child->state >= ORTE_PROC_STATE_LAUNCHED) {
                    continue;
                }

                // we will think this is a new launched proc
                mon->detected_proc_num++;
                new_detected_proc++;
                child->alive = true;
                child->state = ORTE_PROC_STATE_LAUNCHED;
            }
        }
        closedir(dirp);
    }

    if (new_detected_proc > 0) {
        OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                     "%s odls:yarn we have [%d] local_children, now [%d] are detected",
                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), total_local_children_num, mon->detected_proc_num));
    }

    mon->total_wait_time += WAIT_TIME_MICRO_SEC;

    if (mon->total_wait_time > MAX_WAIT_TIME_MICRO_SEC) {
        opal_output(0, "%s odls:yarn timed out for wait local proc launched",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        ORTE_ERROR_LOG(ORTE_ERR_TIMEOUT);
        other_error = true;
        goto MOVEON;
    }

    if (mon->detected_proc_num < total_local_children_num) {
        // start to query finished processes, every 1 sec
        opal_event_t* ev = NULL;
        ev = (opal_event_t*)malloc(sizeof(opal_event_t));
        opal_evtimer_set(ev, monitor_local_launch, mon);
        struct timeval delay;
        delay.tv_sec = 0;
        delay.tv_usec = WAIT_TIME_MICRO_SEC;
        opal_evtimer_add(ev, &delay);
        return;
    }
    free(mon);

MOVEON:
    // we will mark all proc not launched to LAUNCH_FAILED when this error happened
    if (other_error) {
        for (item = opal_list_get_first(&orte_local_children); 
            item != opal_list_get_end(&orte_local_children);
            item = opal_list_get_next(item)) {
            child = (orte_odls_child_t*)item;
            if (child->state < ORTE_PROC_STATE_LAUNCHED) {
opal_output(0, "!!!! failed to start");
ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                child->state = ORTE_PROC_STATE_FAILED_TO_START;
            }
        }
    }

    opal_buffer_t alert;

    // send launch app report to HNP
    OBJ_CONSTRUCT(&alert, opal_buffer_t);
    if (ORTE_SUCCESS != (rc = pack_state_update(&alert, true, mon->jobdat))) {
        ORTE_ERROR_LOG(rc);
    }

    // send callback to HNP
    if (ORTE_PROC_IS_HNP) {
        OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                             "%s odls:yarn flagging launch report to myself",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        ORTE_MESSAGE_EVENT(ORTE_PROC_MY_NAME, &alert,
                           ORTE_RML_TAG_APP_LAUNCH_CALLBACK,
                           orte_plm_base_app_report_launch);
    } else {
        OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                             "%s odls:yarn sending launch report to %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(ORTE_PROC_MY_HNP)));
        /* go ahead and send the update to the HNP */
        if (0 > (rc = orte_rml.send_buffer(ORTE_PROC_MY_HNP, &alert, ORTE_RML_TAG_APP_LAUNCH_CALLBACK, 0))) {
            ORTE_ERROR_LOG(rc);
        }
    }

    // cleanup
    OBJ_DESTRUCT(&alert);

    // start to query finished processes, every 1 sec
    opal_event_t* ev = NULL;
    ev = (opal_event_t*)malloc(sizeof(opal_event_t));
    opal_evtimer_set(ev, wait_process_completed, mon->jobid);
    struct timeval delay;
    delay.tv_sec = 1;
    delay.tv_usec = 0;
    opal_evtimer_add(ev, &delay);
}

static int pack_state_for_proc(opal_buffer_t *alert, bool include_startup_info, orte_odls_child_t *child)
{
    int rc;
    
    /* pack the child's vpid */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &(child->name->vpid), 1, ORTE_VPID))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    /* pack startup info if we need to report it */
    if (include_startup_info) {
        if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &child->pid, 1, OPAL_PID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* if we are timing things, pack the time the proc was launched */
        if (orte_timing) {
            int64_t tmp;
            tmp = child->starttime.tv_sec;
            if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &tmp, 1, OPAL_INT64))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            tmp = child->starttime.tv_usec;
            if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &tmp, 1, OPAL_INT64))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }
    }
    /* pack its state */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &child->state, 1, ORTE_PROC_STATE))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    /* pack its exit code */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &child->exit_code, 1, ORTE_EXIT_CODE))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    
    return ORTE_SUCCESS;
}

/*
 * simply copied from odls_base_fns, because we cannot access it
 */
static int pack_state_update(opal_buffer_t *alert, bool include_startup_info, orte_odls_job_t *jobdat)
{
    int rc;
    opal_list_item_t *item;
    orte_odls_child_t *child;
    orte_vpid_t null=ORTE_VPID_INVALID;
    
    /* pack the jobid */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &jobdat->jobid, 1, ORTE_JOBID))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    /* if we are timing things, pack the time the launch msg for this job was recvd */
    if (include_startup_info && orte_timing) {
        int64_t tmp;
        tmp = jobdat->launch_msg_recvd.tv_sec;
        if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &tmp, 1, OPAL_INT64))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        tmp = jobdat->launch_msg_recvd.tv_usec;
        if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &tmp, 1, OPAL_INT64))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    }
    for (item = opal_list_get_first(&orte_local_children);
         item != opal_list_get_end(&orte_local_children);
         item = opal_list_get_next(item)) {
        child = (orte_odls_child_t*)item;
        /* if this child is part of the job... */
        if (child->name->jobid == jobdat->jobid) {
            if (ORTE_SUCCESS != (rc = pack_state_for_proc(alert, include_startup_info, child))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }
    }
    /* flag that this job is complete so the receiver can know */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &null, 1, ORTE_VPID))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    
    return ORTE_SUCCESS;
}


static void wait_process_completed(int fd, short event, void* cbdata) {
    bool other_error = false; // other error (like cannot find proc correctly) will cause launch failed
    int rc;
    opal_list_item_t *item;
    orte_odls_child_t* child;
    orte_jobid_t jobid = (orte_jobid_t)cbdata;
    orte_vpid_t vpid;
    int completed_proc_num = 0;
    struct dirent *ent;

    // do monitoring logic
    // get processes pid path
    char* pid_root = getenv("HAMSTER_PID_ROOT");
    if (!pid_root) {
        pid_root = "/tmp/hamster-pid";
    }
    char pid_dir[1024];
    strcpy(pid_dir, pid_root);
    int path_len = strlen(pid_dir);
    sprintf(pid_dir + path_len, "/%u", jobid);

    // see files in pid path
    DIR* dirp = opendir(pid_dir);
    int new_detected_proc = 0;
    
    // loop for pid-files and see if any process completed    
    if (dirp) {
        while ((ent = readdir(dirp)) != NULL) {
            if ((ent->d_name) && (strlen(ent->d_name) > 0)) {
                // if ent->d_name[0] not a valid number, skip it
                if (ent->d_name[0] > '9' || ent->d_name[0] < '0') {
                    continue;
                }

                // if we cannot find this proc, it's a wired error, but we will
                // leave a log and ignore it
                vpid = get_vpid_from_normal_file(ent->d_name);
                child = get_local_child_by_id(jobid, vpid);
                if (!child) {
                    ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                    continue;
                }

                // skip if the proc is already finished
                if (child->state > ORTE_PROC_STATE_UNTERMINATED) {
                    continue;
                }

                // get length of file
                char full_file_name[4096];
                sprintf(full_file_name, "%s/%s", pid_dir, ent->d_name);
                int exit_status = -1000; 

                struct stat st;
                bool read_pidfile_failed = false;
                if (0 != (rc = stat(full_file_name, &st))) {
                    // read file failed
                    opal_output(0, "%s odls:yarn failed to get size of pid file, path:%s", 
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), full_file_name);
                    ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                } else if (st.st_size == 0) {
                    // still not finished, skip it
                    continue;
                } else if (st.st_size > 0) {
                    if (child->state <= ORTE_PROC_STATE_UNTERMINATED) {
                        FILE * fp;
                        fp = fopen(full_file_name, "r");
                        if (fp == NULL) {
                            opal_output(0, "%s odls:yarn:local_process_state_monitor_cb: failed to open file, %s", 
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), full_file_name);
                            ORTE_ERROR_LOG(ORTE_ERR_FILE_READ_FAILURE);
                            read_pidfile_failed = true;
                        }

                        if (!read_pidfile_failed) {
                            char line[1024];
                            if (!fgets(line, 1024, fp)) {
                                opal_output(0, "%s odls:yarn:local_process_state_monitor_cb: failed to read file content, %s", 
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), full_file_name);
                                ORTE_ERROR_LOG(ORTE_ERR_FILE_READ_FAILURE);
                                read_pidfile_failed = true;
                            }
                            fclose(fp);

                            if (!read_pidfile_failed) {
                                exit_status = atoi(line);
                            }
                        }
                        
                    }
                }
                new_detected_proc++;
                handle_proc_exit(child, exit_status);
            }
        }
        closedir(dirp);
    }

    if (new_detected_proc > 0) {
        OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                     "%s odls:yarn #%d proc completed in this turn",
                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), new_detected_proc));
    }

MOVEON:
    
    // register another check if any living children exist
    if (any_live_children(jobid)) {
        // start process to query finished processes
        opal_event_t* ev = NULL;
        ev = (opal_event_t*)malloc(sizeof(opal_event_t));
        opal_evtimer_set(ev, wait_process_completed, jobid);
        struct timeval delay;
        delay.tv_sec = 1;
        delay.tv_usec = 0;
        opal_evtimer_add(ev, &delay);
    }
}

static void handle_proc_exit(orte_odls_child_t* child, int32_t status) {
    opal_list_item_t *item;
    orte_odls_child_t* chd;

    OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                     "%s odls:waitpid_fired noticed child %s with exit status %d",
                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                     ORTE_NAME_PRINT(child->name), status));

    /* if the child was previously flagged as dead, then just
     * ensure that its exit state gets reported to avoid hanging
     */
    if (!child->alive) {
        OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                             "%s odls:waitpid_fired child %s was already dead",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(child->name)));
        goto MOVEON;
    }

    /* this is our internal error, because of
     * 1) we failed to fetch pid file to fetch exit_code
     */
    if (status == -1000) {
        OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                     "%s odls:waitpid_fired failed to fetch child %s status",
                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                     ORTE_NAME_PRINT(child->name)));
        child->state = ORTE_PROC_STATE_ABORTED;
        goto MOVEON;
    }
    
    /* determine the state of this process */
    if(WIFEXITED(status)) {
        /* set the exit status appropriately */
        child->exit_code = WEXITSTATUS(status);
        
        /* okay, it terminated normally - check to see if a sync was required and
         * if it was received
         */
        if (child->init_recvd) {
            if (!child->fini_recvd) {
                /* we required a finalizing sync and didn't get it, so this
                 * is considered an abnormal termination and treated accordingly
                 */
                child->state = ORTE_PROC_STATE_TERM_WO_SYNC;
                
                OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                                     "%s odls:waitpid_fired child process %s terminated normally "
                                     "but did not provide a required finalize sync - it "
                                     "will be treated as an abnormal termination",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(child->name)));
                
                goto MOVEON;
            }
            /* if we did recv a finalize sync, then it terminated normally */
            child->state = ORTE_PROC_STATE_TERMINATED;
        } else {
            /* has any child in this job already registered? */
            for (item = opal_list_get_first(&orte_local_children);
                 item != opal_list_get_end(&orte_local_children);
                 item = opal_list_get_next(item)) {
                chd = (orte_odls_child_t*)item;
                
                if (chd->init_recvd) {
                    /* someone has registered, and we didn't before
                     * terminating - this is an abnormal termination
                     */
                    child->state = ORTE_PROC_STATE_TERM_WO_SYNC;
                    OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                                         "%s odls:waitpid_fired child process %s terminated normally "
                                         "but did not provide a required init sync - it "
                                         "will be treated as an abnormal termination",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         ORTE_NAME_PRINT(child->name)));
                    
                    goto MOVEON;
                }
            }
            /* if no child has registered, then it is possible that
             * none of them will. This is considered acceptable
             */
            child->state = ORTE_PROC_STATE_TERMINATED;
        }
        
        OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                             "%s odls:waitpid_fired child process %s terminated normally",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(child->name)));
    } else {
        /* the process was terminated with a signal! That's definitely
         * abnormal, so indicate that condition
         */
        child->state = ORTE_PROC_STATE_ABORTED_BY_SIG;
        /* If a process was killed by a signal, then make the
         * exit code of orterun be "signo + 128" so that "prog"
         * and "orterun prog" will both yield the same exit code.
         *
         * This is actually what the shell does for you when
         * a process dies by signal, so this makes orterun treat
         * the termination code to exit status translation the
         * same way
         */
        child->exit_code = WTERMSIG(status) + 128;
        
        OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                             "%s odls:waitpid_fired child process %s terminated with signal",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(child->name)));
    }
    
MOVEON:
    /* indicate the waitpid fired */
    child->waitpid_recvd = true;
    child->iof_complete = true;

    /* check for everything complete */
    orte_base_check_proc_complete(child);
}

/* this is copied from odls_base_default_fns.c, because we cannot access it */
static bool any_live_children(orte_jobid_t job)
{
    opal_list_item_t *item;
    orte_odls_child_t *child;

    /* the thread is locked elsewhere - don't try to do it again here */
    
    for (item = opal_list_get_first(&orte_local_children);
         item != opal_list_get_end(&orte_local_children);
         item = opal_list_get_next(item)) {
        child = (orte_odls_child_t*)item;
        
        /* is this child part of the specified job? */
        if ((job == child->name->jobid || ORTE_JOBID_WILDCARD == job) &&
            child->alive) {
            return true;
        }
    }
    
    /* if we get here, then nobody is left alive from that job */
    return false;
}

/* this is copied from odls_base_default_fns.c, because we cannot access it */
static void orte_base_check_proc_complete(orte_odls_child_t *child)
{
    int rc;
    opal_buffer_t alert;
    orte_plm_cmd_flag_t cmd=ORTE_PLM_UPDATE_PROC_STATE;
    opal_list_item_t *item, *next;
    orte_odls_job_t *jdat;
    
    /* is this proc fully complete? */
    if (!child->waitpid_recvd || !child->iof_complete) {
        /* apparently not - just return */
        return;
    }
    
    /* CHILD IS COMPLETE */
    child->alive = false;
    
    /* Release only the stdin IOF file descriptor for this child, if one
     * was defined. File descriptors for the other IOF channels - stdout,
     * stderr, and stddiag - were released when their associated pipes
     * were cleared and closed due to termination of the process
     */
    orte_iof.close(child->name, ORTE_IOF_STDIN);
    
    /* Clean up the session directory as if we were the process
     * itself.  This covers the case where the process died abnormally
     * and didn't cleanup its own session directory.
     */
    orte_session_dir_finalize(child->name);
    
    /* setup the alert buffer */
    OBJ_CONSTRUCT(&alert, opal_buffer_t);
    
    /* find the jobdat */
    jdat = NULL;
    for (item = opal_list_get_first(&orte_local_jobdata);
         item != opal_list_get_end(&orte_local_jobdata);
         item = opal_list_get_next(item)) {
        jdat = (orte_odls_job_t*)item;
        
        /* is this the specified job? */
        if (jdat->jobid == child->name->jobid) {
            break;
        }
    }
    if (NULL == jdat) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        goto unlock;
    }
    /* decrement the num_local_procs as this one is complete */
    jdat->num_local_procs--;

    /* if the proc aborted, tell the HNP right away */
    if (ORTE_PROC_STATE_TERMINATED != child->state) {
        /* pack update state command */
        if (ORTE_SUCCESS != (rc = opal_dss.pack(&alert, &cmd, 1, ORTE_PLM_CMD))) {
            ORTE_ERROR_LOG(rc);
            goto unlock;
        }
        /* pack only the data for this proc - have to start with the jobid
         * so the receiver can unpack it correctly
         */
        if (ORTE_SUCCESS != (rc = opal_dss.pack(&alert, &child->name->jobid, 1, ORTE_JOBID))) {
            ORTE_ERROR_LOG(rc);
            goto unlock;
        }
        /* now pack the child's info */
        if (ORTE_SUCCESS != (rc = pack_state_for_proc(&alert, false, child))) {
            ORTE_ERROR_LOG(rc);
            goto unlock;
        }
        
        /* remove the child from our local list as it is no longer alive */
        opal_list_remove_item(&orte_local_children, &child->super);

        OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                             "%s odls:proc_complete reporting proc %s aborted to HNP",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(child->name)));
        
        /* release the child object */
        OBJ_RELEASE(child);

        /* if we are the HNP, then we would rather not send this to ourselves -
         * instead, we queue it up for local processing
         */
        if (ORTE_PROC_IS_HNP) {
            ORTE_MESSAGE_EVENT(ORTE_PROC_MY_NAME, &alert,
                               ORTE_RML_TAG_PLM,
                               orte_plm_base_receive_process_msg);
        } else {
            /* go ahead and send it */
            if (0 > (rc = orte_rml.send_buffer(ORTE_PROC_MY_HNP, &alert, ORTE_RML_TAG_PLM, 0))) {
                ORTE_ERROR_LOG(rc);
                goto unlock;
            }
        }
    } else {
        /* since it didn't abort, let's see if all of that job's procs are done */
        if (!any_live_children(child->name->jobid)) {
            /* all those children are dead - alert the HNP */
            /* pack update state command */
            if (ORTE_SUCCESS != (rc = opal_dss.pack(&alert, &cmd, 1, ORTE_PLM_CMD))) {
                ORTE_ERROR_LOG(rc);
                goto unlock;
            }
            /* pack the data for the job */
            if (ORTE_SUCCESS != (rc = pack_state_update(&alert, false, jdat))) {
                ORTE_ERROR_LOG(rc);
                goto unlock;
            }
            
            OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                                 "%s odls:proc_complete reporting all procs in %s terminated",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_JOBID_PRINT(jdat->jobid)));
            
            /* remove all of this job's children from the global list - do not lock
             * the thread as we are already locked
             */
            for (item = opal_list_get_first(&orte_local_children);
                 item != opal_list_get_end(&orte_local_children);
                 item = next) {
                child = (orte_odls_child_t*)item;
                
                next = opal_list_get_next(item);
                
                if (jdat->jobid == child->name->jobid) {
                    opal_list_remove_item(&orte_local_children, &child->super);
                    OBJ_RELEASE(child);
                }
            }

            /* ensure the job's local session directory tree is removed */
            orte_session_dir_cleanup(jdat->jobid);
            
            /* remove this job from our local job data since it is complete */
            opal_list_remove_item(&orte_local_jobdata, &jdat->super);
            OBJ_RELEASE(jdat);
            
            /* if we are the HNP, then we would rather not send this to ourselves -
             * instead, we queue it up for local processing
             */
            if (ORTE_PROC_IS_HNP) {
                ORTE_MESSAGE_EVENT(ORTE_PROC_MY_NAME, &alert,
                                   ORTE_RML_TAG_PLM,
                                   orte_plm_base_receive_process_msg);
            } else {
                /* go ahead and send it */
                if (0 > (rc = orte_rml.send_buffer(ORTE_PROC_MY_HNP, &alert, ORTE_RML_TAG_PLM, 0))) {
                    ORTE_ERROR_LOG(rc);
                    goto unlock;
                }
            }
        }
    }
    
unlock:
    OBJ_DESTRUCT(&alert);
}

/*
 * Callback when non-blocking RML send completes.
 */
static void send_cb(int status, orte_process_name_t *peer,
                          opal_buffer_t *buf, orte_rml_tag_t tag,
                          void *cbdata)
{
    /* nothing to do here - just release buffer and return */
    OBJ_RELEASE(buf);
}

/**
 * Launch all processes allocated to the current node.
 */
static int orte_odls_yarn_launch_local_procs(opal_buffer_t *data)
{
    int rc, i;
    opal_buffer_t *msg;
    opal_list_item_t *item;
    orte_odls_child_t *child;
    orte_jobid_t jobid;
    orte_odls_job_t* jobdat;

    /* construct the list of children we are to launch */
    if (ORTE_SUCCESS != (rc = orte_odls_base_default_construct_child_list(data, &jobid))) {
        opal_output(0, "%s odls:yarn:orte_odls_yarn_launch_local_procs: failed to construct child list on error %s",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_ERROR_NAME(rc));
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    /* find the jobdat for this job */
    jobdat = NULL;
    for (item = opal_list_get_first(&orte_local_jobdata);
         item != opal_list_get_end(&orte_local_jobdata);
         item = opal_list_get_next(item)) {
        jobdat = (orte_odls_job_t*)item;
        
        /* is this the specified job? */
        if (jobdat->jobid == jobid) {
            break;
        }
    }
    if (NULL == jobdat) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        rc = ORTE_ERR_NOT_FOUND;
        return rc;
    }

    /* mark all local children to alive make xcast behave correct */
    for (item = opal_list_get_first(&orte_local_children);
         item != opal_list_get_end(&orte_local_children);
         item = opal_list_get_next(item)) {
        child = (orte_odls_child_t*)item;
        child->alive = true;
        child->state = ORTE_PROC_STATE_INIT;
    }

    yarn_local_monitor_t* mon = (yarn_local_monitor_t*)malloc(sizeof(yarn_local_monitor_t));
    mon->detected_proc_num = 0;
    mon->total_wait_time = 0;
    mon->jobid = jobid;
    mon->jobdat = jobdat;

    /* register a callback for hnp sync response */
    if (ORTE_SUCCESS != (rc = orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                                                      ORTE_RML_TAG_YARN_SYNC_RESPONSE,
                                                      ORTE_RML_PERSISTENT,
                                                      yarn_daemon_sync_recv, mon))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    /* send a sync msg to hnp */
    msg = OBJ_NEW(opal_buffer_t);
    if (0 > (rc = orte_rml.send_buffer_nb(ORTE_PROC_MY_HNP, msg, ORTE_RML_TAG_YARN_SYNC_REQUEST, 0,
                                          send_cb, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(msg);
        return rc;
    }
    OPAL_OUTPUT_VERBOSE((5, orte_odls_globals.output,
                     "%s odls:yarn:orte_odls_yarn_launch_local_procs: finish send sync request to hnp, waitting for response ",
                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    return ORTE_SUCCESS;
}

static orte_vpid_t get_vpid_from_err_file(const char* filename) {
    orte_vpid_t id = 0;
    const char* ptr = filename;
    while (*ptr != '_') {
        id = id * 10 + ((*ptr) - 48);
        ptr++;
    }
    return id;
}

static orte_vpid_t get_vpid_from_normal_file(const char* filename) {
    orte_vpid_t id = atoi(filename);
    return id;
}