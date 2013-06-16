# -*- shell-script -*-
#
# Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
#                         University Research and Technology
#                         Corporation.  All rights reserved.
# Copyright (c) 2004-2005 The University of Tennessee and The University
#                         of Tennessee Research Foundation.  All rights
#                         reserved.
# Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
#                         University of Stuttgart.  All rights reserved.
# Copyright (c) 2004-2005 The Regents of the University of California.
#                         All rights reserved.
# Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

# MCA_ras_yarn_CONFIG([action-if-found], [action-if-not-found])
# -----------------------------------------------------------
AC_DEFUN([MCA_ras_yarn_CONFIG],[
    ORTE_CHECK_SLURM([ras_yarn], [ras_yarn_good=1], [ras_yarn_good=0])
         
    # if check worked, set wrapper flags if so.  
    # Evaluate succeed / fail
    AS_IF([test "$ras_yarn_good" = "1"],
          [ras_yarn_WRAPPER_EXTRA_LDFLAGS="$ras_yarn_LDFLAGS"
           ras_yarn_WRAPPER_EXTRA_LIBS="$ras_yarn_LIBS"
           $1],
          [$2])

    # set build flags to use in makefile
    AC_SUBST([ras_yarn_CPPFLAGS])
    AC_SUBST([ras_yarn_LDFLAGS])
    AC_SUBST([ras_yarn_LIBS])
])dnl
