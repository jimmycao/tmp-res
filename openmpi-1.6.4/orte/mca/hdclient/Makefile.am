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
# Copyright (c) 2010 Cisco Systems, Inc.  All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

# main library setup
noinst_LTLIBRARIES = libmca_hdclient.la
libmca_hdclient_la_SOURCES =

# pkgdata setup
dist_pkgdata_DATA = hamster_protos.pb

# local files
headers = hdclient.h

# Conditionally install the header files
if WANT_INSTALL_HEADERS
ortedir = $(includedir)/openmpi/$(subdir)
nobase_orte_HEADERS = $(headers)
endif

include src/Makefile.am

distclean-local: 
	rm -f src/static-components.h

install-data-local:
	$(MKDIR_P) $(DESTDIR)$(sysconfdir)/protos/
	$(INSTALL_DATA) hamster_protos.pb $(DESTDIR)$(sysconfdir)/protos/

uninstall-local:
	rm -rf $(DESTDIR)$(sysconfdir)/protos/hamster_protos.pb
