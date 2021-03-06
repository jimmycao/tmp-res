package com.pivotal.hamster.appmaster.webapps;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.JQueryUI;

import com.pivotal.hamster.appmaster.utils.HamsterAppMasterUtils;
import com.pivotal.hamster.common.LaunchContext;

public class IndexBlock extends HtmlBlock {
  private static final Log LOG = LogFactory.getLog(IndexBlock.class);

  @Override
  protected void render(Block html) {
    LaunchContext[] launchedContexts = HamsterWebAppContext.getSortedLaunchContexts();
    int mpiJobStartIdx = -1;
    for (int i = 0; i< launchedContexts.length; i++) {
      if (launchedContexts[i].getName().getJobId() > 0) {
        mpiJobStartIdx = i;
        break;
      }
    }
    
    String userName = null;
    try {
      userName = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.warn("error while geting userName", e);
      userName = "null-user-name";
    }
    
    THEAD<TABLE<Hamlet>> table = html.
        h2("Launched MPI procs").
        table("#jobs").$style("text-align:left").
        thead().
        tr().
        th("Rank").
        th("Host").
        th("Container ID").
        th("Log")._();
    
    if (mpiJobStartIdx >= 0) {
      for (int i = mpiJobStartIdx; i < launchedContexts.length; i++) {
        LaunchContext ctx = launchedContexts[i];
        table = table.tr().
          td(String.format("%-4d (MPI)", ctx.getName().getVpId()).replace(' ', '_')).
          td(ctx.getHost()).
          td(ctx.getContainer().getId().toString()).
          td().a(String.format("http://%s/node/containerlogs/%s/" + userName,
              ctx.getContainer().getNodeHttpAddress(),
              ctx.getContainer().getId().toString()),
              "Link")._()._();
      }
    }
    
    // render it
    table._().tbody()._()._();
    
    // make mpi daemon table
    table = html.
        h2("Launched Daemon procs").
        table("#jobs").$style("text-align:left").
        thead().
        tr().
        th("Rank").
        th("Host").
        th("Container ID").
        th("Log")._();
    
    int daemonEndIdx = mpiJobStartIdx;
    if (daemonEndIdx < 0) {
      daemonEndIdx = launchedContexts.length;
    }
    
    // add hnp
    String localNMAddr = HamsterAppMasterUtils.getLocalNMHttpAddr();
    ContainerId localContainerId = HamsterAppMasterUtils.getContainerIdFromEnv();
    
    if (localNMAddr != null && localContainerId != null) {
      table = table.tr().
          td(String.format("%-4d (HNP)", 0).replace(' ', '_')).
          td(System.getenv("NM_HOST")).
          td(localContainerId.toString()).
          td().a(String.format("http://%s/node/containerlogs/%s/" + userName,
              localNMAddr,
              localContainerId.toString()),
              "Link")._()._();
    }
    
    if (mpiJobStartIdx >= 0) {
      for (int i = 0; i < daemonEndIdx; i++) {
        LaunchContext ctx = launchedContexts[i];
        table = table.tr().
            td(String.format("%-4d (DMN)", ctx.getName().getVpId()).replace(' ', '_')).
            td(ctx.getHost()).
            td(ctx.getContainer().getId().toString()).
            td().a(String.format("http://%s/node/containerlogs/%s/" + userName,
                ctx.getContainer().getNodeHttpAddress(),
                ctx.getContainer().getId().toString()),
                "Link")._()._();
      }
    }
    
    // make mpi daemon table
    table._().tbody()._()._();
  }
}