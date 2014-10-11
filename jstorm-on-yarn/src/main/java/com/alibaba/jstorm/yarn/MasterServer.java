package com.alibaba.jstorm.yarn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//import backtype.storm.security.auth.ThriftServer;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.yarn.generated.StormMaster;
import com.alibaba.jstorm.yarn.generated.StormMaster.Processor;
import com.alibaba.jstorm.yarn.thrift.ThriftServer;

//import org.apache.thrift7.TProcessor;

public class MasterServer extends ThriftServer {

	private static final Logger LOG = LoggerFactory.getLogger(MasterServer.class);
    private static StormMasterServerHandler _handler;

    private Thread initAndStartHeartbeat(final StormAMRMClient client,
            final BlockingQueue<Container> launcherQueue,
            final int heartBeatIntervalMs) {
        Thread thread = new Thread() {
        @Override
        public void run() {
          try {
            while (client.getServiceState() == Service.STATE.STARTED &&
                !Thread.currentThread().isInterrupted()) {
              
              Thread.sleep(heartBeatIntervalMs);

              // We always send 50% progress.
              AllocateResponse allocResponse = client.allocate(0.5f);

              AMCommand am_command = allocResponse.getAMCommand();
              if (am_command!=null &&
                      (am_command == AMCommand.AM_SHUTDOWN || am_command==AMCommand.AM_RESYNC)) {
                LOG.info("Got AM_SHUTDOWN or AM_RESYNC from the RM");
                _handler.stop();
                System.exit(0);
              }

              List<Container> allocatedContainers = allocResponse.getAllocatedContainers();
              if (allocatedContainers.size() > 0) {
                // Add newly allocated containers to the client.
                LOG.info("HB: Received allocated containers (" + allocatedContainers.size() + ")");
                client.addAllocatedContainers(allocatedContainers);
                if (client.supervisorsAreToRun()) {
                  LOG.info("HB: Supervisors are to run, so queueing (" + allocatedContainers.size() + ") containers...");
                  launcherQueue.addAll(allocatedContainers);
                } else {
                  LOG.info("HB: Supervisors are to stop, so releasing all containers...");
                  client.stopAllSupervisors();
                }
              }

              List<ContainerStatus> completedContainers =
                  allocResponse.getCompletedContainersStatuses();
              
              if (completedContainers.size() > 0 && client.supervisorsAreToRun()) {
                LOG.debug("HB: Containers completed (" + completedContainers.size() + "), so releasing them.");
                client.startAllSupervisors();
              }
            
            }
          } catch (Throwable t) {
            // Something happened we could not handle.  Make sure the AM goes
            // down so that we are not surprised later on that our heart
            // stopped..
            LOG.error("Unhandled error in AM: ", t);
            _handler.stop();
            System.exit(1);
          }
        }
      };
      thread.start();
      return thread;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        LOG.info("Starting the AM!!!!");

        Options opts = new Options();
        opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used " +
                "unless for testing purposes");

        CommandLine cl = new GnuParser().parse(opts, args);

        ApplicationAttemptId appAttemptID;
        Map<String, String> envs = System.getenv();
        if (cl.hasOption("app_attempt_id")) {
          String appIdStr = cl.getOptionValue("app_attempt_id", "");
          appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
        } else if (envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
          ContainerId containerId = ConverterUtils.toContainerId(envs
                  .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
          appAttemptID = containerId.getApplicationAttemptId();
          LOG.info("appAttemptID from env:" + appAttemptID.toString());
        } else {
          LOG.error("appAttemptID is not specified for storm master");
          throw new Exception("appAttemptID is not specified for storm master");
        }

        @SuppressWarnings("rawtypes")
        Map storm_conf = Config.readJStormConfig(null);
        Util.rmNulls(storm_conf);

        YarnConfiguration hadoopConf = new YarnConfiguration();

        final String host = InetAddress.getLocalHost().getHostName();
        storm_conf.put("nimbus.host", host);

        StormAMRMClient rmClient =
                new StormAMRMClient(appAttemptID, storm_conf, hadoopConf);
        rmClient.init(hadoopConf);
        rmClient.start();

        BlockingQueue<Container> launcherQueue = new LinkedBlockingQueue<Container>();

        MasterServer server = new MasterServer(storm_conf, rmClient);
        try {
            final int port = Utils.getInt(storm_conf.get(Config.MASTER_THRIFT_PORT));
            final String target = host + ":" + port;
            InetSocketAddress addr = NetUtils.createSocketAddr(target);
            RegisterApplicationMasterResponse resp =
                    rmClient.registerApplicationMaster(addr.getHostName(), port, null);
            LOG.info("Got a registration response "+resp);
            LOG.info("Max Capability "+resp.getMaximumResourceCapability());
            rmClient.setMaxResource(resp.getMaximumResourceCapability());
            LOG.info("Starting HB thread");
            server.initAndStartHeartbeat(rmClient, launcherQueue,
                    (Integer) storm_conf
                    .get(Config.MASTER_HEARTBEAT_INTERVAL_MILLIS));
            LOG.info("Starting launcher");
            initAndStartLauncher(rmClient, launcherQueue);
            rmClient.startAllSupervisors();
            LOG.info("Starting Master Thrift Server");
            server.serve();
            LOG.info("StormAMRMClient::unregisterApplicationMaster");
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                    "AllDone", null);
        } finally {
            if (server.isServing()) {
                LOG.info("Stop Master Thrift Server");
                server.stop();
            }
            LOG.info("Stop RM client");
            rmClient.stop();
        }
        System.exit(0);
    }

    private static void initAndStartLauncher(final StormAMRMClient client,
            final BlockingQueue<Container> launcherQueue) {
        Thread thread = new Thread() {
            Container container;
            @Override
            public void run() {
                while (client.getServiceState() == Service.STATE.STARTED &&
                        !Thread.currentThread().isInterrupted()) {
                    try {
                        container = launcherQueue.take();
                        LOG.info("LAUNCHER: Taking container with id ("+container.getId()+") from the queue.");
                        if (client.supervisorsAreToRun()) {
                            LOG.info("LAUNCHER: Supervisors are to run, so launching container id ("+container.getId()+")");
                            client.launchSupervisorOnContainer(container);
                        } else {
                            // Do nothing
                            LOG.info("LAUNCHER: Supervisors are not to run, so not launching container id ("+container.getId()+")");
                        }
                    } catch (InterruptedException e) {
                        if (client.getServiceState() == Service.STATE.STARTED) {
                            LOG.error("Launcher thread interrupted : ", e);
                            System.exit(1);
                        }
                        return;
                    } catch (IOException e) {
                        LOG.error("Launcher thread I/O exception : ", e);
                        System.exit(1);
                    }
                }
            }
        };
        thread.start();
    }

    public MasterServer(@SuppressWarnings("rawtypes") Map storm_conf, 
            StormAMRMClient client) {
        this(storm_conf, new StormMasterServerHandler(storm_conf, client));
    }

    private MasterServer(@SuppressWarnings("rawtypes") Map storm_conf,
            StormMasterServerHandler handler) {
        super(storm_conf, 
                new Processor<StormMaster.Iface>(handler), 
                Utils.getInt(storm_conf.get(Config.MASTER_THRIFT_PORT)));
        try {
            _handler = handler;
            _handler.init(this);
            
            LOG.info("launch nimbus");
            _handler.startNimbus();

            LOG.info("launch ui");
            _handler.startUI();

            int numSupervisors =
                    Utils.getInt(storm_conf.get(Config.MASTER_NUM_SUPERVISORS));
            LOG.info("launch " + numSupervisors + " supervisors");
            _handler.addSupervisors(numSupervisors);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        super.stop();
        if (_handler != null) {
            _handler.stop();
            _handler = null;
        }
    }
	
    public String getStormConf() throws TException {
    	return _handler.getStormConf();
    }

}
