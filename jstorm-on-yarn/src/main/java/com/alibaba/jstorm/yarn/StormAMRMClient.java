package com.alibaba.jstorm.yarn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

public class StormAMRMClient extends AMRMClientImpl<ContainerRequest> {
	
	private static final Logger LOG = LoggerFactory.getLogger(StormAMRMClient.class);

	@SuppressWarnings("rawtypes")
	private final Map storm_conf;
	private final YarnConfiguration hadoopConf;
	private final Priority DEFAULT_PRIORITY = Records.newRecord(Priority.class);
	private final Set<Container> containers;
	private volatile boolean supervisorsAreToRun = false;
	private AtomicInteger numSupervisors;
	private Resource maxResourceCapability;
	private Resource defaultResource;
	private ApplicationAttemptId appAttemptId;
	private NMClientImpl nmClient;

	public StormAMRMClient(ApplicationAttemptId appAttemptID, @SuppressWarnings("rawtypes") Map storm_conf,
			YarnConfiguration hadoopConf) {
		this.appAttemptId = appAttemptID;
	    this.storm_conf = storm_conf;
	    this.hadoopConf = hadoopConf;
	    Integer pri = Utils.getInt(storm_conf.get(Config.MASTER_CONTAINER_PRIORITY));
	    this.DEFAULT_PRIORITY.setPriority(pri);
	    this.containers = new TreeSet<Container>();
	    numSupervisors = new AtomicInteger(0);

	    // start am nm client
	    nmClient = (NMClientImpl) NMClient.createNMClient();
	    nmClient.init(hadoopConf);
	    nmClient.start();
	}

	public void setMaxResource(Resource maximumResourceCapability) {
		this.maxResourceCapability = maximumResourceCapability;
		this.defaultResource = maximumResourceCapability;
		this.defaultResource.setMemory(1024);
		this.defaultResource.setVirtualCores(1);
	    LOG.info("Max Capability is now "+this.maxResourceCapability);
	}

	public synchronized void startAllSupervisors() {
	    LOG.debug("Starting all supervisors, requesting containers...");
	    this.supervisorsAreToRun = true;
	    this.addSupervisorsRequest();
	}

	private void addSupervisorsRequest() {
		int num = numSupervisors.getAndSet(0);
	    for (int i=0; i<num; i++) {
//	      ContainerRequest req = new ContainerRequest(this.maxResourceCapability,
	      ContainerRequest req = new ContainerRequest(this.defaultResource,
	              null, // String[] nodes,
	              null, // String[] racks,
	              DEFAULT_PRIORITY);
	      super.addContainerRequest(req);
	    }
	}

	public synchronized boolean addAllocatedContainers(List<Container> containers) {
		for (int i=0; i<containers.size(); i++) {
//	      ContainerRequest req = new ContainerRequest(this.maxResourceCapability,
	      ContainerRequest req = new ContainerRequest(this.defaultResource,
	              null, // String[] nodes,
	              null, // String[] racks,
	              DEFAULT_PRIORITY);
	      super.removeContainerRequest(req);
	    }
	    return this.containers.addAll(containers);

	}

	public boolean supervisorsAreToRun() {
		return this.supervisorsAreToRun;
	}

	public void stopAllSupervisors() {
		LOG.debug("Stopping all supervisors, releasing all containers...");
	    this.supervisorsAreToRun = false;
	    releaseAllSupervisorsRequest();
	}

	private void releaseAllSupervisorsRequest() {
		Iterator<Container> it = this.containers.iterator();
	    ContainerId id;
	    while (it.hasNext()) {
	      id = it.next().getId();
	      LOG.debug("Releasing container (id:"+id+")");
	      releaseAssignedContainer(id);
	      it.remove();
	    }
	}

	public void launchSupervisorOnContainer(Container container) 
			throws IOException{
		// create a container launch context
	    ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);
	    UserGroupInformation user = UserGroupInformation.getCurrentUser();
	    try {
	      Credentials credentials = user.getCredentials();
	      DataOutputBuffer dob = new DataOutputBuffer();
	      credentials.writeTokenStorageToStream(dob);
	      ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
	      launchContext.setTokens(securityTokens);
	    } catch (IOException e) {
	      LOG.warn("Getting current user info failed when trying to launch the container"
	              + e.getMessage());
	    }

	    // CLC: env
	    Map<String, String> env = new HashMap<String, String>();
	    env.put("JSTORM_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
	    launchContext.setEnvironment(env);
	    LOG.info("container launch env:" + env);

	    // CLC: local resources includes storm, conf
	    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
	    String jstorm_zip_path = (String) storm_conf.get("jstorm.zip.path");
	    Path zip = new Path(jstorm_zip_path);
	    FileSystem fs = FileSystem.get(hadoopConf);
	    String vis = (String) storm_conf.get("jstorm.zip.visibility");
	    if (vis.equals("PUBLIC"))
	      localResources.put("jstorm", Util.newYarnAppResource(fs, zip,
	              LocalResourceType.ARCHIVE, LocalResourceVisibility.PUBLIC));
	    else if (vis.equals("PRIVATE"))
	      localResources.put("jstorm", Util.newYarnAppResource(fs, zip,
	              LocalResourceType.ARCHIVE, LocalResourceVisibility.PRIVATE));
	    else if (vis.equals("APPLICATION"))
	      localResources.put("jstorm", Util.newYarnAppResource(fs, zip,
	              LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION));

	    LOG.info("local resources:" + localResources);
	    LOG.info("appAttemptId:" + appAttemptId);
	    String appHome = Util.getApplicationHomeForId(appAttemptId.toString());
	    
	    String containerHome = appHome + Path.SEPARATOR + container.getId().getId();
	    
	    Path confDst = Util.createConfigurationFileInFs(fs, containerHome,
	                 this.storm_conf, this.hadoopConf);
	    
	    localResources.put("conf", Util.newYarnAppResource(fs, confDst));

	    launchContext.setLocalResources(localResources);

	    // CLC: command
	    List<String> supervisorArgs = Util.buildSupervisorCommands(this.storm_conf);
	    LOG.info("start supervisor command: " + supervisorArgs);
	    launchContext.setCommands(supervisorArgs);

	    try {
	      LOG.info("Use NMClient to launch supervisors in container. ");
	      nmClient.startContainer(container, launchContext);

	      String userShortName = user.getShortUserName();
	      if (userShortName != null)
	        LOG.info("Supervisor log: http://" + container.getNodeHttpAddress() + "/node/containerlogs/"
	                + container.getId().toString() + "/" + userShortName + "/supervisor.log");
	    } catch (Exception e) {
	      LOG.error("Caught an exception while trying to start a container", e);
	      System.exit(-1);
	    }

	}

	public void addSupervisors(int number) {
		int num = numSupervisors.addAndGet(number);
	    if (this.supervisorsAreToRun) {
	      LOG.info("Added " + num + " supervisors, and requesting containers...");
	      addSupervisorsRequest();
	    } else {
	      LOG.info("Added " + num + " supervisors, but not requesting containers now.");
	    }

	}

}
