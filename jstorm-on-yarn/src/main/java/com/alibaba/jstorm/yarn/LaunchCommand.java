package com.alibaba.jstorm.yarn;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.yarn.Client.ClientCommand;
import com.alibaba.jstorm.yarn.generated.StormMaster;

public class LaunchCommand implements ClientCommand {

	private static final Logger LOG = LoggerFactory.getLogger(LaunchCommand.class);

	  @Override
	  public String getHeaderDescription() {
	    return "jstorm-yarn launch <master.yaml>";
	  }
	  
	  @Override
	  public Options getOpts() {
	    Options opts = new Options();
	    opts.addOption("appname", true, "Application Name. Default value - JStorm-on-Yarn");
	    opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
	    opts.addOption("jstormHome", true, "JStorm Home Directory");
	    opts.addOption("output", true, "Output file");
	    opts.addOption("jstormConfOutput", true, "storm.yaml file");
	    opts.addOption("jstormZip", true, "file path of jstorm.zip");
	    return opts;
	  }

	  @Override
	  public void process(CommandLine cl) throws Exception {
	    
	    String config_file = null;
	    List remaining_args = cl.getArgList();
	    if (remaining_args!=null && !remaining_args.isEmpty()) {
	        config_file = (String)remaining_args.get(0);
	    }
	    
	    Map stormConf = Config.readJStormConfig();
	    
	    LOG.info("initial storm conf:" + stormConf);
	    
	    String appName = cl.getOptionValue("appname", "JStorm-on-Yarn");
	    String queue = cl.getOptionValue("queue", "default");

	    String storm_zip_location = cl.getOptionValue("jstormZip");
	    Integer amSize = (Integer) stormConf.get(Config.MASTER_SIZE_MB);

	    System.out.println("amSize: " + amSize);
	    
	    JStormOnYarn jstorm = null;
	    try {
	      jstorm = JStormOnYarn.launchApplication(appName,
	                  queue, amSize,
	                  stormConf,
	                  storm_zip_location);
	      LOG.debug("Submitted application's ID:" + jstorm.getAppId());

	      //download storm.yaml file
	      String storm_yaml_output = cl.getOptionValue("jstormConfOutput");
	      if (storm_yaml_output != null && storm_yaml_output.length() > 0) {
	        //try to download storm.yaml
	        StormMaster.Client client = jstorm.getClient();
	        if (client != null)
	          StormMasterCommand.downloadStormYaml(client, storm_yaml_output);
	        else
	          LOG.warn("No storm.yaml is downloaded");
	      }

	      //store appID to output
	      String output = cl.getOptionValue("output");
	      if (output == null)
	          System.out.println(jstorm.getAppId());
	      else {
	          PrintStream os = new PrintStream(output);
	          os.println(jstorm.getAppId());
	          os.flush();
	          os.close();
	      }
	    } finally {
	      if (jstorm != null) {
	        jstorm.stop();
	      }
	    }
	  }

}
