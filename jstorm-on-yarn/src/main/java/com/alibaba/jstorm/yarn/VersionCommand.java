package com.alibaba.jstorm.yarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.yarn.Client.ClientCommand;

public class VersionCommand implements ClientCommand {

	private static final Logger LOG = LoggerFactory
		      .getLogger(VersionCommand.class);
	
	VersionCommand() {
		
	}
	@Override
	public Options getOpts() {
		Options opts = new Options();
	    return opts;
	}

	@Override
	public String getHeaderDescription() {
		return "jstorm-yarn version";
	}

	@Override
	public void process(CommandLine cl) throws Exception {
		Version version = Util.getJStormVersion();
	    System.out.println(version.toString());
	}

}
