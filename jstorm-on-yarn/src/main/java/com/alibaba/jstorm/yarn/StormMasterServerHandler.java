package com.alibaba.jstorm.yarn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.alibaba.jstorm.yarn.generated.*;
import com.google.common.base.Joiner;

public class StormMasterServerHandler implements StormMaster.Iface {

	private static final Logger LOG = LoggerFactory.getLogger(StormMasterServerHandler.class);
    @SuppressWarnings("rawtypes")
    Map _storm_conf;
    StormAMRMClient _client;
    MasterServer _masterServer;
    
	public StormMasterServerHandler(@SuppressWarnings("rawtypes") Map storm_conf, StormAMRMClient client) {
		 _storm_conf = storm_conf;
        setStormHostConf();
        Util.rmNulls(_storm_conf);
        _client = client;
	}

	@SuppressWarnings("unchecked")
	private void setStormHostConf() {
		try {
            String host_addr = InetAddress.getLocalHost().getHostAddress();
            LOG.info("Storm master host:"+host_addr);
            _storm_conf.put(Config.NIMBUS_HOST, host_addr);
        } catch (UnknownHostException ex) {
            LOG.warn("Failed to get IP address of local host");
        }
	}

	@Override
	public String getStormConf() throws TException {
		LOG.info("getting configuration...");
        return JSONValue.toJSONString(_storm_conf);
	}

	@Override
	public void setStormConf(String storm_conf) throws TException {
		LOG.info("setting configuration...");

        // stop processes
        stopSupervisors();
        stopUI();
        stopNimbus();

        Object json = JSONValue.parse(storm_conf);
        Map<?, ?> new_conf = (Map<?, ?>)json;
        _storm_conf.putAll(new_conf);
        Util.rmNulls(_storm_conf);
        setStormHostConf();
        
        // start processes
        startNimbus();
        startUI();
        startSupervisors();
	}

	@Override
	public void addSupervisors(int number) throws TException {
		LOG.info("adding "+number+" supervisors...");
        _client.addSupervisors(number);
	}


	class JStormProcess extends Thread {
        Process _process;
        String _name;

        public JStormProcess(String name){
            _name = name;
        }

        public void run(){
            startJStormProcess();
            try {
                _process.waitFor();
                LOG.info("Storm process "+_name+" stopped");
            } catch (InterruptedException e) {
                LOG.info("Interrupted => will stop the storm process too");
                _process.destroy();
            }
        }

        private void startJStormProcess() {
            try {
                LOG.info("Running: " + Joiner.on(" ").join(buildCommands()));
                LOG.info("current working dir:" + System.getProperty("user.dir"));
                ProcessBuilder builder =
                        new ProcessBuilder(buildCommands());
                
                _process = builder.start();
//                Util.redirectStreamAsync(_process.getInputStream(), System.out);
//                Util.redirectStreamAsync(_process.getErrorStream(), System.err);
                
            } catch (IOException e) {
                LOG.warn("Error starting nimbus process ", e);
            }
        }

        private List<String> buildCommands() throws IOException {
            if (_name == "nimbus") {
                return Util.buildNimbusCommands(_storm_conf);
            } else if (_name == "ui") {
                return Util.buildUICommands(_storm_conf);
            }

            throw new IllegalArgumentException(
                    "Cannot build command list for \"" + _name + "\"");
        }

        public void stopJStormProcess() {
            _process.destroy();
        }
    }
	
	JStormProcess nimbusProcess;
    JStormProcess uiProcess;

	@Override
	public void startNimbus() throws TException {
		LOG.info("starting nimbus...");
        synchronized(this) {
            if (nimbusProcess!=null && nimbusProcess.isAlive()){
                LOG.info("Received a request to start nimbus, but it is running now");
                return;
            }
            nimbusProcess = new JStormProcess("nimbus");
            nimbusProcess.start(); 
        }       
	}

	@Override
	public void stopNimbus() throws TException {
		synchronized(this) {
            if (nimbusProcess == null) return;
            LOG.info("stopping nimbus...");
            if (!nimbusProcess.isAlive()){
                LOG.info("Received a request to stop nimbus, but it is not running now");
                return;
            }
            nimbusProcess.stopJStormProcess();
            nimbusProcess = null;
        }
	}

	@Override
	public void startUI() throws TException {
		LOG.info("starting UI...");
        synchronized(this) {
            if (uiProcess!=null && uiProcess.isAlive()){
                LOG.info("Received a request to start UI, but it is running now");
                return;
            }
            uiProcess = new JStormProcess("ui");
            uiProcess.start();
        } 
	}

	@Override
	public void stopUI() throws TException {
		synchronized(this) {
            if (uiProcess == null) return;
            LOG.info("stopping UI...");
            if (!uiProcess.isAlive()){
                LOG.info("Received a request to stop UI, but it is not running now");
                return;
            }
            uiProcess.stopJStormProcess();
            uiProcess = null;
        }
	}

	@Override
	public void startSupervisors() throws TException {
		LOG.info("starting supervisors...");
        _client.startAllSupervisors();
	}

	@Override
	public void stopSupervisors() throws TException {
		LOG.info("stopping supervisors...");
        _client.stopAllSupervisors();
	}

	@Override
	public void shutdown() throws TException {
		LOG.info("shutdown storm master...");
        _masterServer.stop();
	}

	public void init(MasterServer masterServer) {
		_masterServer = masterServer;
	}

	public void stop() {
		try {
            stopSupervisors();
            stopUI();
            stopNimbus();
        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	}

}
