package com.alibaba.jstorm.yarn;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import backtype.storm.utils.Utils;

import org.yaml.snakeyaml.Yaml;


public class Config {

	final public static String MASTER_DEFAULTS_CONFIG = "master_defaults.yaml";
    final public static String MASTER_CONFIG = "master.yaml";
    final public static String MASTER_HOST = "master.host";
    final public static String MASTER_THRIFT_PORT = "master.thrift.port";
    final public static String MASTER_TIMEOUT_SECS = "master.timeout.secs";
    final public static String MASTER_SIZE_MB = "master.container.size-mb";
    final public static String MASTER_NUM_SUPERVISORS = "master.initial-num-supervisors";
    final public static String MASTER_CONTAINER_PRIORITY = "master.container.priority";
    //# of milliseconds to wait for YARN report on Storm Master host/port
    final public static String YARN_REPORT_WAIT_MILLIS = "yarn.report.wait.millis";
    final public static String MASTER_HEARTBEAT_INTERVAL_MILLIS = "master.heartbeat.interval.millis";
    
    @SuppressWarnings("rawtypes")
    static public Map readJStormConfig() {
        return readJStormConfig(null);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static Map readJStormConfig(String jstormYarnConfigPath) {
        //default configurations
        Map ret = Utils.readDefaultConfig();
        Map conf = Utils.findAndReadConfigFile(Config.MASTER_DEFAULTS_CONFIG);
        ret.putAll(conf);
        System.out.println("default ret:" + ret);
        
        //standard storm configuration
        String confFile = System.getProperty("jstorm.conf.file");
//        String confFile = Util.getJStormHome() + "/conf/storm.yaml";
        Map storm_conf;
        if (confFile==null || confFile.equals("")) {
            storm_conf = Utils.findAndReadConfigFile("storm.yaml", false);
        } else {
            storm_conf = Utils.findAndReadConfigFile(confFile, true);
        }
        System.out.println("storm_conf:" + storm_conf);
        ret.putAll(storm_conf);
        
        //configuration file per command parameter 
        if (jstormYarnConfigPath == null) {
            Map master_conf = Utils.findAndReadConfigFile(Config.MASTER_CONFIG, false);
            ret.putAll(master_conf);
        }
        else {
            try {
                Yaml yaml = new Yaml();
                FileInputStream is = new FileInputStream(jstormYarnConfigPath);
                Map storm_yarn_config = (Map) yaml.load(is);
                if(storm_yarn_config!=null)
                    ret.putAll(storm_yarn_config);
                is.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        //other configuration settings via CLS opts per system property: storm.options
        ret.putAll(Utils.readCommandLineOpts());

        return ret;
    }
}
