---
title: IConfigLoader
layout: documentation
documentation: true
---


### Introduction
IConfigLoader is an interface designed to allow way to dynamically load scheduler resource constraints into scheduler implementations. Currently, the MultiTenant scheduler uses this interface to dynamically load the number of isolated nodes a given user has been guaranteed, and the ResoureAwareScheduler uses the interface to dynamically load per user resource guarantees.

------

### Interface
```
public interface IConfigLoader {
    void prepare(Map<String, Object> conf);
    Map<?,?> load();
    public static IConfigLoader getConfigLoader(Map conf, String loaderClassConfig, String loaderConfConfig);
    public static Map<?,?> loadYamlConfigFromFile(File file);
};
```
#### Description
 - prepare is called upon class loading, and allows schedulers to pass in configuation items to the classes that implement IConfigLoader
 - load is called by the scheduler whenever it wishes to retrieve the most recent configuration map
 - getConfigLoader is a static helper class that will return the implementation of IConfigLoader that the scheduler requests
 - loadYamlConfigFromFile is a utility function used by implemenations of IConfigLoader that will load in a local yaml file and return a map

#### Loader Configuration
The loaders are dynamically selected and dynamically configured through configuration items in the scheduler implementations.

##### Example
```
resource.aware.scheduler.user.pools.loader: "org.apache.storm.scheduler.utils.ArtifactoryConfigLoader"
resource.aware.scheduler.user.pools.loader.params:
    artifactory.config.loader.uri: "http://artifactory.my.company.com:8000/artifactory/configurations/clusters/my_cluster/ras_pools"
    artifactory.config.loader.timeout.secs: "30"
multitenant.scheduler.user.pools.loader: "org.apache.storm.scheduler.utils.FileConfigLoader"
multitenant.scheduler.user.pools.loader.params:
    file.config.loader.local.file.yaml: "/path/to/my/config.yaml"
```
### Implementations

There are currently two implemenations of IConfigLoader
 - org.apache.storm.scheduler.utils.ArtifactoryConfigLoader: Load configurations from an Artifactory server
 - org.apache.storm.scheduler.utils.FileConfigLoader: Load configurations from a local file

Each of these have configurations that the scheduler can pass into the implemenation when the prepare method is called

#### FileConfigLoader
 - file.config.loader.local.file.yaml: A path to a local yaml file that represents the map the scheduler will use

#### ArtifactoryConfigLoader
 
 - artifactory.config.loader.uri: This can either be a reference to an individual file in Artifactory or to a directory.  If it is a directory, the file with the largest lexographic name will be returned.
 - artifactory.config.loader.timeout.secs: This is the amount of time an http connection to the artifactory server will wait before timing out.  The default is 10.
 - artifactory.config.loader.polltime.secs: This is the frequency at which the plugin will call out to artifactory instead of returning the most recently cached result. The default is 600 seconds.
 - artifactory.config.loader.scheme: This is the scheme to use when connecting to the Artifactory server. The default is http.
 - artifactory.config.loader.base.directory: This is the part of the uri, configurable in Artifactory, which represents the top of the directory tree. It defaults to "/artifactory".
