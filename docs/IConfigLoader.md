---
title: IConfigLoader
layout: documentation
documentation: true
---


### Introduction
IConfigLoader is an interface designed to allow dynamic loading of scheduler resource constraints. Currently, the MultiTenant scheduler uses this interface to dynamically load the number of isolated nodes a given user has been guaranteed, and the ResoureAwareScheduler uses the interface to dynamically load per user resource guarantees.

The following interface is provided for users to create an IConfigLoader instance based on the scheme of the `scheduler.config.loader.uri`.
```
ConfigLoaderFactoryService.createConfigLoader(Map<String, Object> conf)
``` 

------

### Interface
```
public interface IConfigLoader {
    Map<?,?> load();
};
```
#### Description
  - load is called by the scheduler whenever it wishes to retrieve the most recent configuration map.
 
#### Loader Configuration
The loaders are dynamically selected and dynamically configured through configuration items in the scheduler implementations.

##### Example
```
scheduler.config.loader.uri: "artifactory+http://artifactory.my.company.com:8000/artifactory/configurations/clusters/my_cluster/ras_pools"
scheduler.config.loader.timeout.sec: 30
```
Or
```
scheduler.config.loader.uri: "file:///path/to/my/config.yaml"
```
### Implementations

There are currently two implemenations of IConfigLoader
 - org.apache.storm.scheduler.utils.ArtifactoryConfigLoader: Load configurations from an Artifactory server. 
 It will be used if users add `artifactory+` to the scheme of the real URI and set to `scheduler.config.loader.uri`.
 - org.apache.storm.scheduler.utils.FileConfigLoader: Load configurations from a local file. It will be used if users use `file` scheme.

#### Configurations
 - scheduler.config.loader.uri: For `ArtifactoryConfigLoader`, this can either be a reference to an individual file in Artifactory or to a directory.  If it is a directory, the file with the largest lexographic name will be returned.
 For `FileConfigLoader`, this is the URI pointing to a file.
 - scheduler.config.loader.timeout.secs: Currently only used in `ArtifactoryConfigLoader`. It is the amount of time an http connection to the artifactory server will wait before timing out. The default is 10.
 - scheduler.config.loader.polltime.secs: Currently only used in `ArtifactoryConfigLoader`. It is the frequency at which the plugin will call out to artifactory instead of returning the most recently cached result. The default is 600 seconds.
 - scheduler.config.loader.artifactory.base.directory: Only used in `ArtifactoryConfigLoader`. It is the part of the uri, configurable in Artifactory, which represents the top of the directory tree. It defaults to "/artifactory".