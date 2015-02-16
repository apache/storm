[JStorm English introduction](http://42.121.19.155/jstorm/JStorm-introduce-en.pptx)
[JStorm Chinese introduction](http://42.121.19.155/jstorm/JStorm-introduce.pptx)

#Release 0.9.6.3
## New features
1. 实现tick tuple
2. 支持logbak
3. 支持加载用户自定义Log4j 配置文件
4. Web UI显示用户自定义metrics
5. jstorm list 命令支持topologyName
6. 所有底层使用ip，自定义调度的时候，支持自定义调度中ip和hostname混用
7. 本地模式支持junit test
8. 客户端命令（比如提交jar时）可以指定storm.yaml 配置文件
## Bug fix
1. 在spout 的prepare里面增加active动作
2. 多语言支持
3. 异步检查worker启动心跳，加快worker启动速度
4. 进程pid检查，加快发现worker已经死去的速度
5. 使用错误的disruptor 类， 当disruptor队列满时，producer狂占CPU
6. kill worker时， disruptor 报错，引起误解
7. restart 命令可能失败
8. JStorm 升级后，客户端提交一不兼容版本的应用jar时， 需要正确报错
9. 本地模式时， 如果用户配置了log4j或logback时， 会打印日志2次
10. 本地模式时， 应用调用了killTopology时， 可能出现exception
11. 避免应用hack logger, 需要额外设置jstorm 的log level为info
12. 增加一个logback 配置文件模板
13. 上传lib jar时， 会有可能把lib jar当作应用的jar来处理了
14. 删除一个topology后， 发现zk还是有一些node没有清理干净
15. java core dump时，必须带上topology的名字
16. JDK8 里-XX:MaxTenuringThreshold 的最大值是15，默认配置里的是20
17. 一些特殊情况下，无法获取cpu 核数，导致supervisor slot数为0
18. Fix 本地模式时，zk 报"Address family not supported by protocol family"
19. Fix 本地模式时，关闭logview http server
20. 在检查supervisor是否存活脚本中，创建日志目录
21. 启动脚本对nimbus ip 的完整word检查，避免误启动其他机器nimbus
22. 启动脚本对环境变量$JAVA_HOME/$JSTORM_HOME/$JSTORM_CONF_DIR检查, 并自动加载bash配置文件
23. rpm安装包中，修改日志目录为/home/admin/logs
24. rpm安装后，需要让/home/admin/jstorm, /home/admin/logs 可以被任意用户读取
25. rpm安装包中，设置本地临时端口区间
26. 需要一个noarch的rpm包

#Release 0.9.6.2
1. Add option to switch between BlockingQueue and Disruptor
2. Fix the bug which under sync netty mode, client failed to send message to server 
3. Fix the bug let web UI can dispaly 0.9.6.1 cluster
4. Fix the bug topology can be submited without main jar but a lot of little jar
5. Fix the bug restart command 
6. Fix the bug trident bug
7. Add the validation of topology name, component name... Only A-Z, a-z, 0-9, '_', '-', '.' are valid now.
8. Fix the bug close thrift client

#Release 0.9.6.2-rc
1. Improve user experience from Web UI
1.1 Add jstack link
1.2 Add worker log link in supervisor page
1.3 Add Web UI log encode setting "gbk" or "utf-8"
1.4 Show starting tasks in component page
1.5 Show dead task's information in UI
1.6 Fix the bug that error info can not be displayed in UI when task is restarting
2. Add restart command, with this command, user can reload configuration, reset worker/task parallism
3. Upgrade curator/disruptor/guava version
4. Revert json lib to google-simple json, wrap all json operation into two utility method
5. Add new storm submit api, supporting submit topology under java 
6. Enable launch process with backend method
7. Set "spout.pending.full.sleep" default value as true
8. Fix the bug user define sceduler not support a list of workers
9. Add disruptor/JStormUtils junit test
10. Enable user to configure the name of monitor name of alimonitor
11. Add tcp option "reuseAddress" in netty framework
12. Fix the bug: When spout does not implement the ICommitterTrident interface, MasterCoordinatorSpout will stick on commit phase.

#Release 0.9.6.2-rc
1. Improve user experience from Web UI
1.1 Add jstack link
1.2 Add worker log link in supervisor page
1.3 Add Web UI log encode setting "gbk" or "utf-8"
1.4 Show starting tasks in component page
1.5 Show dead task's information in UI
1.6 Fix the bug that error info can not be displayed in UI when task is restarting
2. Add restart command, with this command, user can reload configuration, reset worker/task parallism
3. Upgrade curator/disruptor/guava version
4. Revert json lib to google-simple json, wrap all json operation into two utility method
5. Add new storm submit api, supporting submit topology under java 
6. Enable launch process with backend method
7. Set "spout.pending.full.sleep" default value as true
8. Fix the bug user define sceduler not support a list of workers
9. Add disruptor/JStormUtils junit test
10. Enable user to configure the name of monitor name of alimonitor
11. Add tcp option "reuseAddress" in netty framework
12. Fix the bug: When spout does not implement the ICommitterTrident interface, MasterCoordinatorSpout will stick on commit phase.

#Release 0.9.6.1
1. Add management of multiclusters to Web UI. Added management tools for multiclusters in WebUI.
2. Merged Trident API from storm-0.9.3
3. Replaced gson with fastjson
4. Refactored metric json generation code.
5. Stored version info with $JSTORM_HOME/RELEASE.
6. Replaced SingleThreadDisruptorQueue with MultiThreadDisruptorQueue in task deserialize thread.
7. Fixed issues with worker count on Web UI.
8. Fixed issues with accessing the task map with multi-threads.
9. Fixed NullPointerException while killing worker and reading worker's hearbeat object.
10. Netty client connect to server only in NettyClient module.
11. Add break loop operation when netty client connection is closed
12. Fix the bug that topology warning flag present in cluster page is not consistent with error information present in topology page
13. Add recovery function when the data of task error information is corrupted
14. Fix the bug that the metric data can not be uploaded onto Alimonitor when ugrading from pre-0.9.6 to 0.9.6 and executing pkill java without restart the topologying
15. Fix the bug that zeroMq failed to receive data
16. Add interface to easily setting worker's memory
17. Set default value of topology.alimonitor.metrics.post to false
18. Only start NETTY_SERVER_DECODE_TIME for netty server
19. Keep compatible with Storm for local mode
20. Print rootId when tuple failed
21. In order to keep compatible with Storm, add submitTopologyWithProgressBar interface
22. Upgrade netty version from 3.2.7 to 3.9.0
23. Support assign topology to user-defined supervisors


#Release 0.9.6
1. Update UI 
  - Display the metrics information of task and worker
  - Add warning flag when errors occur for a topology
  - Add link from supervisor page to task page
2. Send metrics data to Alimonitor
3. Add metrics interface for user
4. Add task.cleanup.timeout.sec setting to let task gently cleanup
5. Set the worker's log name as topologyName-worker-port.log
6. Add setting "worker.redirect.output.file", so worker can redirect System.out/System.err to one setting file
7. Add storm list command
8. Add closing channel check in netty client to avoid double close
9. Add connecting check in netty client to avoid connecting one server twice at one time 

#Release 0.9.5.1
1. Add netty sync mode
2. Add block operation in netty async mode
3. Replace exception with Throwable in executor layer
4. Upgrade curator-framework version from 1.15 to 1.3.2
5. Add more netty junit test
6. Add log when queue is full

#Release 0.9.5
##Big feature:
1. Redesign scheduler arithmetic, basing worker not task .

## Bug fix
1. Fix disruptor use too much cpu
2. Add target NettyServer log when f1ail to send data by netty

#Release 0.9.4.1
##Bug fix:
1. Improve speed between tasks who is running in one worker
2. Fix wrong timeout seconds
3. Add checking port when worker initialize and begin to kill old worker
4. Move worker hearbeat thread before initializing tasks
5. Move init netty-server before initializeing tasks 
6. Check whether tuple's rootId is duplicated
7. Add default value into Utils.getInt
8. Add result function in ReconnectRunnable
9. Add operation to start Timetick
10. Halt process when master nimbus lost ZK node
11. Add exception catch when cgroups kill process
12. Speed up  reconnect to netty-server
13. Share one task hearbeat thread for all tasks
14. Quickly haltprocess when initialization failed.
15. Check web-ui logview page size 



#Release 0.9.4

## Big features
1. Add transaction programming mode
2. Rewrite netty code, 1. use share boss/worker thread pool;2 async send batch tuples;3 single thread to do reconnect job;4 receive batch tuples
3. Add metrics and statics
4. Merge Alimama storm branch into this version, submit jar with -conf, -D, -lib


## Enhancement
1. add setting when supervisor has been shutdown, worker will shutdown automatically
2. add LocalFristGrouping api
3. enable cgroup for normal user



##Bug fix:
1. Setting buffer size  when upload jar
2. Add lock between ZK watch and timer thread when refresh connection
3. Enable nimbus monitor thread only when topology is running in cluster mode
4. Fix exception when failed to read old assignment of ZK
5. classloader fix when both parent and current classloader load the same class
6. Fix log view null pointer exception

#Release 0.9.3.1

## Enhancement
1. switch apache thrift7 to storm thrift7
2. set defatult acker number is 1
3. add "spout.single.thread" setting
4. make nimbus logview port different from supervisor's
5. web ui can list all files of log's subdir
6. Set gc dump dir as log's dir


#Release 0.9.3
## New feature
1. Support Aliyun Apsara/Hadoop Yarn

## Enhancement
1. Redesign Logview
2. Kill old worker under the same port when worker is starting
3. Add zk information/version information on UI
4. Add nodeport information for dead task in nimbus
5. Add interface to get values when spout doing ack
6. Add timeout statics in bolt
7. jstorm script return status
8. Add logs when fail to deserialize tuple 
9. Skip sleep operation when max_pending is 1 and waiting ack
10. Remove useless dependency
11. Longer task timeout setting
12. Add supervisor.use.ip setting
13. Redirect supervisor out/err to /dev/null, redirect worker out/err to one file


## Bug Fix
1. Fix kryo fail to deserialize object when enable classloader
2. Fix fail to reassign dead task when worker number is less than topology apply
3. Set samller jvm heap memory for jstorm-client 
4. Fix fail to set topology status as active when  do rebalance operation twice at one time,
5. Fix local mode bug under linux
6. Fix average latency isn't accurate
7. GC tuning.
8. Add default kill function for AysncLoopRunnable
 


#Release 0.9.2
## New feature
1. Support LocalCluster/LocalDrpc mode, support debugging topology under local mode
2. Support CGroups, assigning CPU in hardware level.
3. Support simple logview

## Bug fix or enhancement
1. Change SpoutExecutor's RotatingMap to TimeCacheMap, when putting too much timeout tuple is easy to cause deadlock in spout acker thread
2. Tunning gc parameter, improve performance and avoid full GC
3. Improve Topology's own gc priority, make it higher than JStorm system setting.
4. Tuning Nimbus HA, switch nimbus faster, when occur nimbus failure.
5. Fix bugs found by FindBugs tool.
6. Revert Trident interface to 0.8.1, due to 0.8.1's trident interface's performance is better.
7. Setting nimbus.task.timeout.secs as 60 to avoid nimbus doing assignment when task is under full gc.
8. Setting default rpc framework as netty
9. Tunning nimbus shutdown flow
10. Tunning worker shutdown flow
11. Add task heartbeat log
12. Optimize Drpc/LocalDrpc source code.
13. Move classloader to client jar.
14  Fix classloader fail to load  anonymous class
15. Web Ui display slave nimbus
16. Add thrift max read buffer size
17. Setting CPU slot base double
18. Move Zk utility to jstorm-client-extension.jar
19. Fix localOrShuffle null pointer
20. Redirecting worker's System.out/System.err to file is configurable.
21. Add new RPC frameworker JeroMq
22. Fix Zk watcher miss problem
23. Update sl4j 1.5.6 to 1.7.5
24. Shutdown worker when occur exception in Smart thread
25. Skip downloading useless topology in Supervisor
26. Redownload the topology when failed to deserialize topology in Supervisor.
27. Fix topology codeDir as resourceDir
28. Catch error when normalize topology
29. Add log when found one task is dead
30. Add maven repository, JStorm is able to build outside of Alibaba
31. Fix localOrShuffle null pointer exception
32. Add statics counting for internal tuples in one worker
33. Add thrift.close after download topology binary in Supervisor


# Release 0.9.1

## new features
1. Application classloader. when Application jar is conflict with jstorm jar, 
   please enable application classloader.
2. Group Quato, Different group with different resource quato.

## Bug fix or enhancement
1. Fix Rotation Map competition issue.
2. Set default acker number as 0
3. Set default spout/bolt number as 1
4. Add log directory in log4j configuration file
5. Add transaction example
6. Fix UI showing wrong worker numbe in topology page
7. Fix UI showing wrong latency in topology page
8. Replace hardcode Integer convert with JStormUtils.parseInt
9. Support string parse in Utils.getInt
10. Remove useless dependency in pom.xml
11. Support supervisor using IP or special hostname
12. Add more details when no resource has been assigned to one new topology
13. Replace normal thread with Smart thread
14. Add gc details 
15. Code format
16. Unify stormId and topologyId as topologyId
17. Every nimbus will regist ip to ZK



# Release 0.9.0
In this version, it will follow storm 0.9.0 interface, so the application running
on storm 0.9.0 can run in jstorm 0.9.0 without any change.

## Stability
1. provide nimbus HA. when the master nimbus shuts down, it will select another
 online nimbus to be the master. There is only one master nimbus online 
 any time and the slave nimbuses just synchronouse the master's data.
2. RPC through netty is stable, the sending speed is match with receiving speed. 


## Powerful scheduler
1. Assigning resource on four dimensions:cpu, mem, disk, net
2. Application can use old assignment.
3. Application can use user-define resource.
4. Task can apply extra cpu slot or memory slot.
4. Application can force tasks run on different supervisor or the same supervisor








# Release 0.7.1
In this version, it will follow storm 0.7.1 interface, so the topology running
in storm 0.7.1 can run in jstorm without any change.

## Stability
* Assign workers in balance
* add setting "zmq.max.queue.msg" for zeromq
* communication between worker and tasks without zeromq
* Add catch exception operation
  * in supervisor SyncProcess/SyncSupervisor
  * add catch exception and report_error in spout's open and bolt's prepare
  * in all IO operation
  * in all serialize/deserialize
  * in all ZK operation
  *  in topology upload/download function
  *  during initialization zeromq
* do assignmen/reassignment operation in one thread to avoid competition
* redesign nimbus 's topology assign algorithm, make the logic simple much.
* redesign supervisor's sync assignment algorithm, make the logic simple much
* reduce zookeeper load
  * redesign nimbus monitor logic, it will just scan tasks' hearbeat, frequency is 10s
  * nimbus cancel watch on supervisor
  * supervisor heartbeat frequence change to 10s
  * supervisor syncSupervisor/syncProcess frequence change to 10s
  * supervisor scan /$(ZKROOT)/assignment only once in one monitor loop
  * task hearbeat change to 10s
* create task pid file before connection zk, this is very import when zk is unstable.


## Performance tuning
* reduce once memory copy when deserialize tuple, improve performance huge.
* split executor thread as two thread, one handing receive tuples, one sending tuples, improve performance much
* redeisign sample code, it will sampling every 5 seconds, not every 20 tuple once, improve performance much
* simplify the ack's logic, make acker more effeciency
* Communication between worker and tasks won't use zeromq, just memory share in process
* in worker's Drainer/virtualportdispatch thread, spout/bolt recv/send thread, 
   the thread will sleep 1 ms when there is not tuple in one loop
* communication between worker and tasks without zeromq
* sampling frequence change to 5s, not every 20 tuple once.

## Enhancement:
* add IFailValueSpout interface
* Redesign sampling code, collection statics model become more common.
  *  Add sending/recving tps statics, statics is more precise.
* Atomatically do deactivate action when kill/rebalance topology, and the wait time is 2 * MSG_TIMEOUT
* fix nongrouping bug, random.nextInt will generate value less than 0.
* Sleep one setting time(default is 1 minute) after finish spout open, 
   which is used to wait other task finish initialization.
* Add check component name when submit topology, forbidding the component 
   which name start with "__"
* change the zk's node /$(ZKROOT)/storm to /$(ZKROOT)/topology
* abstract topology check logic from generating real topology function
* when supervisor is down and topology do rebalance, the alive task under down 
   supervisor is unavailable.
* add close connection operation after finish download topology binary
* automatically create all local dirtorie, such as 
   /$(LOCALDIR)/supervisor/localstate
* when killing worker, add "kill and sleep " operation before "kill -9" operation
* when generate real topology binary,
  * configuration priority different.   
      component configuration > topology configuration > system configuration
  * skip the output stream which target component doesn't exist.
  * skip the component whose parallism is 0.
  * component's parallism is less than 0, throw exception.
* skip ack/fail when inputstream setting is empty
* add topology name to the log
* fix ui select option error, default is 10 minutes
* supervisor can display all worker's status