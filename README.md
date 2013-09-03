JStorm is an implementation storm in java.

if you have any question, please contact to hustjackie@gmail.com

# Why we want to develop JStorm?
Storm is a wonderful product, but it is being developed in Clojure. We don't like clojure,
which isn't a populate language. So in Alibaba, few guys can fix the bug of storm.<br />
At the same time, we found several problem such as zeromq, zookeeper, performance. 
So we decide to implement JStorm, implementing storm in java.
# What's the status of JStorm?
From 2013-05-01, Alipay begin to use JStorm. Till now 2013-09-01, JStorm of Alipay will handle 
40TB/3billion messages from other cluster . In some topologies, it won't allow lose one message.
# What is the difference between JStorm and Storm?
In general, JStorm is an implementation storm in java, most of client interfaces are from Storm. so the 
old topology can directly run on JStorm without any change, even building.<br />
But there are 3 points different from Storm.<br />
<br />JStorm is more stable than Storm
<br />JStorm is more fast than Storm
<br />Jstorm provide more useful features.
<br />Please refer to the file "changehistory" for details
# How to build/deploy?
Please refer to the file "deploy" for details