

Implement Storm with Java.

if you have any question, please contact to hustjackie@gmail.com

1. Why we wan't implement JStorm?

Storm is wonderful product, but it is implemented with Clojure. We don't like clojure.
it isn't a populate language, so in Alibaba, few people can fix the bug of storm.
We are the first users of Storm since it has been open source. During using, we found 
several problem such as zeromq, zookeeper, performance. 

So we decide to implement JStorm.


2. What's the status of JStorm?

From 20130430, Alipay begin to use JStorm. In Alipay, JStorm will handle 
40TB/3billion message from outside. In some topology, it won't allow lose one message.


3. What is difference between JStorm and Storm?

In general, JStorm is Java Storm, most of all client interfaces are from Storm. so the 
old topology can directly run on JStorm without any change.

But there are 3 point different from Storm.

3.1 JStorm is more stable than Storm
3.2 JStorm is more fast than Storm
3.3 Jstorm provide more useful feature.

Please refer to the file "changehistory"


4. How to build/deploy?

Please refer to the file "deploy"



    