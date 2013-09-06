JStorm is an implementation of Storm in java.

If you have any question, please contact hustjackie@gmail.com

# Why we develop JStorm?
Storm is a wonderful product, but it is developed in Clojure,which is not a popular language. 
And in our company, few guys can fix the bug of Storm, which we already found in zeromq, zookeeper, performance,etc.<br />
So, we decided to implement JStorm.
# What's the status of JStorm?
Alipay began to use JStorm in May 2013.Till Sep 2013, JStorm in Alipay can handle  
40TB/3billion messages from other clusters . In some topologies, no message misses.
# What is the difference between JStorm and Storm?
Generally, JStorm is an implementation of Storm in java, major client interfaces are the same as that of Storm. 
Any topology running on Storm can also run on JStorm, no need to modify . <br />
Key improvements as follows:.<br />
<br />JStorm is more stable than Storm;
<br />JStorm is faster than Storm;
<br />JStorm provides several new and very useful features.
<br />Please refer to "changehistory" for details.
# How to build and deploy?
Please refer to "deploy" for details.
# Contributors
[@longdafeng(∑‚÷Ÿ—Õ)](https://github.com/longdafeng)<br />
[@tumen(¿ÓˆŒ)](https://github.com/tumen)<br />
[@muyannian(ƒ∏—”ƒÍ)](https://github.com/muyannian)<br />
[@zhouxinxust(÷‹ˆŒ)](https://github.com/zhouxinxust)<br />