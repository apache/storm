apt-get --yes install zookeeper=3.3.5* zookeeperd=3.3.5*
service zookeeper stop
echo maxClientCnxns=200 >> /etc/zookeeper/conf/zoo.cfg
service zookeeper start
