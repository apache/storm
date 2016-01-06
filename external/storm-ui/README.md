##Package
* package war

```
mvn clean package -DskipTests=true -Dwar
cp ./target/storm-ui.war $TOMCAT_HOME/webapps/
```

* package jar 

```
mvn clean package -DskipTests=true
cp ./target/storm-ui-*.jar  $STORM_HOME/external/storm-ui/
```
## MUST Config STORM_CONF_DIR

You can specify the storm configuration directory by setting system environment variable. By default,
you can create a new directory named ".storm" in the user home,and set the storm configuration by storm.yaml

* config by setting system environment variable

```    
   export STORM_CONF_DIR
```

* Create a new directory named ".storm" in the user home. Like "~/.storm"

```    
    cd ~
    mkdir .storm
    vim storm.yaml
```

## How to Deploy to Tomcat
* Download apache-tomcat

```
  wget http://apache.fayea.com/apache-mirror/tomcat/tomcat-7/v7.0.56/bin/apache-tomcat-7.0.56.tar.gz
```
* Config Env

```
vi /etc/profile
JAVA_HOME=~/software/jdk-current
CLASS_PATH=$JAVA_HOME/lib:JAVA_HOME/jre/lib:JAVA_HOME/lib/tools.jar:$CLASS_PATH
PATH=$JAVA_HOME/bin:$PATH

TOMCAT_HOME=~/software/apache-tomcat-7.0.56
CATALINA_HOME=$TOMCAT_HOME
PATH=$TOMCAT_HOME/bin:$PATH

export PATH USER LOGNAME MAIL HOSTNAME HISTSIZE INPUTRC CLASS_PATH JAVA_HOME TOMCAT_HOME CATALINA_HOME
```
*modify $TOMCAT_HOME/conf/server.xml

```
add "<Context path="" docBase="storm-ui" debug="0"  reloadable="true" crossContext="true"/>" between <host> and </host>. 
Like:
    <Host name="localhost"  appBase="webapps"
            unpackWARs="true" autoDeploy="true">
          
        <!-- SingleSignOn valve, share authentication between web applications
             Documentation at: /docs/config/valve.html -->
        <!--
        <Valve className="org.apache.catalina.authenticator.SingleSignOn" />
        --> 

        <!-- Access log processes all example.
             Documentation at: /docs/config/valve.html 
             Note: The pattern used is equivalent to using pattern="common" -->
        <Valve className="org.apache.catalina.valves.AccessLogValve" directory="logs"
               prefix="localhost_access_log." suffix=".txt"
               pattern="%h %l %u %t &quot;%r&quot; %s %b" />
    
       <Context path="" docBase="storm-ui" debug="0"  reloadable="true" crossContext="true"/>
       
    </Host>
```

* modify ./startup.sh & shutdown.sh

```
write  Config Env to ./startup.sh & shutdown.sh
```
* Start TomCat Server

```
# $TOMCAT_HOME/bin/startup.sh
Using CATALINA_BASE:  ~/software/apache-tomcat-7.0.56
Using CATALINA_HOME:  ~/software/apache-tomcat-7.0.56
Using CATALINA_TMPDIR: ~/software/apache-tomcat-7.0.56/temp
Using JRE_HOME:  ~/software/jdk1.6.0_45
Using CLASSPATH:   ~/software/apache-tomcat-7.0.56/bin/bootstrap.jar:~/software/apache-tomcat-7.0.56/bin/tomcat-juli.jar
```
* Shut Down TomCat Server

```
# $TOMCAT_HOME/bin/shutdown.sh
```
