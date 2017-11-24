DS:BDA Task 2 Report from Mr-yakunin (github: https://github.com/Mr-yakunin/task_2)

### Challenge (Task 2) ###

- Program which aggregate raw metrics into selected scale
		Data input format: metricId, timestamp, value
		Data output format: metricId, timestamp, scale, value;
- Ingest technology: Flume from file agent;
- Storage technology: HDFS;
- Computation technology: Spark SQL (DataFrame, DataSet).

### Report contains ###

- README.txt (report and instructions);
- working application with source code src/main/java/JavaAggregator.java;
- unit tests with source code src/test/java/JavaTests.java and test file 
src/test/resources/testinput.txt;
- text_generator directory with python script (makefile.py) with generated file 
testfile.txt, script upload generated file into HDFS;
- pom.xml file that contains information about the project and various 
configuration detail used by Maven to build the project;
- screenshots directory with successfully executed tests, successfully executed 
job and result (logs), successfully worked flume agent, system components 
communication;
- logs directory with contains some txt file logs from maven;
- flume_conf/application.conf file configuration for flume agent.

### Requirements ###

- Linux-based OS;
- apache hadoop version 2.8.2;
- apache flume-ng version 1.8.0;
- apache spark version 2.2.0;
- java version 1.8.0_151;
- scala version 2.12.4;
- maven version 3.3.9.

### Install Java ###

To install and use java you can do by visiting the following link (use tutorial): 
https://www.java.com/en/download/help/linux_x64_install.xml

For setting up PATH and JAVA_HOME variables, add the following commands to 
~/.bashrc file.
#
	export JAVA_HOME=/usr/local/jdk1.7.0_71
	export PATH=PATH:$JAVA_HOME/bin
#
Now verify the java -version command from the terminal.
If everything works fine it will give you the following output.
$ source .bashrc
$ java -version
java version "1.8.0_151"
Java(TM) SE Runtime Environment (build 1.8.0_151-b12)
Java HotSpot(TM) 64-Bit Server VM (build 25.151-b12, mixed mode)

### Hadoop install on Linux ###

To install and use hadoop you can do by visiting the following links (use tutorial):
https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-common/SingleCluster.html 
https://www.edureka.co/blog/install-hadoop-single-node-hadoop-cluster

Setting up variables in ~/.bashrc file.
#
	export HADOOP_HOME=/usr/local/hadoop
	export PATH=$PATH:$HADOOP_HOME/bin
	export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
	export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
	export JAVA_LIBRARY_PATH=$HADOOP_HOME/lib/native/
	export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/
#
Configure hadoop:
1. Create a "data" folder inside hadoop directory ($HADOOP_HOME), and also create
two more folders in the "data" folder as "data" and "name".
2. Create a folder to store temporary data during execution of a project, such as 
"$HADOOP_HOME/temp".
3. Create a log folder, such as "$HADOOP_HOME/userlog"
4. Go to "$HADOOP_HOME/etc/hadoop" and edit four files (add these properties)  
in <configuration></configuration> (put site-specific property in this file):
	i. core-site.xml

	<property>
		<name>hadoop.tmp.dir</name>
		<value>$HADOOP_HOME/temp</value>
	</property>

	<property>
		<name>fs.default.name</name>
		<value>hdfs://localhost:50071</value>
	</property>

	<property>
		<name>io.compression.codecs</name>
		<value>
			org.apache.hadoop.io.compress.GzipCodec,
			org.apache.hadoop.io.compress.DefaultCodec,
			org.apache.hadoop.io.compress.BZip2Codec,
			org.apache.hadoop.io.compress.SnappyCodec
		</value>
	</property>

	ii. hdfs-site.xml

	<property>
		<name>dfs.replication</name><value>1</value>
	</property>

	<property>
		<name>dfs.namenode.name.dir</name>
		<value>$HADOOP_HOME/data/name</value><final>true</final>
	</property>

	<property>
		<name>dfs.datanode.data.dir</name>
		<value>$HADOOP_HOME/data/data</value><final>true</final>
	</property>

	iii. mapred-site.xml

	<property>
		<name>mapred.job.tracker</name>
		<value>localhost:9001</value>
	</property>

	iv. yarn.xml

	<property>
		<name>yarn.nodemanager.log-dirs</name>
		<value>$HADOOP_HOME/userlog</value><final>true</final>
	</property>

	<property>
		<name>yarn.nodemanager.local-dirs</name>
		<value>$HADOOP_HOME/temp/nm-local-dir</value>
	</property>

Now verify the hadoop version command from the terminal.
If everything works fine it will give you the following output.	
$ source .bashrc
$ hadoop version
Hadoop 2.8.2
Subversion https://git-wip-us.apache.org/repos/asf/hadoop.git -r 66c47f2a01ad9637879e95f80c41f798373828fb
Compiled by jdu on 2017-10-19T20:39Z
Compiled with protoc 2.5.0
From source with checksum dce55e5afe30c210816b39b631a53b1d
This command was run using /usr/local/hadoop/share/hadoop/common/hadoop-common-2.8.2.jar

### Install Maven ###

To install and use maven you can do by visiting the following links (use tutorial):
https://maven.apache.org/install.html
http://www.baeldung.com/install-maven-on-windows-linux-mac

Now verify the maven version command from the terminal.
If everything works fine it will give you the following output.	
$ mvn -version
Apache Maven 3.3.9
Maven home: /usr/share/maven
Java version: 1.8.0_151, vendor: Oracle Corporation
Java home: /usr/local/jdk1.8.0_151/jre
Default locale: ru_RU, platform encoding: UTF-8
OS name: "linux", version: "4.9.0-4-amd64", arch: "amd64", family: "unix"

### Install Scala ###

To install and use scala you can do by visiting the following link (use tutorial):
https://scala-lang.org/download/install.html

Setting up PATH variable in ~/.bashrc file.
#
	export PATH=$PATH:/usr/local/scala/bin
#

Now verify the scala version command from the terminal.
If everything works fine it will give you the following output.	
$ source .bashrc
$ scala -version
Scala code runner version 2.12.4 -- Copyright 2002-2017, LAMP/EPFL and Lightbend, Inc.

### Install Spark ###

To install and use spark you can do by visiting the following link (use tutorial):
https://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm

Setting up PATH variable in ~/.bashrc file.
#
	export PATH=$PATH:/usr/local/spark/bin
#

Now verify the spark version command from the terminal.
If everything works fine it will give you the following output.	
$ source .bashrc
$ spark-submit --version
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.2.0
      /_/
                        
Using Scala version 2.11.8, Java HotSpot(TM) 64-Bit Server VM, 1.8.0_151
Branch 
Compiled by user jenkins on 2017-06-30T22:58:04Z
Revision 
Url 
Type --help for more information.

### Install Flume ###

To install and use spark you can do by visiting the following link (use tutorial):
https://data-flair.training/blogs/apache-flume-installation-tutorial/

Setting up variables in ~/.bashrc file.
#
	export FLUME_HOME=/usr/local/flume
	export PATH=$PATH:/usr/local/flume/bin
	export CLASSPATH=$CLASSPATH:/$FLUME_HOME/lib/*
#

Now verify the flume version command from the terminal.
If everything works fine it will give you the following output.	
$ source .bashrc
$ flume-ng version
Flume 1.8.0
Source code repository: https://git-wip-us.apache.org/repos/asf/flume.git
Revision: 99f591994468633fc6f8701c5fc53e0214b6da4f
Compiled by denes on Fri Sep 15 14:58:00 CEST 2017
From source with checksum fbb44c8c8fb63a49be0a59e27316833d

### Get test files for application ###

To create testing file for application you must start your hadoop cluster.
After this go to folder text_generator and run python script for creating test
file with name testfile.txt. This script upload generated file into /app/test in
HDFS. 
To run start script use:
$ python3 makefile.py
Screenshots of successfully generated file and uploaded it into HDFS you will 
see in screenshots/make_file_test.png and screenshots/upload_test_file.png.

### Start Flume agent ###

To run flume agent I need the conf file, that stores in $FLUME_HOME/conf.
In this project you can find it in flume_conf/application.conf.
This file describes a single-node Flume configuration that contatins:
- name the components of this agent4
- describe/configure the source;
- describe the sink;
- bind the source and sink to the channel.
The agent recieves data from spark application using avro source in localhost:44444
and saves if to HDFS in hdfs://localhost:50071/flume.
To run agent from $FLUME_HOME/conf use:
$ flume-ng agent --conf conf --conf-file application.conf --name application -Dflume.root.logger=INFO,console
Screenshots of successfully worked flume agent you can find in
screenshots/flume_worked_*.png.

### Testing Application ###

Source code of application testing methods you can fine in 
src/test/java/JavaTests.java. I use Junit java. Such dependency you can find in
pom.xml file. 
So, I test the following functions (methods) of my application:
- creating schema;
- get average value by id from sql operation;
- get minimum value by id from sql operation;
- get maximum value by id from sql operation;
- read from test file;
- using Metric class as schema for creating dataframe.
To run tests do following from project folder (where you can find pom.xml):
$ mvn package 
$ mvn -Dtest=JavaTests test
Screenshots of successfully executed tests and logs you can find in 
screenshots/executed_tests_*.png and logs/JavaTests.txt, logs/TEST-JavaTests.xml

### Main application ###

Application reads input text file (/app/test that was prepared and uploaded to 
HDFS). Then I check input arguments: it must be only two with input file and 
scale metric. Then I read the input file and create a RDD of Metric objects, 
apply a schema to an RDD of JavaBeans to get a DataFrame. After this execute sql
operation for obtaining the desired metrics (scale argument in args). Then I 
prepare data for sending it to flume agent, that stores the result of program in
HDFS. Then I close the RPC connection, stop spark session.
The output data format: metricId, timestamp, scale, value
To run spark application use:
$ spark-submit --class JavaAggregator --master local target/myid-0.0.1.jar <input file> <scale>
Example:
$ spark-submit --class JavaAggregator --master local target/myid-0.0.1.jar /app/test avg
Screenshots of successfully building project by maven you can find in
screenshots/build_mvn_package_*.png
Screenshots of successfully executed job and result you can find in
screenshots/executed_spark_job_*.png, screenshots/executed_spark_flume_result_*.png.
System components communication you can find in screenshots/system_components_communication.png

Thanks.

Copyright © 2017, All rights reserved