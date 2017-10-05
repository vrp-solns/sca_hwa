Contents

	HdfsWrite.scala - Scala source code
	build/ - folder for scala class files
	lib - folder for lib files

Assumption
	Hadoop cluster is up and running
	Scala is installed

Compile
	cd scala-hdfs
	scalac -classpath lib/hadoop-common-2.7.1.jar:lib/hadoop-mapreduce-client-core-2.7.1.jar:lib/hadoop-hdfs-2.7.2.jar -d build *.scala


Run
	cd build
	scala -classpath ../lib/hadoop-common-2.7.1.jar:../lib/hadoop-mapreduce-client-core-2.7.1.jar:../lib/commons-logging-1.2.jar:../lib/guava-23.0.jar:../lib/commons-collections-3.2.2.jar:../lib/commons-configuration-1.10.jar:../lib/commons-lang-2.6.jar:../lib/hadoop-auth-2.7.1.jar:../lib/slf4j-api-1.7.20.jar:../lib/log4j-1.2.15.jar:../lib/hadoop-hdfs-2.7.2.jar:../lib/htrace-core-3.1.0-incubating.jar:../lib/servlet-api-2.3.jar:../lib/commons-cli-1.1.jar:../lib/protobuf-java-3.4.0.jar:../lib/commons-io-2.5.jar:. com.example.HdfsWrite