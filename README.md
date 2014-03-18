Spark filter examples
=====================

This project using the calliope project to integrate Cassandra and spark. This has been tested with Apache Cassandra 2.0.5 and DSE 4.0.

http://tuplejump.github.io/calliope/


First start an instance of Cassandra locally. Then you will need to populate the Cassandra cluster with data from
	
	https://github.com/PatrickCallaghan/datastax-storm-cql3-demo

To run this example run the following

	mvn install exec:java -Dexec.mainClass=com.datastax.spark.RiskSensitivityFilter


