name := "hadoop-capstone"

version := "0.1"

scalaVersion := "2.10.7"

resolvers += "conjars.org" at "http://conjars.org/repo"

libraryDependencies += "com.opencsv" % "opencsv" % "4.0"
libraryDependencies += "org.apache.flume" % "flume-ng-core" % "1.8.0"
libraryDependencies += "org.apache.hive" % "hive-exec" % "2.2.0"
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "3.0.4" % "test"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.7"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.7"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.7"
libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.6"
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.6.7.1"
libraryDependencies += "io.netty" % "netty-all" % "4.0.29.Final"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0"

excludeDependencies += "commons-lang" % "commons-lang"
excludeDependencies += "org.mortbay.jetty"