name := "spark-scala"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % s"${sparkVersion}"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % s"${sparkVersion}"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % s"${sparkVersion}"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % s"${sparkVersion}"
// Query [id = fd565f20-fbaf-4202-981a-ee9161ff7235, runId = 2218668e-a963-4956-9e6d-ba9f3b0e8c7b] terminated with exception: Job aborted due to stage failure: Task 69 in stage 1.0 failed 1 times, most recent failure: Lost task 69.0 in stage 1.0 (TID 200, localhost, executor driver): java.lang.NoSuchMethodError: net.jpountz.lz4.LZ4BlockInputStream.<init>(Ljava/io/InputStream;Z)V
//libraryDependencies += "net.jpountz.lz4" % "lz4-java" % "1.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % s"${sparkVersion}"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.5"
// Contains Jetty implementation and other network features
libraryDependencies += "org.apache.spark" % "spark-network-common_2.11" % s"${sparkVersion}"
// Yarn cluster manager code
libraryDependencies += "org.apache.spark" % "spark-yarn_2.11" % s"${sparkVersion}"
// https://mvnrepository.com/artifact/com.h2database/h2
libraryDependencies += "com.h2database" % "h2" % "1.4.195"
libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.6"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.0"
