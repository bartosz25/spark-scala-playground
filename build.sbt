name := "spark-scala"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % s"${sparkVersion}"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % s"${sparkVersion}"
libraryDependencies += "org.apache.spark" %% "spark-sql" % s"${sparkVersion}"
libraryDependencies += "org.apache.spark" %% "spark-hive" % s"${sparkVersion}"
// Query [id = fd565f20-fbaf-4202-981a-ee9161ff7235, runId = 2218668e-a963-4956-9e6d-ba9f3b0e8c7b] terminated with exception: Job aborted due to stage failure: Task 69 in stage 1.0 failed 1 times, most recent failure: Lost task 69.0 in stage 1.0 (TID 200, localhost, executor driver): java.lang.NoSuchMethodError: net.jpountz.lz4.LZ4BlockInputStream.<init>(Ljava/io/InputStream;Z)V
libraryDependencies += "net.jpountz.lz4" % "lz4" % "1.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % s"${sparkVersion}"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.5"
// Contains Jetty implementation and other network features
libraryDependencies += "org.apache.spark" %% "spark-network-common" % s"${sparkVersion}"
// Yarn cluster manager code
libraryDependencies += "org.apache.spark" %% "spark-yarn" % s"${sparkVersion}"
// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % s"${sparkVersion}"

// https://mvnrepository.com/artifact/com.h2database/h2
libraryDependencies += "com.h2database" % "h2" % "1.4.195"
libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.6"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.0"

//resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
libraryDependencies += "graphframes" % "graphframes" % "0.6.0-spark2.3-s_2.11"