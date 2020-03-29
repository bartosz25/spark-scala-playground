name := "spark-scala"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % s"${sparkVersion}"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % s"${sparkVersion}"
libraryDependencies += "org.apache.spark" %% "spark-sql" % s"${sparkVersion}"
libraryDependencies += "org.apache.spark" %% "spark-hive" % s"${sparkVersion}"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % s"${sparkVersion}"
libraryDependencies += "org.apache.spark" %% "spark-avro" %  s"${sparkVersion}"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// Contains Jetty implementation and other network features
libraryDependencies += "org.apache.spark" %% "spark-network-common" % s"${sparkVersion}"
// Yarn cluster manager code
libraryDependencies += "org.apache.spark" %% "spark-yarn" % s"${sparkVersion}"
// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % s"${sparkVersion}"

// https://mvnrepository.com/artifact/com.h2database/h2
libraryDependencies += "com.h2database" % "h2" % "1.4.195"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.11"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.0"

// Force one version for conflicting dependencies
libraryDependencies += "com.google.guava" % "guava" % "12.0.1"
libraryDependencies += "jline" % "jline" % "2.14.3"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
resolvers += "DynamoDBLocal" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"

libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.3-s_2.11"
libraryDependencies += "org.aspectj" % "aspectjweaver" % "1.7.3"
libraryDependencies += "org.aspectj" % "aspectjrt" % "1.7.3"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.664"
libraryDependencies += "com.amazonaws" % "DynamoDBLocal" % "1.11.86"

libraryDependencies += "com.amazon.deequ" % "deequ" % "1.0.2"

libraryDependencies += "com.almworks.sqlite4java" % "sqlite4java" % "latest.integration" % "test"
libraryDependencies += "com.almworks.sqlite4java" % "sqlite4java-win32-x86" % "latest.integration" % "test"
libraryDependencies += "com.almworks.sqlite4java" % "sqlite4java-win32-x64" % "latest.integration" % "test"
libraryDependencies += "com.almworks.sqlite4java" % "libsqlite4java-osx" % "latest.integration" % "test"
libraryDependencies += "com.almworks.sqlite4java" % "libsqlite4java-linux-i386" % "latest.integration" % "test"
libraryDependencies += "com.almworks.sqlite4java" % "libsqlite4java-linux-amd64" % "latest.integration" % "test"

libraryDependencies += "io.delta" %% "delta-core" % "0.5.0"

/*
If you run tests from com.waitingforcode.sparksummit2019.customstatestore,
all `sbt compile`
lazy val copyJars = taskKey[Unit]("copyJars")
copyJars := {
  import java.nio.file.Files
  import java.io.File
  // For Local Dynamo DB to work, we need to copy SQLLite native libs from
  // our test dependencies into a directory that Java can find ("lib" in this case)
  // Then in our Java/Scala program, we need to set System.setProperty("sqlite4java.library.path", "lib");
  // before attempting to instantiate a DynamoDBEmbedded instance
  val artifactTypes = Set("dylib", "so", "dll")
  val files = Classpaths.managedJars(Test, artifactTypes, update.value).files
  Files.createDirectories(new File(baseDirectory.value, "native-libs").toPath)
  files.foreach { f =>
    val fileToCopy = new File("native-libs", f.name)
    if (!fileToCopy.exists()) {
      Files.copy(f.toPath, fileToCopy.toPath)
    }
  }
}

(compile in Compile) := (compile in Compile).dependsOn(copyJars).value
 */