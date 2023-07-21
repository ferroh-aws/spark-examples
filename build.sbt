ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "spark-examples",
    resolvers += "AmazonEMR" at "https://s3.us-west-1.amazonaws.com/us-west-1-emr-artifacts/emr-6.11.0/repos/maven/",
    libraryDependencies ++= Seq(
      "io.delta" %% "delta-core" % "2.2.0" % "provided",
      "org.apache.hudi" % "hudi" % "0.13.0" % "provided",
      "org.apache.iceberg" % "iceberg-core" % "1.2.0" % "provided",
      "org.apache.iceberg" %% "iceberg-spark-runtime-3.3" % "1.2.0" % "provided",
      "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided"
    ),
    assemblyJarName := "spark-examples-all.jar",
    assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
