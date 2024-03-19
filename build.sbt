ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "MeowSparkToolkit"
  )


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.2" % "provided",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
  "org.apache.spark" %% "spark-mllib" % "3.4.2" % "provided",
  // "com.microsoft.azure" %% "synapseml" % "0.9.5"
    "com.typesafe.play" %% "play-json" % "2.9.2",
    "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.1.2",
)
//
//
//// https://mvnrepository.com/artifact/org.apache.thrift/libthrift
////libraryDependencies += "org.apache.thrift" % "libthrift" % "0.9.2"
//
//// library to work with json file and call api
//libraryDependencies ++= Seq(
//  "com.typesafe.play" %% "play-json" % "2.9.2",
//  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.1.2",
//)
//unmanagedBase := baseDirectory.value / "lib"
//
 assembly/assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
//
//
//assembly/assemblyMergeStrategy := {
//  case PathList("META-INF", xs*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}