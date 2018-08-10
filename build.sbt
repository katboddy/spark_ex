name := "spark_ex"
version := "0.1"

scalaVersion := "2.11.0"
val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % sparkVersion,
  "com.amazonaws" % "aws-java-sdk-sts" % "1.11.76",
  "com.amazonaws" % "amazon-kinesis-client" % "1.7.3",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion classifier "tests",
  "org.mockito" % "mockito-all" % "1.10.19" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

