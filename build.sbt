name := "cloud"

organization := "codecraft"

scalaVersion := "2.11.8"

version := "1.0.0-SNAPSHOT"

resolvers ++= Seq(
  "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
)

libraryDependencies ++= Seq(
  "com.rabbitmq" % "amqp-client" % "3.6.1",
  "com.typesafe.akka" %% "akka-actor" % "2.4.6",
  "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test",
  "com.typesafe.play" %% "play-json" % "2.5.4",
  "codecraft" %% "codegen" % "1.0.0-SNAPSHOT",
  "codecraft" %% "registry-model" % "1.0.0-SNAPSHOT",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "net.databinder" % "unfiltered-netty-server_2.11" % "0.9.0-beta1",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.3"
)
