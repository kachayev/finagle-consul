scalaVersion := "2.11.6"

organization := "com.twitter.finagle"

name := "finagle-consul"

version := "0.0.1"

resolvers += "twttr" at "http://maven.twttr.com/"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

libraryDependencies ++= Seq(
  // twitter infrastructure:
  "com.twitter" %% "finagle-core" % "6.24.0",
  "com.twitter" %% "twitter-server" % "1.9.0",
  // json processor:
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)
