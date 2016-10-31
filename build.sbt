organization := "emcastro"

name := "IOTestbed2"

version := "0.1"

scalaVersion := "2.10.6"
//scalaVersion := "2.11.8"


scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= (Seq(
  "org.scalaz" %% "scalaz-core" % "7.2.6",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "com.thoughtworks.each" %% "each" % "3.0.1"
) ++ (
  if (scalaVersion.value startsWith "2.10")
    Nil
  else
    Seq("com.typesafe.scala-logging" %% "scala-logging" % "3.5.0")
  ))

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.0" % Test,
  "uk.org.lidalia" % "slf4j-test" % "1.1.0" // % Test
)

