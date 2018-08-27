name in ThisBuild := "dblink"
description in ThisBuild := "Distributed Bayesian Entity Resolution"
licenses in ThisBuild := List("GPL-3" -> new URL("https://www.gnu.org/licenses/gpl-3.0.en.html"))
homepage in ThisBuild := Some(url("https://github.com/ngmarchant/dblink"))
version in ThisBuild := "0.1"

developers in ThisBuild := List(
  Developer(
    id    = "Your identifier",
    name  = "Neil G. Marchant",
    email = "ngmarchant@gmail.com",
    url   = url("https://github.com/ngmarchant")
  )
)

scmInfo in ThisBuild := Some(
  ScmInfo(
    url("https://github.com/ngmarchant/dblink"),
    "scm:git@github.com:ngmarchant/dblink.git"
  )
)

useGpg := true

scalaVersion := "2.11.12"

val sparkVersion = "2.2.0"
val breezeVersion = "0.13.2"
val commonsMathVersion = "3.6"
val scalatestVersion = "3.0.5"
val typesafeVersion = "1.3.2"

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-math3" % commonsMathVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources(),
  "org.apache.logging.log4j" % "log4j-scala" % "11.0",
  "com.eatthepath" % "jvptree" % "0.2",
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",
  "com.typesafe" % "config" % typesafeVersion
)

// Remove all additional repository other than Maven Central from POM
pomIncludeRepository in ThisBuild := { _ => false }
publishTo in ThisBuild := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
publishMavenStyle in ThisBuild := true

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)

test in assembly := {}
mainClass in assembly := Some("com.github.ngmarchant.dblink.Run")