val sparkCore = "org.apache.spark" %% "spark-core" % "2.3.1" % "provided" withSources()
val sparkSql = "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided" withSources()
val commonsMath3 = "org.apache.commons" % "commons-math3" % "3.6.1"
val log4j = "org.apache.logging.log4j" % "log4j-scala" % "11.0"
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % "test"
val typesafe = "com.typesafe" % "config" % "1.3.2"
val jvptree = "com.eatthepath" % "jvptree" % "0.2"

ThisBuild / name := "dblink"
ThisBuild / version := "0.1-dev"
ThisBuild / scalaVersion := "2.11.12"

ThisBuild / developers := List(
  Developer(
    id    = "Your identifier",
    name  = "Neil G. Marchant",
    email = "ngmarchant@gmail.com",
    url   = url("https://github.com/ngmarchant")
  )
)

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/ngmarchant/dblink"),
    "scm:git@github.com:ngmarchant/dblink.git"
  )
)

ThisBuild / description := "Distributed Bayesian Entity Resolution"
ThisBuild / licenses := List("GPL-3" -> new URL("https://www.gnu.org/licenses/gpl-3.0.en.html"))
ThisBuild / homepage := Some(url("https://github.com/cleanzr/dblink"))

useGpg := true

libraryDependencies ++= Seq(
  commonsMath3,
  sparkCore,
  sparkSql,
  log4j,
  scalaTest,
  typesafe,
  jvptree
)

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)

test in assembly := {}
mainClass in assembly := Some("com.github.cleanzr.dblink.Run")