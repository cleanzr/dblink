# dblink
A Spark package for distributed Bayesian entity resolution.

## Overview
TODO

## How to: add dblink as a dependency
\[Note: This won't work yet. Waiting for project to be accepted.\]

Maven:
```xml
<dependency>
  <groupId>com.github.ngmarchant</groupId>
  <artifactId>dblink</artifactId>
  <version>0.1.0</version>
</dependency>
```

sbt:
```scala
libraryDependencies += "com.github.ngmarchant" % "dblink" % 0.1.0
```

## How to: build a JAR
You can build a JAR using sbt by running the following command from
within the project directory:
```bash
$ sbt package
```

This should output a JAR file at `./target/scala-2.11/dblink-0.1.jar`
relative to the project directory.
Note that the JAR file does not bundle Spark or Hadoop.

## Example
TODO

Submit to Spark cluster

```bash
$SPARK_HOME/bin/spark-submit \
  --master "local" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --conf "spark.driver.extraClassPath=./target/scala-2.11/dblink-assembly-0.1.jar" \
  ./target/scala-2.11/dblink-assembly-0.1.jar \
  <config file>
```

## License
GPL-3

## Citing the package
TODO
