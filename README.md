# dblink
A Spark package for distributed Bayesian entity resolution.

## Overview
**TODO**

## How to: Add dblink as a project dependency
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

## How to: Build a fat JAR
You can build a fat JAR using sbt by running the following command from
within the project directory:
```bash
$ sbt assembly
```

This should output a JAR file at `./target/scala-2.11/dblink-assembly-0.1.jar`
relative to the project directory.
Note that the JAR file does not bundle Spark or Hadoop, but it does include
all other dependencies.

## Example: RLdata500
A small data set called `RLdata500` is included in the examples directory as a
CSV file.
It was extracted from the [RecordLinkage](https://cran.r-project.org/web/packages/RecordLinkage/index.html)
R package and contains 500 synthetically generated records, with some distorted
values.
A dblink config file `RLdata500.conf` is included in the examples directory for
this data set.
To run it, build the fat JAR according to the instructions above, then use
`spark-submit` as follows:
```bash
$SPARK_HOME/bin/spark-submit \
  --master local[1] \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --conf "spark.driver.extraClassPath=./target/scala-2.11/dblink-assembly-0.1.jar" \
  ./target/scala-2.11/dblink-assembly-0.1.jar \
  ./examples/RLdata500.conf
```

The output will be saved at `./examples/RLdata500_results/` (as specified in
the dblink config file).

## License
GPL-3

## Citing the package
**TODO**