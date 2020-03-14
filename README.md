# dblink: Distributed End-to-End Bayesian Entity Resolution
`dblink` is a Spark package for performing unsupervised entity resolution 
(ER) on structured data.
It's based on a Bayesian model called `blink` 
[(Steorts, 2015)](https://projecteuclid.org/euclid.ba/1441790411), 
with extensions proposed in
[(Marchant et al, 2019)](https://arxiv.org/abs/1909.06039).
Unlike many ER algorithms, `dblink` approximates the full posterior 
distribution over clusterings of records (into entities).
This facilitates propagation of uncertainty to post-ER analysis, 
and provides a framework for answering probabilistic queries about entity 
membership.

`dblink` approximates the posterior using Markov chain Monte Carlo.
It writes samples (of clustering configurations) to disk in Parquet format.
Diagnostic summary statistics are also written to disk in CSV format—these are 
useful for assessing convergence of the Markov chain.

## Documentation
The step-by-step [guide](docs/guide.md) includes information about 
building dblink from source and running it locally on a test data set. 
Further details about configuration options for dblink is provided 
[here](docs/configuration.md).

## Example: RLdata
Two synthetic data sets RLdata500 and RLdata10000 are included in the examples 
directory as CSV files.
These data sets were extracted from the [RecordLinkage](https://cran.r-project.org/web/packages/RecordLinkage/index.html)
R package and have been used as benchmark data sets in the entity resolution 
literature.
Both contain 10 percent duplicates and are non-trivial to link due to added 
distortion.
Standard entity resolution metrics can be computed as unique ids are provided 
in the files.
Config files for these data sets are included in the examples directory: 
see `RLdata500.conf` and `RLdata10000.conf`.
To run these examples locally (in Spark pseudocluster mode), 
ensure you've built or obtained the JAR according to the instructions 
above, then change into the source code directory and run the following 
command:
```bash
$SPARK_HOME/bin/spark-submit \
  --master "local[*]" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --conf "spark.driver.extraClassPath=./target/scala-2.11/dblink-assembly-0.2.0.jar" \
  ./target/scala-2.11/dblink-assembly-0.2.0.jar \
  ./examples/RLdata500.conf
```
(To run with RLdata10000 instead, replace `RLdata500.conf` with 
`RLdata10000.conf`.)
Note that the config file specifies that output will be saved in
the `./examples/RLdata500_results/` (or `./examples/RLdata10000_results`) 
directory. 

## How to: Add dblink as a project dependency
_Note: This won't work yet. Waiting for project to be accepted._

Maven:
```xml
<dependency>
  <groupId>com.github.cleanzr</groupId>
  <artifactId>dblink</artifactId>
  <version>0.2.0</version>
</dependency>
```

sbt:
```scala
libraryDependencies += "com.github.cleanzr" % "dblink" % "0.2.0"
```

## How to: Build a fat JAR
You can build a fat JAR using sbt by running the following command from
within the project directory:
```bash
$ sbt assembly
```

This should output a JAR file at `./target/scala-2.11/dblink-assembly-0.2.0.jar`
relative to the project directory.
Note that the JAR file does not bundle Spark or Hadoop, but it does include
all other dependencies.

## Contact
If you encounter problems, please [open an issue](https://github.com/ngmarchant/dblink/issues) 
on GitHub. 
You can also contact the main developer by email `<GitHub username> <at> gmail.com`

## License
GPL-3

## Citing the package
Marchant, N. G., Steorts R. C., Kaplan, A., Rubinstein, B. I. P., Elazar, D. N. 
(2019). dblink: Distributed End-to-End Bayesian Entity Resolution. _eprint 
arXiv:1909.06039_ URL: [https://arxiv.org/abs/1909.06039](https://arxiv.org/abs/1909.06039).
