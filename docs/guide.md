# Step-by-step guide

## 1. Install Java
You will need to install Java 8+ on your system. To check whether it is 
installed on macOS or Linux, run the command
```bash
$ java -version
```
You should see a version number greater than 8 (or equivalently, greater 
than 1.8).
Installation instructions for Windows, macOS and Linux are available 
[here](https://java.com/en/download/help/download_options.xml).

## 2. Install Apache Spark
You will need to ensure the correct version of Spark is installed on your 
hardware. Installing Spark locally on a single machine is relatively 
straightforward. Simply download a prebuilt version from 
[here](https://spark.apache.org/downloads.html) (we recommend the 2.3.x series)
and extract the files to a convenient location on your filesystem. 
Older releases are available from the 
[release archive](https://archive.apache.org/dist/spark/).
Installing Spark on a cluster is more involved, and there are several 
deployment options to consider. This is beyond the scope of this guide; 
we refer readers to instructions available 
[here](https://spark.apache.org/docs/latest/#launching-on-a-cluster).

Below are more detailed instructions for a Linux system.

Download Spark 2.3.1.
```bash
$ wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
```
Extract the archive.
```bash
$ tar -xvf spark-2.3.1-bin-hadoop2.7.tgz
```
Move the Spark folder to /opt and create a symbolic link so that you can easily 
switch to another version in the future.
```bash
$ sudo mv spark-2.3.1-bin-hadoop2.7 /opt
$ sudo ln -s /opt/spark-2.3.1-bin-hadoop2.7/ /opt/spark
```

Define the `SPARK_HOME` environment variable and add the Spark binaries to 
your `PATH`. 
The way that this is done depends on your operating system and/or shell.
Assuming enviornment variables are defined in `~/.profile`, you can 
run the following commands:
```bash
$ echo 'export SPARK_HOME=/opt/spark' >> ~/.profile
$ echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.profile
```

***Comment from RCS: Don't we still need to source ~/.profile so it updates?

* If using Bash on Debian, Fedora or RHEL derivatives, environment 
variables are typically defined in `~/.bash_profile` rather than 
`~/.profile`
* If using ZSH, environment variables are typically defined in 
`~/.zprofile`
* You can check which shell you're using by running `echo $SHELL`

## 3. Obtain the dblink JAR file
In this step you'll obtain the dblink fat JAR, which will have file name 
`dblink-assembly-0.1.jar`.
It contains all of the class files and resources, packed together with 
dependencies.

There are two options:
* Download a prebuilt JAR from [here](http://). 
This has been built against Spark 2.3.1 and is not guaranteed to work with 
other versions of Spark.
* Build the fat JAR file from source.

### Building the fat JAR
The build tool used for dblink is called sbt. You will need to install 
sbt on your system. Instructions are available for Windows, macOS and Linux 
in the sbt 
[documentation](https://www.scala-sbt.org/1.x/docs/Setup.html).

On Linux, you can verify that sbt is installed correctly by running.
```bash
$ sbt about
```

Once you've successfully installed sbt, get the dblink source code from 
GitHub:
```bash
$ git clone https://github.com/ngmarchant/dblink.git
```
Then change into the dblink directory and build the package
```bash
$ cd dblink
$ sbt assembly
```
This should produce a fat JAR at `./target/scala-2.11/dblink-assembly-0.1.jar`.

_Note: [IntelliJ IDEA](https://www.jetbrains.com/idea/) can also be used to 
build the fat JAR. It is arguably more user-friendly as it has a GUI and 
users can avoid installing sbt._

## 4. Run dblink
Everything should now be in place to run dblink. As a test, you can try running 
the example provided with the source code for the RLdata500 data set.
Within the dblink directory run the following command:

```bash
$ $SPARK_HOME/bin/spark-submit \
     --master "local[1]" \
     --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
     --conf "spark.driver.extraClassPath=./target/scala-2.11/dblink-assembly-0.1.jar" \
     ./target/scala-2.11/dblink-assembly-0.1.jar \
     ./examples/RLdata500.conf
```
This will run Spark locally in pseudocluster mode with 1 core. You can increase 
the number of cores available by changing `local[1]` to `local[n]` where `n` 
is the number of cores or `local[*]` to use all available cores.
To run dblink on other data sets you will need to edit the config file (called 
`RLdata500.conf` above).
Instructions for doing this are provided [here](configuration.md).

## 5. Results of dblink

The results from the run of dblink can be found by changing into the `examples/RLdata500_results/` directory. 

Here's a brief description of the files:

1. `run.txt`: contains details about the job (MCMC run). This includes the data files, the attributes used, parameter settings etc.
2. `partitions-state.parquet` and `driver-state`: stores the final state of the Markov chain, so that MCMC can be resumed (e.g. you can run the chain for longer without starting from scratch).
3. `diagnostics.csv` contains summary statistics along the chain which can be used to assess convergence/mixing.
4. `linkage-chain.parquet` contains posterior samples of the linkage structure in Parquet format.

Optional files:

1. `evaluation-results.txt`: contains output from an "evaluate" step (e.g. precision, recall, other measures). Requires ground truth entity identifiers in the data files.
2. `cluster-size-distribution.csv` contains the cluster size distribution along the chain (rows are iterations, columns contain counts for each cluster/entity size.  Only appears if requested in a "summarize" step.
3.`partition-sizes.csv` contains the partition sizes along the chain (rows are iterations, columns are counts of the number of entities residing in each partition). Only appears if requested in a  "summarize" step.


## Updating dblink
If you've already installed dblink and would like to upgrade to the latest 
development version, you should only need to repeat step 3 before continuing 
on to step 4.

You can either remove the `dblink` directory and clone the repository again. 
Or you can pull the latest version by running
```bash
$ git pull origin master
```
before running the `sbt assembly` command to build the fat JAR.

