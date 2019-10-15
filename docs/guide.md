# Step-by-step guide
This guide will take you through the steps involved in running dblink on a 
small test data set. To make the guide accessible, we assume that you're 
running dblink on your local machine. Of course, in practical applications 
you'll likely want to run dblink on a cluster. We'll provide some pointers 
for this option as we go along.

## 0. Install Java
The following two steps require that Java 8+ is installed on your system. 
To check whether it is installed on a macOS or Linux system, run the command
```bash
$ java -version
```
You should see a version number of the form 8.x (or equivalently 1.8.x).
Installation instructions for Oracle JDK on Windows, macOS and Linux are 
available [here](https://java.com/en/download/help/download_options.xml).

_Note: As of April 2019, the licensing terms of the Oracle JDK have changed. 
We recommend using an open source alternative such as the OpenJDK. Packages 
are available in many Linux distributions. Instructions for macOS are 
available [here](macos-java8.md)._

## 1. Get access to a Spark cluster
Since dblink is implemented as a Spark application, you'll need access to a 
Spark cluster in order to run it.
Setting up a Spark cluster from scratch can be quite involved and is beyond 
the scope of this guide.
We refer interested readers to the Spark 
[documentation](https://spark.apache.org/docs/latest/#launching-on-a-cluster), 
which discusses various deployment options.
An easier route for most users is to use a preconfigured Spark cluster 
available through public cloud providers, such as 
[Amazon EMR](https://aws.amazon.com/emr/), 
[Azure HDInsight](https://azure.microsoft.com/en-us/services/hdinsight/), 
and [Google Cloud Dataproc](https://cloud.google.com/dataproc/).
In this guide, we take an even simpler approach: we'll run Spark in 
_pseudocluster mode_ on your local machine.
This is fine for testing purposes or for small data sets.

We'll now take you through detailed instructions for setting up Spark in 
pseudocluster mode on a macOS or Linux system.

First, download the prebuilt 2.3.1 release from the Spark
[release archive](https://archive.apache.org/dist/spark/).
```bash
$ wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
```
then extract the archive.
```bash
$ tar -xvf spark-2.3.1-bin-hadoop2.7.tgz
```

Move the Spark folder to `/opt` and create a symbolic link so that you can 
easily switch to another version in the future.
```bash
$ sudo mv spark-2.3.1-bin-hadoop2.7 /opt
$ sudo ln -s /opt/spark-2.3.1-bin-hadoop2.7/ /opt/spark
```

Define the `SPARK_HOME` variable and add the Spark binaries to your `PATH`. 
The way that this is done depends on your operating system and/or shell.
Assuming enviornment variables are defined in `~/.profile`, you can 
run the following commands:
```bash
$ echo 'export SPARK_HOME=/opt/spark' >> ~/.profile
$ echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.profile
```

After appending these two lines, run the following command to update your 
path for the current session. 
```bash
$ source ~/.profile 
```

Notes:
* If using Bash on Debian, Fedora or RHEL derivatives, environment 
variables are typically defined in `~/.bash_profile` rather than 
`~/.profile`
* If using ZSH, environment variables are typically defined in 
`~/.zprofile`
* You can check which shell you're using by running `echo $SHELL`

## 2. Obtain the dblink JAR file
In this step you'll obtain the dblink fat JAR, which has file name 
`dblink-assembly-0.1.jar`.
It contains all of the class files and resources for dblink, packed together 
with any dependencies.

There are two options:
* (Recommended) Download a prebuilt JAR from [here](https://github.com/ngmarchant/dblink/releases). 
This has been built against Spark 2.3.1 and is not guaranteed to work with 
other versions of Spark.
* Build the fat JAR file from source as explained in the section below.

### 2.1. Building the fat JAR
The build tool used for dblink is called sbt. You'll need to install 
sbt on your system. Instructions are available for Windows, macOS and Linux 
in the sbt. We give alternative installtion in the second set of instructions
for those using bash on MacOS. 
[documentation](https://www.scala-sbt.org/1.x/docs/Setup.html)

On macOS or Linux, you can verify that sbt is installed correctly by running.
```bash
$ sbt about
```

Once you've successfully installed sbt, get the dblink source code from 
GitHub:
```bash
$ git clone https://github.com/ngmarchant/dblink.git
```
then change into the dblink directory and build the package
```bash
$ cd dblink
$ sbt assembly
```
This should produce a fat JAR at `./target/scala-2.11/dblink-assembly-0.1.jar`.

_Note: [IntelliJ IDEA](https://www.jetbrains.com/idea/) can also be used to 
build the fat JAR. It is arguably more user-friendly as it has a GUI and 
users can avoid installing sbt._

## 3. Run dblink
Having completed the above two steps, you're now ready to launch dblink.
This is done using the [`spark-submit`](https://spark.apache.org/docs/latest/submitting-applications.html) 
interface, which supports all types of Spark deployments.

As a test, let's try running the RLdata500 example provided with the source 
code on your local machine.
From within the `dblink` directory, run the following command:
```bash
$SPARK_HOME/bin/spark-submit \
  --master "local[1]" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --conf "spark.driver.extraClassPath=./target/scala-2.11/dblink-assembly-0.1.jar" \
  ./target/scala-2.11/dblink-assembly-0.1.jar \
  ./examples/RLdata500.conf
```
This will run Spark in pseudocluster (local) mode with 1 core. You can increase 
the number of cores available by changing `local[1]` to `local[n]` where `n` 
is the number of cores or `local[*]` to use all available cores.
To run dblink on other data sets you will need to edit the config file (called 
`RLdata500.conf` above).
Instructions for doing this are provided [here](configuration.md).

## 4. Output of dblink
dblink saves output into a specified directory. In the RLdata500 example from 
above, the output is written to `./examples/RLdata500_results/`. 

Below we provide a brief description of the files:

* `run.txt`: contains details about the job (MCMC run). This includes the 
data files, the attributes used, parameter settings etc.
* `partitions-state.parquet` and `driver-state`: stores the final state of 
the Markov chain, so that MCMC can be resumed (e.g. you can run the Markov 
chain for longer without starting from scratch).
* `diagnostics.csv` contains summary statistics along the chain which can be 
used to assess convergence/mixing.
* `linkage-chain.parquet` contains posterior samples of the linkage structure 
in Parquet format.

Optional files:

* `evaluation-results.txt`: contains output from an "evaluate" step (e.g. 
precision, recall, other measures). Requires ground truth entity identifiers 
in the data files.
* `cluster-size-distribution.csv` contains the cluster size distribution 
along the chain (rows are iterations, columns contain counts for each 
cluster/entity size.  Only appears if requested in a "summarize" step.
* `partition-sizes.csv` contains the partition sizes along the chain (rows 
are iterations, columns are counts of the number of entities residing in each 
partition). Only appears if requested in a "summarize" step.
* `shared-most-probable-clusters.csv` is a point estimate of the linkage 
structure computed from the posterior samples. Each line in the file contains 
a comma-separated list of record identifiers which are assigned to the same 
cluster/entity.  
