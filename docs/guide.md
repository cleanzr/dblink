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
Define the `SPARK_HOME` variable and add the Spark binaries to your path. 
This can be done for your user account by adding two lines to 
the end of your `~/.bash_profile` or `~/.profile` file.
```bash
$ echo 'export SPARK_HOME=/opt/spark' >> ~/.bash_profile
$ echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bash_profile
```

After appending these two lines, run the following command to update your 
path for the current session. 
```bash
$ source ~/.bash_profile 
```

## 3. Obtain the dblink JAR file
In this step you'll obtain the dblink fat JAR, which will have file name 
`dblink-assembly-0.1.jar`.
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
Comment to NM: I may have an issue regarding sbt, as when I run this bash command below, I get the following 
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
Change into the `examples` directory
```bash
$ cd examples
```
and run the following command:
```bash
$ $SPARK_HOME/bin/spark-submit \
    --master local[1] \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    --conf "spark.driver.extraClassPath=./target/scala-2.11/dblink-assembly-0.1.jar" \
    ./target/scala-2.11/dblink-assembly-0.1.jar \
    RLdata500.conf
```
This will run Spark locally in pseudocluster mode with 1 core. You can increase 
the number of cores available by changing `local[1]` to `local[n]` where `n` 
is the number of cores or `local[*]` to use all available cores.
To run dblink on other data sets you will need to edit the config file (called 
`RLdata500.conf` above).
Instructions for doing this are provided [here](configuration.md).
