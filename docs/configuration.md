# Configuration
For standard use cases, dblink can be launched using `spark-submit` 
(the alternative for more complex use cases is to use the Scala API). 
When launching using `spark-submit`, a dblink config file must be 
provided.
This file contains information about:
* the location of the data to be linked
* metadata
* hyperparameters for the model
* steps/tasks to run

The dblink config file must be in [HOCON](https://github.com/lightbend/config/blob/master/HOCON.md#hocon-human-optimized-config-object-notation) 
(Human-Optimized Config Object Notation) format. It is a superset of 
JSON with a more relaxed syntax. An example is provided in the 
`examples` directory.
Further details are given in the following sections of this document.
Once a config file has been written (e.g. called `my-dblink.conf`), 
dblink can be run using `spark-submit` as follows:

```
$ $SPARK_HOME/bin/spark-submit \
    --master local[1] \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=/path/to/log4j.properties" \
    --conf "spark.driver.extraClassPath=/path/to/dblink-assembly-0.1.jar" \
    /path/to/dblink-assembly-0.1.jar \
    /path/to/my-dblink.conf
```

## Main properties
The table below outlines the properties that must be specified in a 
dblink config file. Most of these properties have no defaults and must be 
specified for dblink to run successfully.

<table class="table">
<tr><th>Property Name</th><th>Description</th></tr>
<tr>
  <td><code>dblink.data.path</code></td>
  <td>
    URI where the data is located. This must point to a CSV file and the 
    first line of the file must be a header with the column names.
  </td>
</tr>
<tr>
  <td><code>dblink.data.recordIdentifier</code></td>
  <td>
    Name of the attribute/column in the data which contains unique 
    record identifiers. This must be specified.
  </td>
</tr>
<tr>
  <td><code>dblink.data.fileIdentifier</code></td>
  <td>
    Name of the attribute/column in the data which contains file 
    identifiers. This is only required if the records come from 
    different files. Defaults to <code>null</code>.
  </td>
</tr>
<tr>
  <td><code>dblink.data.entityIdentifier</code></td>
  <td>
    Name of the attribute/column in the data which contains entity 
    identifiers (ground truth). This is only required for evaluation 
    purposes. Defaults to <code>null</code>.  
  </td>
</tr>
<tr>
  <td><code>dblink.data.nullValue</code></td>
  <td>
    String used in the data to represent null values. This must be 
    specified.
  </td>
</tr>
<tr>
  <td><code>dblink.data.matchingAttributes</code></td>
  <td>
    A JSON array specifying the attributes/columns in the data to 
    use for matching, along with the corresponding similarity function 
    and distortion prior. Each entry in the array is a JSON object with 
    a <code>name</code> (attribute/column name), <code>similarityFn</code> 
    (see next section) and distortion hyperparameters 
    <code>distortionPrior.alpha</code> and 
    <code>distortionPrior.beta</code> (both must be positive numbers).
  </td>
</tr>
<tr>
  <td><code>dblink.outputPath</code></td>
  <td>
    URI to store any outputs of dblink. This may include the state of the 
    Markov chain, posterior samples and/or point estimates of the linkage 
    structure and evaluation metrics.
  </td>
</tr>
<tr>
  <td><code>dblink.checkpointPath</code></td>
  <td>
    URI where checkpoints will be stored. Checkpoints are made periodically 
    to prevent the lineage of the RDD from becoming too long. These 
    are removed automatically when dblink finishes running.
  </td>
</tr>
<tr>
  <td><code>dblink.randomSeed</code></td>
  <td>
    Random integer seed to be passed on to the pseudorandom number 
    generator.
  </td>
</tr>
<tr>
  <td><code>dblink.expectedMaxClusterSize</code></td>
  <td>
    A hint at the size of the largest clusters expected in the data. 
    This is used to optimise caching. Defaults to 10.
  </td>
</tr>
<tr>
  <td><code>dblink.partitioner</code></td>
  <td>
    Specifies the type of partitioner to use. See the next section for
    details. 
  </td>
</tr>
<tr>
  <td><code>dblink.steps</code></td>
  <td>
    A JSON array of steps to be executed (in the order given). Each 
    step is represented as a JSON object with a <code>name</code> and 
    <code>parameters</code>. Supported steps include "sample", 
    "summarize", "evaluate" and "copy-files". See the next section for 
    details.
  </td>
</tr>
</table>

## Details on selected properties
Some of the properties referenced above are not scalar-valued and must be 
specified using a JSON object. We provide further detail on these types 
of properties below.

### Similarity function
A similarity function must be specified for each attribute that appears in 
`dblink.data.matchingAttributes`. This is done using a JSON object with 
`name` and `parameters` keys. Currently only two similarity functions are 
supported: (1) a constant similarity function and (2) a similarity function 
based on normalized Levenshtein (edit) distance. Examples are provided 
for each of these below. 

#### Constant similarity
Likelihood of distorted value being selected is only based on the empirical 
frequency.
```hocon
similarityFn : {
  name : "ConstantSimilarityFn"
}
```

#### Levenshtein similarity
Likelihood of distorted value being selected is proportional to the empirical 
frequency and exponentiated Levenshtein similarity. There are two 
parameters:
* `maxSimilarity`: similarities will be in the range `[0, maxSimilarity]`.
* `threshold`: similarities below this value will be set to zero. A higher 
threshold improves the efficiency of the inference, possibly at the expense of 
accuracy.
```hocon
similarityFn : {
  name : "LevenshteinSimilarityFn"
  parameters : {
    threshold : 7.0
    maxSimilarity : 10.0
  }
}
```


### Partitioner
The partitioner is specified at `dblink.partitioner` in the config file. It 
specifies how the space of entities is partitioned. A partitioner is specified 
using the `name` and `parameters` keys. Currently only one type of partitioner 
is supported called the `KDTreePartitioner`. As the name suggests, it is based 
on a k-d tree. It has two parameters:
* `numLevels` : the depth/number of levels of the tree. The partitions are the 
leaves of the tree, hence the number of partitions is given by `2^numLevels`.
* `matchingAttributes` : splits are performed by cycling through the 
attributes specified in this array. Attributes listed here must also appear in 
`dblink.data.matchingAttributes`. An example configuration is given below.

```hocon
partitioner : {
  name : "KDTreePartitioner"
  parameters : {
    numLevels : 3
    matchingAttributes : ["name", "age", "sex"]
  }
}
```

### Steps
This part of the config file specifies the tasks/steps to be executed on the 
data and/or output. Currently four steps are supported.

#### Sample step
This step either begins sampling from a new initial state, or resumes sampling 
from a saved state. It is specified by adding the following entry to 
`dblink.steps`:
```hocon
{
  name: "sample"
  parameters : {
    sampleSize : 10
    # ...
  }
}
```

The following table outlines the available parameters.
<table class="table">
<tr><th>Parameter</th><th>Description</th></tr>
<tr>
  <td><code>sampleSize</code></td>
  <td>
    A positive integer specifying the desired number of samples (after burn-in 
    and thinning). This parameter is required.
  </td>
</tr>
<tr>
  <td><code>burninInterval</code></td>
  <td>
    A non-negative integer specifying the number of initial samples to discard 
    as burn-in. Defaults to 0, which means no burn-in is applied.
  </td>
</tr>
<tr>
  <td><code>thinningInterval</code></td>
  <td>
    A positive integer specifying the period for saving samples to disk. 
    Defaults to 1, which means no thinning is applied.
  </td>
</tr>
<tr>
  <td><code>resume</code></td>
  <td>
    Whether to continue sampling from a saved state (if one exists on disk). 
    Defaults to true. 
  </td>
</tr>
<tr>
  <td><code>sampler</code></td>
  <td>
    One of the supported samplers: "PCG-I", "PCG-II", "Gibbs" or 
    "Gibbs-Sequential". Defaults to "PCG-I".
  </td>
</tr>
</table>

#### Summarize step
This step produces summary statistics from saved posterior samples. It is 
specified by adding the following entry to `dblink.steps`:
```hocon
{
  name : "summarize"
  parameters : {
    # ...
  }
}
```

The following table outlines the available parameters.
<table class="table">
<tr><th>Parameter</th><th>Description</th></tr>
<tr>
  <td><code>lowerIterationCutoff</code></td>
  <td>
    A positive integer specifying a lower cut-off for iterations included in 
    the summary. Defaults to 0.
  </td>
</tr>
<tr>
  <td><code>quantities</code></td>
  <td>
    An array containing one or more of the supported quantities: 
    "cluster-size-distribution" or "partition-sizes". "cluster-size-distribution" 
    computes the distribution of entity cluster sizes along the chain and 
    "partition-sizes" computes the number of entities in each partition along 
    the chain.
  </td>
</tr>
</table>

#### Evaluate step
This step computes evaluation metrics using the provided ground truth. 
It is specified by adding the following entry to `dblink.steps`:
```hocon
{
  name : "evaluate"
  parameters : {
    metrics : ["pairwise"]
    # ...
  }
}
```

The following table outlines the available parameters.
<table class="table">
<tr><th>Parameter</th><th>Description</th></tr>
<tr>
  <td><code>lowerIterationCutoff</code></td>
  <td>
    A positive integer specifying a lower cut-off for iterations included 
    in the final sample. Defaults to 0.
  </td>
</tr>
<tr>
  <td><code>metrics</code></td>
  <td>
    An array containing one or more of the supported metrics: 
    "pairwise" or "cluster". Currently "pairwise" computes the pairwise 
    precision and recall and "cluster" computes the adjusted Rand index.
  </td>
</tr>
<tr>
  <td><code>useExistingSMPC</code></td>
  <td>
    If true, try to use a saved point estimate of the linkage structure 
    (based on the SMPC method) to compute the metrics. Otherwise, compute 
    the SMPC point estimate from scratch. Defaults to false.
  </td>
</tr>
</table>

#### Copy-files step
This step allows files to be copied from project directory to another destination. 
This is useful when running dblink on a cluster where the local or Hadoop file 
system is ephemeral. This step is specified by adding the following entry to 
`dblink.steps`:
```hocon
{
  name : "copy-files"
  parameters : {
    fileNames : ["cluster-size-distribution.csv", "partition-sizes.csv", "diagnostics.csv", "shared-most-probable-clusters.csv", "run.txt", "evaluation-results.txt"]
    destinationPath : "S3:///bucket-name/"
    # ...
  }
}
```

The following table outlines the available parameters.
<table class="table">
<tr><th>Parameter</th><th>Description</th></tr>
<tr>
  <td><code>fileNames</code></td>
  <td>
    An array containing the file names in the project directory to copy.
  </td>
</tr>
<tr>
  <td><code>destinationPath</code></td>
  <td>
    URI specifying destination for files.
  </td>
</tr>
<tr>
  <td><code>overwrite</code></td>
  <td>
    Whether to overwrite files at the destination path. Defaults to false.
  </td>
</tr>
<tr>
  <td><code>deleteSource</code></td>
  <td>
    Whether to delete the source files. Defaults to false.
  </td>
</tr>
</table>
