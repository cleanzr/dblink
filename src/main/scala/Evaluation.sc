import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf()
  .setMaster("local[*]")
  .setAppName("EBER evaluation")

val sc = SparkContext.getOrCreate(conf)
sc.setLogLevel("WARN")

RLdata10000.evaluate()