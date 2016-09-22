package spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by magneto on 16-9-8.
  */
object Test extends App {
  print("hello World!")

  val conf = new SparkConf().setMaster("local")
  val sc = new SparkContext(conf)

  //val rdd = sc.makeRDD(1 to 10, 3)

  val collect = Seq((1 to 10, Seq("host1","host3")),(11 to 20, Seq("host2")))
  val rdd = sc.makeRDD(collect)
  rdd.preferredLocations(rdd.partitions(0))
  rdd.preferredLocations(rdd.partitions(1))
}
