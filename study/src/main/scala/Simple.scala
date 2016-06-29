import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by magneto on 16-6-28.
  */
object Simple extends App{
  val conf = new SparkConf().setAppName("索引到es").setMaster("local")
  conf.set("es.index.auto.create", "true")
  //conf.set("es.nodes", "192.168.40.3")
  //conf.set("es.port", "9200")

  val  sc = new SparkContext(conf)

  val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
  val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

  val rdd = sc.makeRDD(Seq(numbers, airports))

  EsSpark.saveToEs(rdd, "spark/docs")
}
