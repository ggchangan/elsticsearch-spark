import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by magneto on 16-7-1.
  */
object DynamicResources extends App{
  val conf = new SparkConf().setAppName("Dynamic resourcees").setMaster("local")
  conf.set("es.nodes", "192.168.41.34")

  val sc = new SparkContext(conf)

  val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
  val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
  val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

  val rdd = sc.makeRDD(Seq(game, book, cd))

  EsSpark.saveToEs(rdd,"my-collection/{media_type}")
}
