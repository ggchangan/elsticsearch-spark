import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by magneto on 16-7-1.
  */
object JsonSimple extends App{
  val conf = new SparkConf().setAppName("Json to elasticsearch").setMaster("local")
  conf.set("es.nodes", "192.168.41.34")

  val json1 = """{"reason" : "business", "airport" : "SFO"}"""
  val json2 = """{"participants" : 5, "airport" : "OTP"}"""

  val sc = new SparkContext(conf)

  val rdd = sc.makeRDD(Seq(json1, json2))

  EsSpark.saveJsonToEs(rdd, "spark/json-trips")
}
