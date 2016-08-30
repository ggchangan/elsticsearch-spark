import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by magneto on 16-7-1.
  */
object MetadataSimple extends App{
  val conf = new SparkConf().setAppName("metadata for elasticsearch").setMaster("local")
  conf.set("es.nodes", "192.168.41.34")

  val sc = new SparkContext(conf)

  val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
  val muc = Map("iata" -> "MUC", "name" -> "Munich")
  val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

  val pairRdd = sc.makeRDD(Seq((1->otp),(2->muc),(3->sfo)))
  Seq((1->otp),(2->muc),(3->sfo)).foreach(print)

  EsSpark.saveToEsWithMeta(pairRdd, "airports/2015")
}
