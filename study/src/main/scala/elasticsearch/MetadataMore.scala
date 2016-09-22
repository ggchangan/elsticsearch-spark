package elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.rdd.Metadata._
/**
  * Created by magneto on 16-7-1.
  */
object MetadataMore extends App{
  val conf = new SparkConf().setAppName("metadata more for elasticsearch").setMaster("local")
  conf.set("es.nodes", "192.168.41.34")

  val sc = new SparkContext(conf)

  val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
  val muc = Map("iata" -> "MUC", "name" -> "Munich")
  val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

  // metadata for each document
  // note it's not required for them to have the same structure
  val otpMeta = Map(ID -> 1, TTL -> "3h")
  val mucMeta = Map(ID -> 2, VERSION -> "23")
  val sfoMeta = Map(ID -> 3)

  val pairedRdd = sc.makeRDD(Seq((otpMeta->otp),(mucMeta->muc),(sfoMeta->sfo)))
  EsSpark.saveToEsWithMeta(pairedRdd, "airports/2015")
}
