package elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by magneto on 16-7-1.
  */
object Query extends App{
  val conf = new SparkConf().setAppName("read for elasticsearch").setMaster("local")
  conf.set("es.nodes", "192.168.41.34")

  val sc = new SparkContext(conf)

  //val queryStr = """{"query": { "match_all": {} }}""";
  //val queryStr = """{"query": { "match_all": {} }}"""
  val queryStr = """{  "query": { "query_string": { "query": "SFO" } }}"""
  //val docs = EsSpark.esRDD(sc,"airports/2015","?q=Munich")
  val docs = EsSpark.esRDD(sc,"airports/2015",queryStr)
  //val docs = EsSpark.esJsonRDD(sc,"airports/2015",queryStr)

  println(docs)
  docs.foreach(doc => println(doc))
}
