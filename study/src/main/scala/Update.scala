import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable

/**
  * Created by magneto on 16-6-29.
  */
object Update extends App{
  val conf = new SparkConf().setAppName("read for elasticsearch").setMaster("local")
  conf.set("es.nodes", "192.168.41.34")
  conf.set("es.write.operation", "update")
  //conf.set("es.mapping.id", "id")

  val sc = new SparkContext(conf)

  //val queryStr = """{"query": { "match_all": {} }}""";
  //val queryStr = """{"query": { "match_all": {} }}"""
  val queryStr = """{  "query": { "query_string": { "query": "sxl" } }}"""
  //val docs = EsSpark.esRDD(sc,"airports/2015","?q=Munich")
  val docs = EsSpark.esRDD(sc,"spark/docs",queryStr)
  //val docs = EsSpark.esJsonRDD(sc,"airports/2015",queryStr)

  //println(docs)
  docs.foreach(doc => println(doc))

  /*
  var newDocs = docs.map(doc => {
    println(doc._2.get("age"))
    val age = doc._2.get("age").getOrElse("0").toString
    println(age)
    val newAge = (age.toInt + 1).toString
    val docContent2 = Map("age" -> newAge)
    doc._1 -> docContent2
  }).collect()


  newDocs.foreach(doc => println(doc))

  val rdd = sc.makeRDD(newDocs)

  EsSpark.saveToEsWithMeta(rdd, "spark/docs")
  */

  var newDocs = docs.map(doc => {
    println(doc._2.get("age"))
    val age = doc._2.get("age").getOrElse("0").toString
    println(age)
    val newAge = (age.toInt + 1).toString
    val docContent2 = Map("age" -> newAge)
    doc._1 -> docContent2
  })

  newDocs.foreach(doc => println(doc))

  //val rdd = sc.makeRDD(newDocs)

  EsSpark.saveToEsWithMeta(newDocs, "spark/docs")
}
