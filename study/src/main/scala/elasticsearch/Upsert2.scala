package elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by magneto on 16-6-29.
  */
object Upsert2 extends App{
  val index = "address"
  val target = s"$index/contact"

  val props = Map("es.write.operation" -> "upsert",
    "es.input.json" -> "true",
    "es.mapping.id" -> "id",
    "es.update.script.lang" -> "groovy"
  )

  val conf = new SparkConf().setAppName("read for elasticsearch").setMaster("local")
  conf.set("es.nodes", "172.24.63.14")

  val sc = new SparkContext(conf)
  val lines = sc.makeRDD(List("""{"id":"1","address":"anhui"}"""))
  val up_params = "new_address:address"
  val up_script = "ctx._source.address+=new_address"
  lines.saveToEs(target, props + ("es.update.script.params" -> up_params) + ("es.update.script" -> up_script))
}
