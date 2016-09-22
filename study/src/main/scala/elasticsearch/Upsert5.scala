package elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by magneto on 16-6-29.
  */
object Upsert5 extends App{
  val index = "test"
  val target = s"$index/docs"

  val props = Map("es.write.operation" -> "upsert",
    //"es.input.json" -> "true",
    "es.mapping.id" -> "id",
    "es.update.script.lang" -> "groovy"
  )

  val conf = new SparkConf().setAppName("read for elasticsearch").setMaster("local")
  conf.set("es.nodes", "172.24.63.14")

  val sc = new SparkContext(conf)
  //val name= Map("id" -> 2, "name" -> Set("sxl1989","sxl9199"))
  val name= Map("id" -> 5, "name" -> Set("zhangren"))
  val lines = sc.makeRDD(Seq(name))
  val up_params = "new_name:name"
  //val up_script =  "if (ctx._source.containsKey(\"name\")) {ctx._source.name += new_name;} else {ctx._source.name = [new_name];}"
  val up_script =  "if (ctx._source.containsKey('name')) {if(ctx._source.name instanceof List){ctx._source.name = ctx._source.name - new_name;} else {ctx._source.name = [ctx._source.name] - new_name}} else {ctx._source.name = new_name;}"
  lines.saveToEs(target, props + ("es.update.script.params" -> up_params) + ("es.update.script" -> up_script))
}
