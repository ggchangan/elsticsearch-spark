import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark._

/**
  * Created by magneto on 16-6-29.
  */
object Upsert extends App{
  val conf = new SparkConf().setAppName("read for elasticsearch").setMaster("local")
  conf.set("es.nodes", "172.24.63.14")
  conf.set("es.write.operation", "update")
  val up_script = "if (ctx._source.containsKey('name')) {ctx._source.name += new_name;} else {ctx._source.name = [new_name]}"
  conf.set("es.update.script", up_script)
  val up_params = "new_name:name"
  //val up_params = "{\"tag\":\"sxl\"}";
  //val up_params = "tag:xx"
  //conf.set("es.update.script.params.json", up_params)
  conf.set("es.update.script.params", up_params)
  conf.set("es.mapping.id", "id")

  val sc = new SparkContext(conf)

  //val up_all = Map("es.update.script.params" -> up_params, "es.update.script" -> up_script, "es.mapping.id" -> "id")

  //val rdd = sc.makeRDD(Seq(up_all))
  //EsSpark.saveToEs(rdd,"test/docs")

  //val lines = sc.parallelize( List("""{"id":"67861","address":{"zipcode":"25381","id":"67861"}}""")).saveToEs("test/docs", up_all)
  //sc.parallelize(Seq(Map("id"->2,"xx"->"xx"))).saveToEs("test/docs")

  val numbers = Map("id" -> 2, "name" -> "zhangren33")

  val rdd = sc.makeRDD(Seq(numbers))


  rdd.saveToEs("test/docs")
}
