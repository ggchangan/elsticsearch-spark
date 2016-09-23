package elasticsearch

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by magneto on 16-6-29.
  */
object Upsert56 extends App{
  val objTypeId = UUID.fromString("5929ee25-3599-4f79-836f-3da80df7535a")
  val conf = new SparkConf().setAppName("read for elasticsearch").setMaster("local")
  conf.set("es.nodes", "172.24.63.33")
  val sc = new SparkContext(conf)
  //val labelRdd = sc.makeRDD(Seq("5929ee25-3599-4f79-836f-000001ad555a","703126ed-20ec-402c-b3d5-3c74e031ae2b"))
  val labelRdd = sc.parallelize(Seq(Tuple2("5929ee25-3599-4f79-836f-000001ad555a","703126ed-20ec-402c-b3d5-3c74e031ae2b")))

  //esDelete(objTypeId, sc, labelRdd)

  //def esDelete(objTypeId: UUID, sc: SparkContext, labelRdd: RDD[(UUID, UUID)]): Unit = {
    val esRdd = labelRdd.map(row => Map("id" -> row._1.toString, "xlabel" -> Set(row._2.toString)))

    val index = "mgobjectforsearch"
    val target = s"$index/" + objTypeId.toString

    val props = Map("es.write.operation" -> "upsert",
      //"es.input.json" -> "true",
      "es.mapping.id" -> "id",
      "es.update.script.lang" -> "groovy"
    )

    val up_params = "new_xlabel:xlabel"
    val up_script = "if (ctx._source.containsKey('xlabel')) {ctx._source.xlabel =ctx._source.xlabel - new_xlabel;}"
    esRdd.saveToEs(target, props + ("es.update.script.params" -> up_params) + ("es.update.script" -> up_script))

  //}
}
