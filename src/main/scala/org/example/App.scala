package org.example

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark


object App {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .set("es.nodes", args(0))
      .set("es.port", "9200")
      .set("es.net.http.auth.user","elastic")
      .set("es.net.http.auth.pass", "1122334")
      .set("es.nodes.wan.only", args(1))

    val esQuery =
      """
        |{
        | "query": { "match_all": {} },
        |	"size": 500
        |}
        |""".stripMargin

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .appName("spark-es-demo")
      .getOrCreate()

    val sc = spark.sparkContext

    val esRDD: RDD[(String, collection.Map[String, AnyRef])] = EsSpark.esRDD(sc, "platform_order_*/_doc", esQuery)
    println(s"读取es count: ${esRDD.count()}")

    esRDD.take(10).foreach(t => {
      println(t._1)
    })


    spark.stop()

  }
}
