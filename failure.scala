package com.mozilla.telemetry.views

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

case class Base (
    client_id: String
)

case class Target (
    client_id: String
)

object FailingView {
  val sparkConf = new SparkConf().setAppName(this.getClass.getName)
  sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
  val sc = new SparkContext(sparkConf)

  val hiveContext = new HiveContext(sc)
  import hiveContext.implicits._

  def generateCrossSectional(base: Base) = {
    Target(base.client_id)
  }

  def main(args: Array[String]): Unit = {
    val ds = hiveContext
      .sql("SELECT * FROM longitudinal")
      .selectExpr("client_id")
      .as[Base]
    val output = ds.map(generateCrossSectional)

    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
