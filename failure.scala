package com.harterrt.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

case class BaseClass (
    ugly_string: Long
)

case class TargetClass (
    pretty_string: Long
)

object FailingMap {
  val sparkConf = new SparkConf().setAppName(this.getClass.getName)
  sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
  val sc = new SparkContext(sparkConf)

  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  def generateTargetClass(input: BaseClass) = {
    TargetClass(input.ugly_string)
  }

  def main(args: Array[String]): Unit = {
    val ds = (1 to 1000).map(xx => new BaseClass(xx)).toDS()
    val output = ds.map(generateTargetClass)

    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
