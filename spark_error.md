title: Strange Spark Error
date: 2016-08-26
tags: spark, scala
category:mozilla

[TOC]

# Introduction
I spend the better part of last week debugging a Spark error, so I figure it's worth writing up.

# The Bug
I added the [this very simple view](https://github.com/harterrt/spark-failure/blob/master/failure.scala) to our [batch views repository](https://github.com/mozilla/telemetry-batch-view/tree/master/src/main/scala/com/mozilla/telemetry/views).

```scala
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
  def generateCrossSectional(base: Base) = {
    Target(base.client_id)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val ds = hiveContext
      .sql("SELECT * FROM longitudinal")
      .selectExpr("client_id")
      .as[Base]
    val output = ds.map(generateCrossSectional)

    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
```

When I run this on an ATMO cluster, I observe the following error:

```
Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 24 in stage 1.0 failed 4 times, most recent failure: Lost task 24.3 in stage 1.0 (TID 64, ip-172-31-8-250.us-west-2.compute.internal): java.io.IOException: org.apache.spark.SparkException: Failed to get broadcast_2_piece0 of broadcast_2
    at org.apache.spark.util.Utils$.tryOrIOException(Utils.scala:1222)
    at org.apache.spark.broadcast.TorrentBroadcast.readBroadcastBlock(TorrentBroadcast.scala:165)
    at org.apache.spark.broadcast.TorrentBroadcast._value$lzycompute(TorrentBroadcast.scala:64)
    at org.apache.spark.broadcast.TorrentBroadcast._value(TorrentBroadcast.scala:64)
    at org.apache.spark.broadcast.TorrentBroadcast.getValue(TorrentBroadcast.scala:88)
    at org.apache.spark.broadcast.Broadcast.value(Broadcast.scala:70)
    at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:65)
    at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:41)
    at org.apache.spark.scheduler.Task.run(Task.scala:89)
    at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:214)
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
    at java.lang.Thread.run(Thread.java:745)
Caused by: org.apache.spark.SparkException: Failed to get broadcast_2_piece0 of broadcast_2
    at org.apache.spark.broadcast.TorrentBroadcast$$anonfun$org$apache$spark$broadcast$TorrentBroadcast$$readBlocks$1$$anonfun$2.apply(TorrentBroadcast.scala:138)
    at org.apache.spark.broadcast.TorrentBroadcast$$anonfun$org$apache$spark$broadcast$TorrentBroadcast$$readBlocks$1$$anonfun$2.apply(TorrentBroadcast.scala:138)
...
```

I don't get much information from this message, and searching for this error yields a variety of threads with half solved solutions.
My hunch is this message pops up for a variety of issues.
What's even more strange is that this function runs successfully when we read local data using a SQLContext.

# Fixes?

I dug in for a while and I found two possible solutions.

Instead of calling generateCrossSectional, we can just inline the meat of the function and everything works.
This isn't a great solution, because this function is going to grow over the next month and I don't want to maintain the behemoth. 

After a few refactors, I found that the function will run if I change the scope of the HiveContext val.
Take a look at [this solution](https://github.com/harterrt/spark-failure/blob/master/fixed.scala), which successfully runs.

```scala
package com.mozilla.telemetry.views

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

case class Base (
    client_id: String
)

case class Target (
    client_id: String
)

object PassingView {
  def generateCrossSectional(base: Base) = {
    Target(base.client_id)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val ds = hiveContext
      .sql("SELECT * FROM longitudinal")
      .selectExpr("client_id")
      .as[Base]
    val output = ds.map(generateCrossSectional)

    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
```

# Conclusion
This solution isn't totally gratifying since I'm still unclear on what's causing the error, but I'm stopping here.
The cluster this was tested on is still running Spark 1.6, which apparently has some known issues.
Once we upgrade to 2.0 I may take another look.
