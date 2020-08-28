package com.aliyun.emr.examples.streaming


import com.aliyun.datahub.model.RecordEntry
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.aliyun.datahub.DatahubUtils
import org.apache.spark.streaming.dstream.DStream

object TestDatahub {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      // scalastyle:off
      System.err.println(
        """Usage: TestDatahub <project> <topic> <subscribe Id> <access key id>
          |         <access key secret> <endpoint> <batch interval seconds> [<shard Id>]
        """.stripMargin)
      // scalastyle:on
      System.exit(1)
    }

    var isShardDefined = false
    if (args.length == 8) {
      isShardDefined = true
    }

    val project = args(0)
    val topic = args(1)
    val subId = args(2)
    val accessKeyId = args(3)
    val accessKeySecret = args(4)
    val endpoint = args(5)
    val batchInterval = Milliseconds(args(6).toInt * 1000)

    println(s"======= project:$project,topic:$topic,subscribe Id:$subId,access key id:$accessKeyId,endpoint:$endpoint,batchInterval:$batchInterval")

    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf().setAppName("Test Datahub")
      val ssc = new StreamingContext(conf, batchInterval)
      var datahubStream: DStream[Array[Byte]] = null
      if (isShardDefined) {
        val shardId = args(7)
        datahubStream = DatahubUtils.createStream(
          ssc,
          project,
          topic,
          subId,
          accessKeyId,
          accessKeySecret,
          endpoint,
          shardId,
          read(_),
          StorageLevel.MEMORY_AND_DISK)
      } else {
        datahubStream = DatahubUtils.createStream(
          ssc,
          project,
          topic,
          subId,
          accessKeyId,
          accessKeySecret,
          endpoint,
          read(_),
          StorageLevel.MEMORY_AND_DISK)
      }

      // scalastyle:off
      datahubStream.checkpoint(batchInterval * 2)
        .foreachRDD(rdd => println("======= batch count:" + rdd.count()))
      // scalastyle:on
      ssc.checkpoint("hdfs:///tmp/spark/streaming") // set checkpoint directory
      ssc
    }

    val ssc = StreamingContext.getOrCreate("hdfs:///tmp/spark/streaming", functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()
  }

  def read(record: RecordEntry): String = {
    record.getString(0)
  }
}
