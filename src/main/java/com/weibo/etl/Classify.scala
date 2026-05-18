package com.weibo.etl


import org.apache.spark.sql.functions.{col, date_format, hour, to_date, to_timestamp, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.weibo.utils.hiveUtils.{HiveTableExists, ReadHiveUtils}
import org.apache.spark.sql.streaming.Trigger
import com.weibo.utils.mysqlUtils.WriteMysql
import scala.collection.mutable.ListBuffer
/**
 * @author Xbx
 * @date 2026/5/7 22:39
 */
/*
* 对数据进行分类 边分边写入hive
* */

object Classify {
  // 计算数据写入hive
  def classByDayAndHour(spark:SparkSession, data:DataFrame):Unit = {
    println("进入方法classByDayAndHour")
    data.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"batchId = $batchId, count = ${batchDF.count()}")
        val df2 = batchDF
          .withColumn("hour", hour(col("created_at")))
          .withColumn("time_period",
            when(col("hour") < 6, "凌晨")
              .when(col("hour") < 9, "早晨")
              .when(col("hour") < 12, "上午")
              .when(col("hour") < 18, "下午")
              .when(col("hour") < 22, "晚间")
              .otherwise("午夜")
          )
          .withColumn("dt", date_format(col("created_at"), "yyyy-MM-dd"))
          .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm"))

        df2.show()
        // 写入hive
        try {
          println("写入hive")
          if (!HiveTableExists.tableExists(spark, "weibo", "classbydayandhour")) {
            println("表不存在")
            spark.sql(
              """
                |CREATE TABLE IF NOT EXISTS classByDayAndHour (
                |id  STRING,
                |bid  STRING,
                |user_id             STRING,
                |screen_name         STRING,
                |text                STRING,
                |topics              STRING,
                |reposts_count       INT,
                |comments_count      INT,
                |attitudes_count     INT,
                |created_at          TIMESTAMP,
                |user_authentication STRING,
                |hour                INT,    -- 小时 0-23（你要的跨天统计用）
                |time_period      STRING  -- 深夜/早间/午间/下午/晚间
                |)
                |PARTITIONED BY (dt STRING)      -- 按天分区
                |STORED AS PARQUET;
                |""".stripMargin
            )
          }
          df2.write.mode("append")
            .insertInto("weibo.classbydayandhour")
          spark.sql("MSCK REPAIR TABLE weibo.classbydayandhour")
          ()
        } catch {
          case e: Exception => println("写入hive失败")
            e.printStackTrace()
        }
      }
      .option("checkpointLocation", "./checkpoint/weibo")
      .trigger(Trigger.ProcessingTime("600 seconds"))
      .start()
  }

  // 从hive读取数据聚合后（要去重聚合） 写入集群的mysql
  def classByIndicator(spark:SparkSession):Unit = {
    // data 是流数据
    println("进入方法classByIndicator")
    // 判断表是否存在
    val isExist = HiveTableExists.tableExists(spark, "weibo","classbydayandhour")
    if (!isExist) {
      println("表不存在")
      return
    }
    val data = ReadHiveUtils.readHiveclassByDayAndHour(spark, "weibo","classbydayandhour")

          // 统计各类用户的数量发帖 根据用户认证user_authentication
          val userCount = data.groupBy("user_authentication").count()
          // 统计不同时间段的活跃度 time_period 午夜 ...
          val timePeriodCount = data.groupBy("time_period").count()
          // 统计不同天的相同时段的活跃度 hour 23 0 10 ...
          val timeHourCount = data.groupBy("hour").count().orderBy("hour")
          // 统计时序数据 dt
          val timeSeriesCount = data.groupBy("dt").count().orderBy("dt")
          userCount.show()
          timePeriodCount.show()
          timeHourCount.show()
          timeSeriesCount.show()
          WriteMysql.writeTable(userCount, "user_post_count")
          WriteMysql.writeTable(timePeriodCount, "time_period_count")
          WriteMysql.writeTable(timeHourCount, "hour_post_count")
          WriteMysql.writeTable(timeSeriesCount, "day_post_count")
  }
}
