package com.weibo.etl


import org.apache.spark.sql.functions.{col, date_format, explode, hour, lit, to_date, to_timestamp, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.weibo.utils.hiveUtils.{HiveTableExists, ReadHiveUtils}
import org.apache.spark.sql.streaming.Trigger
import com.weibo.utils.mysqlUtils.WriteMysql
import spire.random.Random.int

import scala.collection.mutable.ListBuffer

/**
 * @author Xbx
 * @date 2026/5/7 22:39
 */
/*
* 对数据进行分类 边分边写入hive
* */

object Classify {

  // 情感分析，每次去hive读取所有 label 为null的数据 调用接口分析情感

  // 计算数据写入hive
  def classByDayAndHour(spark: SparkSession, data: DataFrame): Unit = {
    println("进入方法classByDayAndHour")
    data.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"batchId = $batchId, count = ${batchDF.count()}")
        val df2 = batchDF
          .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm"))
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
          // 转换评论 转发 点赞为整数
          .withColumn("reposts_count", col("reposts_count").cast("int"))
          .withColumn("comments_count", col("comments_count").cast("int"))
          .withColumn("attitudes_count", col("attitudes_count").cast("int"))
        val finalDF = df2.select(
          "id","bid","user_id","screen_name","text","topics",
          "reposts_count","comments_count","attitudes_count",
          "created_at","location","user_authentication", "hour","time_period" ,"dt"
        )
        finalDF.show()
        // 写入hive
        try {
          println("写入hive")
          if (!HiveTableExists.tableExists(spark, "weibo", "classbydayandhour")) {
            println("表不存在")
            spark.sql(
              """
                |CREATE TABLE IF NOT EXISTS weibo.classbydayandhour (
                |   id                  STRING,
                |   bid                 STRING,
                |   user_id             STRING,
                |   screen_name         STRING,
                |   text                STRING,
                |   topics              STRING,
                |   reposts_count       INT,
                |   comments_count      INT,
                |   attitudes_count     INT,
                |   created_at          TIMESTAMP,
                |   location           STRING,
                |   user_authentication STRING,
                |   hour                INT,    -- 小时 0-23（你要的跨天统计用）
                |   time_period         STRING  -- 深夜/早间/午间/下午/晚间
                |)
                |PARTITIONED BY (dt STRING)      -- 按天分区
                |STORED AS PARQUET;
                |""".stripMargin
            )
          }
          finalDF.write.mode("append")
            .insertInto("weibo.classbydayandhour")
          spark.sql("MSCK REPAIR TABLE weibo.classbydayandhour")

//          finalDF.write
//            .mode("append")
//            .partitionBy("dt")
//            .format("hive")
//            .saveAsTable("weibo.classbydayandhour")

          println("写入成功")
          ()
        } catch {
          case e: Exception => println("写入hive失败")
            e.printStackTrace()
        }
      }
      .option("checkpointLocation", "./checkpoint/weibo")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .start()
  }

  // 从hive读取数据聚合后（要去重聚合） 写入集群的mysql
  def classByIndicator(spark: SparkSession): Unit = {
    // data 是流数据
    println("进入方法classByIndicator")
    val lit = org.apache.spark.sql.functions.lit _
    // 判断表是否存在
    val isExist = HiveTableExists.tableExists(spark, "weibo", "classbydayandhour")
    if (!isExist) {
      println("表不存在")
      return
    }
    val data = ReadHiveUtils.readHiveclassByDayAndHour(spark, "weibo", "classbydayandhour")

    // 获取当前时间
    val now = java.time.LocalDateTime.now()
    val nowStr = now.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    // 统计各类用户的数量发帖 根据用户认证user_authentication
    val userCount = data.groupBy("user_authentication").count()
      .withColumn("create_time", lit(nowStr))
    // 统计不同时间段的活跃度 time_period 午夜 ...
    val timePeriodCount = data.groupBy("time_period").count()
      .withColumn("create_time", lit(nowStr))
    // 统计不同天的相同时段的活跃度 hour 23 0 10 ...
    val timeHourCount = data.groupBy("hour").count().orderBy("hour")
      // 添加时间字段
      .withColumn("create_time", lit(nowStr)) // 添加时间字段
    // 统计时序数据 dt
    val timeSeriesCount = data.groupBy("dt").count().orderBy("dt")
      .withColumn("create_time", lit(nowStr))
    // 统计各个时段各类型用户的发帖数
    val timePeriodUserCount = data.groupBy("time_period",
      "user_authentication").count()
      .withColumn("create_time", lit(nowStr))
    // 统计各个地区的数量 间接得到热度
    import org.apache.spark.sql.functions._

    // 统计地区热度（拆分 | 分隔的多地区 → 单独统计）
    val regionCount = data
      .filter(col("location").isNotNull && col("location") =!= "")
      .select(explode(split(col("location"), " \\| ")).as("region"))
      .filter(col("region").isNotNull && col("region") =!= "")
      .groupBy("region")
      .count()
      .orderBy(desc("count"))

    // 对regionCount的每个数值做
    // 计算最大、最小值
    val minMax = regionCount.agg(min("count").as("min"), max("count").as("max")).head()
    val minCount = minMax.getLong(0)
    val maxCount = minMax.getLong(1)
    // 3. 把 count 归一化到 0~100 热度
    val regionHot = regionCount
      .withColumn(
        "hot",
        round(
          (col("count") - minCount) / (maxCount - minCount) * 100,
          2
        )
      )
      .orderBy(desc("hot"))
      .withColumn("create_time", lit(nowStr))

    regionHot.show()
    regionCount.show()
    userCount.show()
    timePeriodCount.show()
    timeHourCount.show()
    timeSeriesCount.show()
    WriteMysql.writeTable(userCount, "user_post_count")
    WriteMysql.writeTable(timePeriodCount, "time_period_count")
    WriteMysql.writeTable(timeHourCount, "hour_post_count")
    WriteMysql.writeTable(timeSeriesCount, "day_post_count")
    WriteMysql.writeTable(timePeriodUserCount, "time_period_user_count")
    WriteMysql.writeTable(regionHot, "regionHot")
  }
}
