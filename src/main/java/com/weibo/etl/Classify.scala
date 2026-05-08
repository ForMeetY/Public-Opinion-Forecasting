package com.weibo.etl


import org.apache.spark.sql.functions.{col, date_format, hour, to_date, to_timestamp, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.weibo.utils.hiveUtils.{HiveTableExists, ReadHiveUtils}
import org.apache.spark.sql.streaming.Trigger
/**
 * @author Xbx
 * @date 2026/5/7 22:39
 */
/*
* 对数据进行分类 边分边写入hive
* */

object Classify {

  def classByDayAndHour(spark:SparkSession, data:DataFrame):Unit = {
    //TODO 构造hour和time_period列
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
          .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"))
        print("df2是")
        df2.show()
        df2.write.mode("append").insertInto("weibo.classbydayandhour")
      }
      .option("checkpointLocation", "./checkpoint/weibo")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .start()
  }

  // TODO congHive去读批数据 统计各类指标交给Mysql
  def classByIndicator(spark:SparkSession):Unit = {
    // data 是流数据
    println("进入方法classByIndicator")
    // 判断表是否存在
    val isExist = HiveTableExists.tableExists(spark, "weibo","classbydayandhour")
    if (!isExist) {
      println("表不存在")
//      return
    }
    val data = ReadHiveUtils.readHiveclassByDayAndHour(spark, "weibo","classbydayandhour")

          // TODO 统计各类用户的数量发帖 根据用户认证user_authentication
          val userCount = data.groupBy("user_authentication").count()
          // TODO 统计不同时间段的活跃度 time_period 午夜 ...
          val timePeriodCount = data.groupBy("time_period").count()
          // TODO 统计不同天的相同时段的活跃度 hour 23 0 10 ...
          val timeHourCount = data.groupBy("hour").count()
          // TODO 统计时序数据 dt
          val timeSeriesCount = data.groupBy("dt").count()
          userCount.show()
          timePeriodCount.show()
          timeHourCount.show()
          timeSeriesCount.show()

  }
}
