package com.weibo.etl


import org.apache.spark.sql.functions.{col, date_format, hour, to_timestamp, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.weibo.utils.hiveUtils.{HiveTableExists, ReadHiveUtils}
import org.apache.spark.sql.streaming.Trigger
import com.weibo.utils.mysqlUtils.WriteMysql
import com.weibo.utils.Api.CallBatchApi
import com.weibo.utils.CheckpointUtils

/**
 * @author Xbx
 * @date 2026/5/7 22:39
 */
/*
* 对数据进行分类 边分边写入hive
* */

object Classify {

  // 情感分析，每次去hive读取所有 label 为null的数据 调用接口分析情感
  def incrementalSentimentAnalysis(spark: SparkSession): Unit = {
    import spark.implicits._
    import scala.collection.JavaConverters._

    println("正在执行：增量情感分析")

    // 开启动态分区
    spark.sql("SET hive.exec.dynamic.partition = true")
    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

    // 读取 原始微博表（自带去重）
    val rawDF = ReadHiveUtils.readHiveclassByDayAndHour(spark, "weibo", "classbydayandhour")

    // 读取已预测表
    val predDF = spark.table("weibo.predstatistic").select("id")

    // 增量过滤：left_anti join  只查没预测过的数据
    val needDF = rawDF
      .join(predDF, Seq("id"), "left_anti")
      .filter(col("text").isNotNull && col("text") =!= "")

    val count = needDF.count()
    println(s"待预测数据：$count 条")
    // 如果待测数据小于 50条等下一批
    if (count < 50){
      println("待测数据过少，等待下一批")
      return
    }

    if (count == 0) {
      println("暂无新数据需要预测")
      return
    }

    // 批量调用接口
//    val rows = needDF.collectAsList().asScala.toList
//    val texts = rows.map(_.getAs[String]("text"))
//
//    // 调用 BatchApi 工具类
//    val resultMap = CallBatchApi.batchSentimentPredict(texts)


    val rows = needDF.collectAsList().asScala.toList
    val texts = rows.map(_.getAs[String]("text"))
    val resultMap = CallBatchApi.batchSentimentPredict(texts)

    // 【核心修改】拼接结果：把 user_id 和 screen_name 统统带进去
    val writeDF = rows.map { row =>
      val text = row.getAs[String]("text")
      val (label, labelId, score, confidence) = resultMap(text)

      (
        row.getAs[String]("id"),
        row.getAs[String]("bid"),              // 新增带入
        row.getAs[String]("user_id"),          // 核心修复：带入用户ID
        row.getAs[String]("screen_name"),      // 核心修复：带入用户昵称
        text,
        row.getAs[String]("topics"),
        row.getAs[Int]("reposts_count"),
        row.getAs[Int]("comments_count"),
        row.getAs[Int]("attitudes_count"),
        row.getAs[java.sql.Timestamp]("created_at"),
        row.getAs[String]("location"),
        row.getAs[String]("user_authentication"),
        row.getAs[Int]("hour"),
        row.getAs[String]("time_period"),
        label,
        labelId,
        score,
        confidence,
        row.getAs[String]("dt")
      )
    }.toDF(
      "id", "bid", "user_id", "screen_name", "text", "topics",
      "reposts_count", "comments_count", "attitudes_count",
      "created_at", "location", "user_authentication",
      "hour", "time_period",
      "label", "label_id", "score", "confidence", "dt"
    )

    // 写入全新的预测结果表
    writeDF.write
      .mode("append")
      .insertInto("weibo.predstatistic")

    println(s"全新情感预测数据流写入完成！共写入 $count 条")

    // 后续我们再将统计结果写入 mysql

  }

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

          println("Hive写入成功")
          ()
        } catch {
          case e: Exception => println("写入hive失败")
            e.printStackTrace()
        }
      }
      .option("checkpointLocation", "./checkpoint/weibo")
      .trigger(Trigger.ProcessingTime("200 seconds"))
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

    // 情感统计
    println("情感统计")
    // 读取weibo.predstatistic 表
    val isExistPre = HiveTableExists.tableExists(spark, "weibo", "predstatistic")
    if (!isExistPre) {
      println("表不存在")
      return
    }
    val preData = ReadHiveUtils.readHiveclassByDayAndHour(spark, "weibo", "predstatistic")
    if (preData.isEmpty){
      println("预测表为空")
      return
    }
    // 情感总体统计  情感类型分布（正面/负面/中性）
    //select label, label_id, count(*) as cnt
    //from weibo.predstatistic
    //group by label, label_id

    val labelCount = preData
      .filter(col("label").isin("正面", "负面", "中性")) // 过滤非法值
      .groupBy("label")
      .count()
      .withColumnRenamed("count", "value")
      .withColumn("create_time", lit(nowStr)) // 加统计时间

    //时段 + 情感联动统计
    //select time_period, label, count(*) as cnt
    //from weibo.predstatistic
    //group by time_period, label
    val labelCountByTime = preData
      .groupBy("time_period", "label")
      .count()
      .withColumnRenamed("count", "value")
      .withColumn("create_time", lit(nowStr)) // 加统计时间


    // 用户类型（认证）与情感关系
    //-- 不同认证用户的情感倾向
    //select user_authentication, label, count(*) as cnt
    //from weibo.predstatistic
    //group by user_authentication, label
    val labelCountByUser = preData
      .groupBy("user_authentication", "label")
      .count()
      .withColumnRenamed("count", "value")
      .withColumn("create_time", lit(nowStr)) // 加统计时间
    labelCountByUser.show()

    //帖子热度（转评赞）与情感关联
    //-- 不同情感的平均转发/评论/点赞
    //select label,
    //       avg(reposts_count) as avg_repost,
    //       avg(comments_count) as avg_comment,
    //       avg(attitudes_count) as avg_attitude
    //from weibo.predstatistic
    //group by label
    val interactionCountByLabel = preData
      .groupBy("label")
      .agg(
        // 保留2位小数
        round(avg("reposts_count"), 2).alias("avg_repost"),
        round(avg("comments_count"), 2).alias("avg_comment"),
        round(avg("attitudes_count"), 2).alias("avg_attitude")
      )
      .withColumn("create_time", lit(nowStr)) // 加统计时间

    interactionCountByLabel.show()


    // 地区综合情感热度（平均分 score）
    // 每个地区一个综合情感分数
    // 同一个地区的所有微博的 score 加起来求平均值
    // 从 -1 - 1   越大越好
    val regionHeatStatistic = preData
      // 过滤无效数据
      .filter(col("location").isNotNull && col("location") =!= "")
      .filter(col("score").isNotNull)
      // 拆分多地区：例如 "北京|上海"  拆成两行
      .select(
        explode(split(col("location"), " \\| ")).as("region"),
        col("score")
      )
      // 按地区分组 计算综合平均分+ 帖子数量
      .groupBy("region")
      .agg(
        avg("score").as("avg_score"),        // 综合情感分数
        count("*").as("count")              // 帖子数量
      )
      // 过滤掉数据太少的冷门地区，图表更干净
      .filter(col("count") >= 5)
      // 分数从高到低排序
      .orderBy(desc("avg_score"))
      // 添加统计时间
      .withColumn("create_time", lit(nowStr))

    // 每日情感趋势 占比统计 无偏差
    //  先算每日各类情感数量
    val dayLabelCount = preData
      .filter(col("label").isNotNull && col("label") =!= "")
      .groupBy("dt", "label")
      .count()
      .withColumnRenamed("count", "value")

    //  再算每日总数量
    val dayTotalCount = dayLabelCount
      .groupBy("dt")
      .agg(sum("value").as("total"))

    //  关联 计算占比
    val dayLabelRatio = dayLabelCount
      .join(dayTotalCount, "dt")
      .withColumn("ratio", round(col("value") / col("total") * 100, 2)) // 占比 %
      .withColumn("create_time", lit(nowStr))
      .orderBy("dt")

    // 展示


    // 模型稳定性分析 模型置信度统计（模型效果展示）
    //-- 模型预测置信度区间分布
    //select case
    //    when confidence >= 0.9 then '高置信'
    //    when confidence >= 0.7 then '中置信'
    //    else '低置信'
    //end as level, count(*) as cnt
    //from weibo.predstatistic
    //group by level
    val confidenceLevelCount = preData
      .filter(col("confidence").isNotNull)
      .withColumn("level",
        when(col("confidence") >= 0.9, "高置信")
          .when(col("confidence") >= 0.7, "中置信")
          .otherwise("低置信")
      )
      .groupBy("level")
      .count()
      .withColumnRenamed("count", "value")
      .withColumn("create_time", lit(nowStr))

    // =========================================================================
    //                    核心构建：独立用户画像（User Profile）模块
    // =========================================================================
    println("正在构建用户画像核心指标，并写入 MySQL...")
    import org.apache.spark.sql.expressions.Window

    // 共用的用户窗口公用器，用于求各类 TOP 极值
    val userWindow = Window.partitionBy("user_id")

    // ------------------ 1. 用户时间行为与情绪波动画像表 ------------------
    // 计算时段基础统计
    val userPeriodStats = preData
      .groupBy("user_id", "time_period")
      .agg(
        count("*").as("period_post_cnt"),
        avg("score").as("period_avg_score")
      )

    // 找出最常发帖时段
    val preferPeriodDF = userPeriodStats
      .withColumn("rank", row_number().over(userWindow.orderBy(desc("period_post_cnt"))))
      .filter(col("rank") === 1)
      .select(col("user_id"), col("time_period").as("prefer_time_period"))

    // 找出情绪最负面的时段 (score 越小越负面)
    val emoPeriodDF = userPeriodStats
      .withColumn("rank", row_number().over(userWindow.orderBy(asc("period_avg_score"))))
      .filter(col("rank") === 1)
      .select(col("user_id"), col("time_period").as("emo_time_period"))

    // 找出情绪最正面的时段 (score 越大越正面)
    val highPeriodDF = userPeriodStats
      .withColumn("rank", row_number().over(userWindow.orderBy(desc("period_avg_score"))))
      .filter(col("rank") === 1)
      .select(col("user_id"), col("time_period").as("high_time_period"))

    // 找出最常发帖的小时 (0-23)
    val preferHourDF = preData
      .groupBy("user_id", "hour")
      .count()
      .withColumn("rank", row_number().over(userWindow.orderBy(desc("count"))))
      .filter(col("rank") === 1)
      .select(col("user_id"), col("hour").as("prefer_hour"))

    // 统计全局指标：波动标准差、最新昵称、深夜发帖比例
    val userGlobalTimeStats = preData
      .groupBy("user_id")
      .agg(
        round(stddev_samp("score"), 4).as("hourly_sentiment_stddev"),
        first("screen_name", ignoreNulls = true).as("screen_name"),
        round(
          sum(when(col("hour") >= 23 || col("hour") < 4, 1).otherwise(0)) / count("*"),
          4
        ).as("night_post_ratio")
      )

    // 最终融合成时间情绪画像大表
    val userTimeProfileDF = userGlobalTimeStats
      .join(preferPeriodDF, Seq("user_id"), "left")
      .join(preferHourDF, Seq("user_id"), "left")
      .join(emoPeriodDF, Seq("user_id"), "left")
      .join(highPeriodDF, Seq("user_id"), "left")
      .withColumn("create_time", lit(nowStr))
      .na.fill(0.0, Seq("hourly_sentiment_stddev")) // 防止单条发帖算不出标准差报 NaN

    // 写入 MySQL
    WriteMysql.writeTable(userTimeProfileDF, "user_time_sentiment_profile")


    // ------------------ 2. 用户地域关注与情绪投射画像表 ------------------
    // 多标签炸开：处理一发多地的情况
    val explodedUserRegionDF = preData
      .filter(col("location").isNotNull && col("location") =!= "")
      .select(
        col("user_id"),
        col("score"),
        explode(split(col("location"), " \\| ")).as("region")
      )
      .filter(col("region").isNotNull && col("region") =!= "")

    // 按 用户+地域 聚合
    val userRegionGroupStats = explodedUserRegionDF
      .groupBy("user_id", "region")
      .agg(
        count("*").as("region_focus_count"),
        round(avg("score"), 4).as("top_region_avg_score")
      )

    // 筛选出最关注的 Top1 地域
    val topRegionDF = userRegionGroupStats
      .withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(desc("region_focus_count"))))
      .filter(col("rank") === 1)
      .select(
        col("user_id"),
        col("region").as("top_focus_region"),
        col("region_focus_count"),
        col("top_region_avg_score")
      )

    // 计算地域关注的去重总数（离散度）
    val regionDiversityDF = explodedUserRegionDF
      .groupBy("user_id")
      .agg(countDistinct("region").as("region_diversity_score"))

    // 融合成地域画像表
    val userRegionProfileDF = topRegionDF
      .join(regionDiversityDF, Seq("user_id"), "left")
      .withColumn("create_time", lit(nowStr))

    // 写入 MySQL
    WriteMysql.writeTable(userRegionProfileDF, "user_region_sentiment_profile")


    // ------------------ 3. 用户身份特征与传播情绪特质画像表 ------------------
    val userAuthProfileDF = preData
      .groupBy("user_id")
      .agg(
        first("user_authentication", ignoreNulls = true).as("user_authentication"),
        // 正面平均互动量
        round(
          sum(when(col("label") === "正面", col("reposts_count") + col("comments_count") + col("attitudes_count")).otherwise(0)) /
            sum(when(col("label") === "正面", 1).otherwise(lit(1))), 2
        ).as("pos_interaction_avg"),
        // 负面平均互动量
        round(
          sum(when(col("label") === "负面", col("reposts_count") + col("comments_count") + col("attitudes_count")).otherwise(0)) /
            sum(when(col("label") === "负面", 1).otherwise(lit(1))), 2
        ).as("neg_interaction_avg"),
        round(avg("confidence"), 4).as("avg_model_confidence")
      )
      // 计算情绪杠杆率
      .withColumn("sentiment_leverage",
        round(col("neg_interaction_avg") / when(col("pos_interaction_avg") === 0, 1).otherwise(col("pos_interaction_avg")), 2)
      )
      .withColumn("create_time", lit(nowStr))

    // 写入 MySQL
    WriteMysql.writeTable(userAuthProfileDF, "user_auth_sentiment_profile")
    println("用户画像核心指标全部顺利写入 MySQL！")
    WriteMysql.writeTable(userCount, "user_post_count")
    WriteMysql.writeTable(timePeriodCount, "time_period_count")
    WriteMysql.writeTable(timeHourCount, "hour_post_count")
    WriteMysql.writeTable(timeSeriesCount, "day_post_count")
    WriteMysql.writeTable(timePeriodUserCount, "time_period_user_count")
    WriteMysql.writeTable(regionHot, "regionHot")

    // 情感数据
    WriteMysql.writeTable(labelCount, "label_count")
    WriteMysql.writeTable(labelCountByTime, "label_count_by_time")
    WriteMysql.writeTable(labelCountByUser, "user_auth_label_count")
    WriteMysql.writeTable(interactionCountByLabel, "interaction_count_by_label")
    WriteMysql.writeTable(regionHeatStatistic, "region_heat_statistic")
    WriteMysql.writeTable(dayLabelRatio, "day_label_ratio")
    WriteMysql.writeTable(confidenceLevelCount, "confidence_level_count")
    println("Mysql 写入完成")
  }
}
