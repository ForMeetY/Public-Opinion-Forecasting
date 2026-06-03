package com.weibo.etl.userPortrait

import com.weibo.utils.Api.CallBatchApi
import com.weibo.utils.mysqlUtils.WriteMysql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit} // 常用基础组件单独引出


/**
 * @author Xbx
 * @date 2026/5/29 21:12
 *
 */
object UserPortrait {
//
//  // 新增用户画像统计  （暂时不用）
//  def newUserColdStartProfile(spark: SparkSession, currentBatchDF: DataFrame): Unit = {
//    import spark.implicits._
//    println("开始执行：新用户画像增量冷启动对比预测...")
//
//    // 获取当前系统时间，用于标记更新时间
//    val nowStr = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
//
//    // 提取当前批次中所有出现过的去重 user_id
//    val batchUsers = currentBatchDF
//      .filter(col("user_id").isNotNull && col("user_id") =!= "")
//      .select("user_id")
//      .distinct()
//
//    // 读取 Hive 画像总表中已经存在的所有历史老用户
//    val existUsers = spark.table("weibo.user_final_gmm_profile").select("user_id")
//
//    // 使用 left_anti join 筛选出这批数据里“从未有过画像”的纯新用户 ID
//    val newUsersToPredict = batchUsers.join(existUsers, Seq("user_id"), "left_anti").cache()
//    val newUserCount = newUsersToPredict.count()
//
//    println(s"经比对，当前批次包含新发帖用户 ${batchUsers.count()} 人，其中纯新用户: $newUserCount 人")
//
//    if (newUserCount == 0) {
//      println("暂无全新用户需要做冷启动画像，流程结束。")
//      newUsersToPredict.unpersist()
//      return
//    }
//  }

  // 进行全量用户的统计
  def fullUserProfile(spark: SparkSession): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    println("开始聚合用户特征，准备全量GMM预测")

    val nowStr = java.time.LocalDateTime.now()
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    val predDF = spark.table("weibo.predstatistic").cache()


    // 聚合5大特征
    //    val feat12 = predDF
    //      .groupBy("user_id")
    //      .agg(
    //        first("screen_name", ignoreNulls = true).as("screen_name"),
    //        round(stddev_samp("score"), 4).as("hourly_sentiment_stddev"),
    //        round(
    //          sum(when(col("hour") >= 23 || col("hour") < 4, 1).otherwise(0)).cast("double") / count("*"), 4
    //        ).as("night_post_ratio")
    //      )
    //      .na.fill(0.0, Seq("hourly_sentiment_stddev"))

    val feat12 = predDF
      .groupBy("user_id")
      .agg(
        first("screen_name", ignoreNulls = true).as("screen_name"),
        //  把认证信息带下来，如果没有这个字段
        first("user_authentication", ignoreNulls = true).as("user_authentication"),
        round(stddev_samp("score"), 4).as("hourly_sentiment_stddev"),
        round(
          sum(when(col("hour") >= 23 || col("hour") < 4, 1).otherwise(0)).cast("double") / count("*"), 4
        ).as("night_post_ratio")
      )
      .na.fill(0.0, Seq("hourly_sentiment_stddev"))
      .na.fill("普通用户", Seq("user_authentication"))

    val explodedRegion = predDF
      .filter(col("location").isNotNull && col("location") =!= "")
      .select(col("user_id"), col("score"),
        explode(split(col("location"), " \\| ")).as("region"))
      .filter(col("region").isNotNull && col("region") =!= "")

    val topRegionDF = explodedRegion
      .groupBy("user_id", "region")
      .agg(count("*").as("cnt"), round(avg("score"), 4).as("region_avg_score"))
      .withColumn("rk", row_number().over(
        org.apache.spark.sql.expressions.Window.partitionBy("user_id").orderBy(desc("cnt"))))
      .filter(col("rk") === 1)
      .select(col("user_id"), col("region_avg_score").as("top_region_avg_score"))

    val regionDiversityDF = explodedRegion
      .groupBy("user_id")
      .agg(countDistinct("region").as("region_diversity_score"))

    val feat34 = topRegionDF
      .join(regionDiversityDF, Seq("user_id"), "left")
      .na.fill(0.0, Seq("top_region_avg_score"))
      .na.fill(0L, Seq("region_diversity_score"))

    val feat5 = predDF
      .groupBy("user_id")
      .agg(
        round(
          sum(when(col("label") === "负面",
            col("reposts_count") + col("comments_count") + col("attitudes_count")).otherwise(0)).cast("double") /
            greatest(sum(when(col("label") === "负面", 1).otherwise(0)).cast("double"), lit(1.0)), 2
        ).as("neg_avg"),
        round(
          sum(when(col("label") === "正面",
            col("reposts_count") + col("comments_count") + col("attitudes_count")).otherwise(0)).cast("double") /
            greatest(sum(when(col("label") === "正面", 1).otherwise(0)).cast("double"), lit(1.0)), 2
        ).as("pos_avg")
      )
      .withColumn("sentiment_leverage",
        round(col("neg_avg") / when(col("pos_avg") === 0, lit(1.0)).otherwise(col("pos_avg")), 4))
      .select("user_id", "sentiment_leverage")
      .na.fill(0.0, Seq("sentiment_leverage"))
    //
    //    val featureDF = feat12
    //      .join(feat34, Seq("user_id"), "left")
    //      .join(feat5, Seq("user_id"), "left")
    //      .na.fill(0.0)
    //      .cache()
    val featureDF = feat12
      .join(feat34, Seq("user_id"), "left")
      .join(feat5, Seq("user_id"), "left")
      .na.fill(0.0)
      // na.fill(0.0) 会把 Long 列也填成 Double，这里强制转回 Long
      .withColumn("region_diversity_score", col("region_diversity_score").cast("long"))
      .cache()


    println(s"特征聚合完成，共 ${featureDF.count()} 条，开始批量调用GMM接口...")
    predDF.unpersist()

    // 1. 拦截官媒
    val officialDF = featureDF.filter(
      col("user_authentication").contains("官方") ||
        col("user_authentication").contains("机构") ||
        col("user_authentication").contains("蓝V") ||
        col("screen_name").like("%官微%") ||
        col("screen_name").like("%融媒%") ||
        col("screen_name").like("%新闻%") ||
        col("screen_name").like("%发布%")
    ).cache() // 建议cache，因为后面多次left_anti用到

    // 2. 分流：沉默用户不调接口（剔除掉官媒
    val silentDF = featureDF.filter(
      col("hourly_sentiment_stddev") <= 0.001 &&
        col("night_post_ratio") === 0.0 &&
        col("region_diversity_score") === 0L &&
        col("sentiment_leverage") === 0.0
    ).join(officialDF, Seq("user_id"), "left_anti")

    // 需要调用接口的活跃普通网民
    val activeDF = featureDF
      .join(officialDF, Seq("user_id"), "left_anti")
      .join(silentDF, Seq("user_id"), "left_anti")
      .cache()

    println(s"机构官媒: ${officialDF.count()} 人 | 沉默路人: ${silentDF.count()} 人 | 活跃预测网民: ${activeDF.count()} 人")

    import scala.collection.JavaConverters._

    // 活跃用户调接口拼结果
    val activeRows = activeDF.collectAsList().asScala.toList
    val resultMap = CallBatchApi.batchGmmPredict(activeRows)

    val activeResult = activeRows.map { row =>
      val uid = row.getAs[String]("user_id")
      val (clusterId, prob0, prob1, prob2, prob3) = resultMap.getOrElse(uid, (-1, 0.25, 0.25, 0.25, 0.25))
      (uid, row.getAs[String]("screen_name"),
        row.getAs[Double]("hourly_sentiment_stddev"), row.getAs[Double]("night_post_ratio"),
        row.getAs[Long]("region_diversity_score"), java.util.Optional.ofNullable(row.getAs[Double]("top_region_avg_score")).orElse(0.0),
        row.getAs[Double]("sentiment_leverage"),
        clusterId, prob0, prob1, prob2, prob3, nowStr)
    }

    // 沉默用户直接填默认值
    val silentRows = silentDF.collectAsList().asScala.toList
    val silentResult = silentRows.map { row =>
      val uid = row.getAs[String]("user_id")
      (uid, row.getAs[String]("screen_name"),
        row.getAs[Double]("hourly_sentiment_stddev"), row.getAs[Double]("night_post_ratio"),
        row.getAs[Long]("region_diversity_score"), java.util.Optional.ofNullable(row.getAs[Double]("top_region_avg_score")).orElse(0.0),
        row.getAs[Double]("sentiment_leverage"),
        -1, 0.25, 0.25, 0.25, 0.25, nowStr)
    }

    // 官媒结果（cluster_id = 99）
    val officialRows = officialDF.collectAsList().asScala.toList
    val officialResult = officialRows.map { row =>
      val uid = row.getAs[String]("user_id")
      (uid, row.getAs[String]("screen_name"),
        row.getAs[Double]("hourly_sentiment_stddev"), row.getAs[Double]("night_post_ratio"),
        row.getAs[Long]("region_diversity_score"), java.util.Optional.ofNullable(row.getAs[Double]("top_region_avg_score")).orElse(0.0),
        row.getAs[Double]("sentiment_leverage"),
        99, 0.0, 0.0, 0.0, 0.0, nowStr) // 99专属标志位，概率全清空
    }

    // 结果拼接
    val profileDF = (activeResult ++ silentResult ++ officialResult).toDF(
      "user_id", "screen_name",
      "hourly_sentiment_stddev", "night_post_ratio",
      "region_diversity_score", "top_region_avg_score", "sentiment_leverage",
      "cluster_id", "prob_0", "prob_1", "prob_2", "prob_3", "update_time"
    )

    WriteMysql.writeTable(profileDF, "user_gmm_profile")

    // 善后释放内存
    activeDF.unpersist()
    officialDF.unpersist()
    featureDF.unpersist()
    println("完成画像")

  }



}