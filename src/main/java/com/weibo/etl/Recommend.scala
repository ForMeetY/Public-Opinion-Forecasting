package com.weibo.etl

import com.weibo.utils.Constant
import com.weibo.utils.mysqlUtils.{ReadMysql, WriteMysql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * @author Xbx
 * @date 2026/6/4 14:55
 */
object Recommend {

  def generateRecommendations(spark: SparkSession): Unit = {
    import spark.implicits._

    val nowStr = java.time.LocalDateTime.now()
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    // 读取用户画像
    println("Recommend 读取用户画像...")
    val userTimeProfile = ReadMysql.readTable(spark, "user_time_sentiment_profile")
      .withColumn("uid_time", col("user_id").cast("string"))
      .select("uid_time", "prefer_time_period")   // 只取用到的列，减少传输量

    val userRegionProfile = ReadMysql.readTable(spark, "user_region_sentiment_profile")
      .withColumn("uid_region", col("user_id").cast("string"))
      .select("uid_region", "top_focus_region")

    val userCount = userTimeProfile.count()
    println(s"Recommend 画像用户数: $userCount")
    if (userCount == 0) {
      println("Recommend 用户画像为空，跳过本次推荐")
      return
    }

    // 候选微博池：先过滤再 join，控制体量
    println("Recommend 读取候选微博池...")
    val candidateRaw = spark.table("weibo.topic_classify")
      .filter(col("dt") >= date_sub(current_date(), 3))   // 只取近3天
      .filter(col("topic_label").isNotNull)
      .filter(col("confidence") >= 0.7)                   // 提前过滤低质量
      .withColumn("author_id", col("user_id").cast("string"))
      .select("id", "bid", "author_id", "screen_name", "text",
        "topic_label", "location", "time_period", "confidence")
    // 如果候选池为空，则跳过推荐
    if (candidateRaw.count() == 0) {
      println("Recommend 候选微博池为空，跳过本次推荐")
      return
    }
    // join 情感热度分
    val candidateWithScore = candidateRaw
      .join(
        spark.table("weibo.predstatistic")
          .select("id", "score", "reposts_count", "comments_count", "attitudes_count"),
        Seq("id"), "left"
      )
      .withColumn("hot_score",
        coalesce(col("reposts_count"), lit(0)) * 3 +
          coalesce(col("comments_count"), lit(0)) * 2 +
          coalesce(col("attitudes_count"), lit(0))
      )
      .withColumn("final_score",
        col("hot_score") + coalesce(col("score"), lit(0.0)) * 10
      )
      .select("id", "bid", "author_id", "screen_name", "text",
        "topic_label", "location", "time_period", "final_score")
      .cache()  // 候选池被两路召回各用一次，cache 避免重算

    val candidateCount = candidateWithScore.count()
    println(s"[Recommend] 候选池大小: $candidateCount 条")
    if (candidateCount == 0) {
      println("[Recommend] 候选池为空，跳过本次推荐")
      candidateWithScore.unpersist()
      return
    }

    // 多路召回
    println("[Recommend] 执行多路召回...")

    val regionRecall = userRegionProfile
      .join(candidateWithScore,
        col("top_focus_region") === col("location"), "inner")
      .select(
        col("uid_region").as("target_user_id"),
        col("id"), col("bid"), col("author_id"), col("screen_name"),
        col("text"), col("topic_label"), col("location"),
        col("final_score"), lit("region").as("recall_reason")
      )

    val timeRecall = userTimeProfile
      .join(candidateWithScore,
        col("prefer_time_period") === col("time_period"), "inner")
      .select(
        col("uid_time").as("target_user_id"),
        col("id"), col("bid"), col("author_id"), col("screen_name"),
        col("text"), col("topic_label"), col("location"),
        col("final_score"), lit("time_period").as("recall_reason")
      )

    // 合并、去重、排序、取 Top20
    println("Recommend 合并去重排序...")

    val recallDF = regionRecall.unionByName(timeRecall)
      .filter(col("target_user_id") =!= col("author_id"))
      .dropDuplicates("target_user_id", "id")

    val windowSpec = Window.partitionBy("target_user_id").orderBy(desc("final_score"))

    // cache topNDF：writeTable 和 count 各触发一次 Action，cache 只算一次
    val topNDF = recallDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") <= 20)
      .select(
        col("target_user_id").as("user_id"),
        col("id").as("weibo_id"),
        col("bid"),
        col("screen_name"),
        col("text"),
        col("topic_label"),
        col("location"),
        col("final_score").as("score"),
        col("recall_reason"),
        lit(nowStr).as("create_time")
      )
      .repartition(4)   // 控制写 MySQL 的并发连接数，避免连接打满
      .cache()

    val total = topNDF.count()
    println(s"Recommend 待写入推荐记录: $total 条")

    if (total == 0) {
      println("Recommend 无推荐结果，跳过写入")
      topNDF.unpersist()
      candidateWithScore.unpersist()
      return
    }

    // 写入 MySQL
    println("Recommend 写入 MySQL...")
    val targetTable = getTargetTable()
    WriteMysql.writeTable(topNDF, targetTable)
    switchView(spark, targetTable) // 这里面不仅要 ALTER VIEW，还要更新 sys_config
    println(s"Recommend 写入完成，共 $total 条")

    // 释放缓存
    topNDF.unpersist()
    candidateWithScore.unpersist()
  }

  def switchView(spark: SparkSession, nextTable: String): Unit = {
    val conn = java.sql.DriverManager.getConnection(Constant.url, Constant.user, Constant.pwd)
    try {
      val stmt = conn.createStatement()
      stmt.execute(s"ALTER VIEW v_recommendations AS SELECT * FROM $nextTable")
      // 同步更新标记表
      stmt.execute(s"UPDATE sys_config SET value = '$nextTable' WHERE key_name = 'recommend_table'")
      println(s"[Recommend] 视图已成功切换至: $nextTable")
    } finally {
      conn.close()
    }
  }

  def getTargetTable(): String = {
    val conn = java.sql.DriverManager.getConnection(Constant.url, Constant.user, Constant.pwd)
    try {
      val rs = conn.createStatement().executeQuery("SELECT value FROM sys_config WHERE key_name = 'recommend_table'")
      val current = if (rs.next()) rs.getString("value") else "user_recommendations_v1"
      // 如果当前是 v1，下一次就是 v2，反之亦然
      if (current == "user_recommendations_v1") "user_recommendations_v2" else "user_recommendations_v1"
    } finally {
      conn.close()
    }
  }
}