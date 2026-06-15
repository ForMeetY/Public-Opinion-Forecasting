package com.weibo.etl

import com.weibo.utils.Constant
import com.weibo.utils.mysqlUtils.{ReadMysql, WriteMysql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author Xbx
 * @date 2026/6/4 14:55
 *
 *       最终得分 final_score = hot_score * 0.6 + score * 0.4
 *       热度得分 hot_score = log1p(reposts * 0.5 + comments * 0.2 + attitudes)
 *       利用 log1p (对数平滑) 强行压缩长尾爆款内容的能量级，规避极少数千万级顶流微博
 *       情感得分 score：来自下游深度学习 Chinese RoBERTa 模型对微博正文产出的 3 分类情感概率概率 [0,1]。
 */
object Recommend {

  def generateRecommendations(spark: SparkSession): Unit = {
    import spark.implicits._

    // 1. 读取用户画像
    println("[Recommend] 读取用户画像...")
    val userTimeProfile = ReadMysql.readTable(spark, "user_time_sentiment_profile")
      .withColumn("uid_time", col("user_id").cast("string"))
      .select("uid_time", "prefer_time_period")

    val userRegionProfile = ReadMysql.readTable(spark, "user_region_sentiment_profile")
      .withColumn("uid_region", col("user_id").cast("string"))
      .select("uid_region", "top_focus_region")

    // 2. 构建候选微博池
    println("[Recommend] 读取并过滤候选微博池...")
    val candidateRaw = spark.table("weibo.topic_classify")
      .filter(col("dt") >= date_sub(current_date(), 4))
      .filter(col("topic_label").isNotNull)
      .filter(col("confidence") >= 0.7)
      .withColumn("author_id", col("user_id").cast("string"))
      .select(
        "id", "bid", "author_id", "screen_name", "text",
        "topic_label", "location", "time_period"
      )

    val predstatistic = spark.table("weibo.predstatistic")
      .select("id", "score", "reposts_count", "comments_count", "attitudes_count")

    val candidateWithScore = candidateRaw
      .join(predstatistic, Seq("id"), "left")
      .withColumn(
        "hot_score",
        log1p(
          coalesce(col("reposts_count"),  lit(0)) * 0.8 +
            coalesce(col("comments_count"), lit(0)) * 0.6 +
            coalesce(col("attitudes_count"),lit(0)) * 0.1   //点赞数量普遍偏高
        )
      )
      .withColumn(
        "final_score",
        col("hot_score") * 0.6 + coalesce(col("score"), lit(0.0)) * 0.4
      )
      .filter(col("final_score") >= 0.5)
      .select(
        col("id"), col("bid"), col("author_id"), col("screen_name"), col("text"),
        col("topic_label"), col("location"), col("time_period"), col("final_score"),
        coalesce(col("score"), lit(0.0)).as("score") // 把原始情感 score 字段保留在物料池中
      )
      .cache()


    // 多路召回召回
    println("Recommend 执行多路召回")

    val regionRecall = broadcast(userRegionProfile)
      .join(candidateWithScore, col("top_focus_region") === col("location"), "inner")
      .select(
        col("uid_region").as("target_user_id"),
        col("id"), col("bid"), col("author_id"), col("screen_name"),
        col("text"), col("topic_label"), col("location"), col("time_period"),
        col("score"), col("final_score"), lit("region").as("recall_reason")
      )

    val timeRecall = broadcast(userTimeProfile)
      .join(candidateWithScore, col("prefer_time_period") === col("time_period"), "inner")
      .select(
        col("uid_time").as("target_user_id"),
        col("id"), col("bid"), col("author_id"), col("screen_name"),
        col("text"), col("topic_label"), col("location"), col("time_period"),
        col("score"), col("final_score"), lit("time_period").as("recall_reason")
      )


    // 合并去重并注入 GMM 软概率进行个性化
    println("Recommend 合并去重排序，进行全量特征心智精排...")

    val rawRecallDF = regionRecall
      .unionByName(timeRecall)
      .filter(col("target_user_id") =!= col("author_id"))   // 过滤自身内容
      .dropDuplicates("target_user_id", "id")               // 同用户同微博去重

    // 全量 GMM 画像表拉取对应的四大性格软概率
    val userGmmProfile = ReadMysql.readTable(spark, "user_gmm_profile")
      .select(
        col("user_id").as("u_id"),
        col("cluster_id"),
        col("prob_0").as("life_prob"),      // 生活号概率
        col("prob_1").as("night_cat_prob"), // 夜猫子概率
        col("prob_2").as("big_v_weight"),   // 大V权重
        col("prob_3").as("vent_prob")       // 情绪宣泄概率
      )

    //  score 和 time_period 以及用户标签
    val personalizedRankDF = rawRecallDF
      .join(userGmmProfile, col("target_user_id") === col("u_id"), "left")
      // 个性评分 为 原始得分 + 软概率
      .withColumn(
        "personalized_score",
        col("final_score") + (
          // 强行加分
          when(col("vent_prob") >= 0.4 && col("score") <= 0.3, col("vent_prob") * (lit(1.0) - col("score")) * 0.5)
            // 夜猫子作息共鸣
            .when(col("night_cat_prob") >= 0.4 && col("time_period") === "night", col("night_cat_prob") * 0.4)
            // 生活号地缘垂直对齐
            .when(col("life_prob") >= 0.4 && col("recall_reason") === "region", col("life_prob") * 0.3)
            .otherwise(lit(0.0))
          )
      )

    // 提取最终 Top8
    val topNDF = personalizedRankDF
      .groupBy("target_user_id")
      .agg(
        slice(
          sort_array(
            collect_list(
              struct(
                col("personalized_score").as("final_score"), // 替换原排序依据
                col("id"), col("bid"), col("author_id"), col("screen_name"),
                col("text"), col("topic_label"), col("location"), col("recall_reason")
              )
            ),
            asc = false
          ),
          1, 8
        ).as("top10")
      )
      .select(
        col("target_user_id").as("user_id"),
        explode(col("top10")).as("item")
      )
      .select(
        col("user_id"),
        col("item.id").as("weibo_id"),
        col("item.bid"),
        col("item.screen_name"),
        col("item.text"),
        col("item.topic_label"),
        col("item.location"),
        col("item.final_score").as("score"), // 映射回业务表需要的标准 score 命名字段
        col("item.recall_reason"),
        current_timestamp().as("create_time")
      )
      .cache()

    val total = topNDF.count()
    println(s"[Recommend] 待写入推荐记录: $total 条")

    if (total == 0) {
      println("[Recommend] 无推荐结果，跳过写入")
      topNDF.unpersist()
      candidateWithScore.unpersist()
      return
    }

    // 5. 写入 MySQL + 切换视图
    println("[Recommend] 写入 MySQL...")
    val targetTable = getNextTable()
    WriteMysql.writeTable(topNDF.repartition(8), targetTable)
    switchView(targetTable)

    println(s"[Recommend] 写入完成，共 $total 条，目标表: $targetTable")

    topNDF.unpersist()
    candidateWithScore.unpersist()
  }

  /**
   * 读取当前活跃表，返回下一张写入表（v1 <-> v2 双缓冲）。
   */
  def getNextTable(): String = {
    val conn = java.sql.DriverManager.getConnection(Constant.url, Constant.user, Constant.pwd)
    try {
      val rs = conn.createStatement().executeQuery(
        "SELECT value FROM sys_config WHERE key_name = 'recommend_table'"
      )
      val current = if (rs.next()) rs.getString("value") else "user_recommendations_v1"
      if (current == "user_recommendations_v1") "user_recommendations_v2"
      else "user_recommendations_v1"
    } finally conn.close()
  }

  /**
   * 切换视图并同步更新 sys_config 标记表。
   * ALTER VIEW 和 UPDATE 在同一连接内执行。
   */
  def switchView(nextTable: String): Unit = {
    val conn = java.sql.DriverManager.getConnection(Constant.url, Constant.user, Constant.pwd)
    try {
      val stmt = conn.createStatement()
      stmt.execute(s"ALTER VIEW v_recommendations AS SELECT * FROM $nextTable")
      stmt.execute(s"UPDATE sys_config SET value = '$nextTable' WHERE key_name = 'recommend_table'")
      println(s"[Recommend] 视图已切换至: $nextTable")
    } finally conn.close()
  }
}