package com.weibo.utils

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

object RuleFeatureBuilder {

  def build(df: DataFrame): DataFrame = {

    df
      // ========= 基础文本特征 =========
      // 文本长度
      .withColumn("text_length", length(col("text")))

      // ========= 结构特征 =========
      // 是否包含#
      .withColumn("has_hashtag",
        when(col("text").contains("#"), 1).otherwise(0))
      // 是否包含@
      .withColumn("has_at",
        when(col("text").contains("@"), 1).otherwise(0))

      // ========= 噪声特征 =========
      // 是否包含重复字符
      .withColumn("has_repeat_char",
        when(col("text").rlike("(.)\\1{4,}"), 1).otherwise(0))
      // emoji的个数

      // ========= 语义规则 =========
      // 是否包含垃圾关键词
      .withColumn("has_spam_keyword",
        when(col("text").rlike("广告|加V|抽奖|秒杀|代理|点击链接"), 1).otherwise(0))
      // 垃圾关键词的个数
      .withColumn("spam_keyword_count",
        size(split(col("text"), "广告|加V|抽奖|秒杀|代理|点击链接")))
      // 是否包含推广
      .withColumn("has_promotion",
        when(col("text").rlike("关注我|私信|领取福利"), 1).otherwise(0))
      // 推广的个数
      .withColumn("promotion_count",
        (length(col("text")) -
          length(regexp_replace(col("text"), "关注我|私信|领取福利|又好又便宜", ""))) / 4
      )

      // ========= 行为特征 =========
      // 是不是热门 （根据回复数）
      .withColumn("is_viral",
        when(col("reposts_count") > 1000, 1).otherwise(0))

      // 互动度
      .withColumn("engagement_score",
        col("reposts_count") + col("comments_count") + col("attitudes_count"))

      // 互动度（原始值）
      .withColumn("engagement",
        col("reposts_count") + col("comments_count") + col("attitudes_count"))

      // 互动度比率
      .withColumn("engagement_ratio",
        (col("reposts_count") + col("comments_count") + col("attitudes_count")) / (length(col("text")) + 1))

      // ========= 强度特征 =========
      // 垃圾密度
      .withColumn("spam_density",
        col("spam_keyword_count") / (length(col("text"))+1))
    // 推广密度
      .withColumn("promotion_density",
        col("promotion_count") / (length(col("text"))+1))
  }
}