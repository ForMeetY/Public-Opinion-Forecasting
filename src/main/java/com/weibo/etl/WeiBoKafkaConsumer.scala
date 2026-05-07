package com.weibo.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.weibo.utils.{KafkaUtils, RuleFeatureBuilder, WeiboFinalPipeline}
import com.weibo.myconfig.Config
import org.apache.spark.sql.functions.{col, length, regexp_replace, udf, when}
import org.apache.spark.ml.PipelineModel
/**
 * @author Xbx
 * @date 2026/5/6 21:10
 */

/*
* 用于从kafka读取数据并进行清洗
* */
object WeiBoKafkaConsumer {
  // main方法
  def main(args: Array[String]): Unit = {
    // 从kafka读取数据
    val spark = SparkSession.builder()
      .appName("WeiBoKafkaConsumer")
      .master("local[*]")
      .getOrCreate()
    val model = WeiboFinalPipeline.trainModel(spark)
    import spark.implicits._
    var data = KafkaUtils.getKafkaStream(spark, Config.KAFKA_TOPIC)
    data = KafkaUtils.parseWeiboJson(data)

    data = dropColumns(data)
    data = cleanRemark(data)
    data = cleanText(data)
    data = cleanUserAuth(data)
    data = cleanInvalidText(spark, data, model)
//val result = data
//  .transform(dropColumns)
//  .transform(cleanRemark)
//  .transform(cleanText)
//  .transform(cleanUserAuth)
//  .transform(cleanInvalidText(_, model)) // 必须这样写！

    data.writeStream
      .format("console")   // 输出到控制台
      .option("truncate", false)
//      .option("checkpointLocation", "./checkpoint/weibo")
      .start()            // 启动流
      .awaitTermination() // 持续运行

  }

  def dropColumns(data: DataFrame): DataFrame = {
    data.drop(
      "article_url",
      "location",
      "at_users",
      "pics",
      "video_url",
      "source",
      "retweet_id",
      "ip",
      "vip_type", "vip_level")
  }

    def cleanUserAuth(data:DataFrame):DataFrame = {
      data
        .withColumn(
          "user_authentication",
          when(col("user_authentication").isNull ||
            col("user_authentication") === "", "普通用户")
            .otherwise(col("user_authentication")) as "user_authentication"
        )
    }
  //TODO 对无效文本进行判断

  def cleanInvalidText(spark: SparkSession, cleaned: DataFrame, model: PipelineModel):DataFrame = {
    // 特征工程
    val featureDF = RuleFeatureBuilder.build(cleaned)
    // 分词
    val jiebaUDF = udf { text: String =>
      val seg = new com.huaban.analysis.jieba.JiebaSegmenter()
      if (text == null || text.trim.isEmpty) Seq.empty[String]
      else seg.sentenceProcess(text).toArray.filter(_ != null).map(_.toString).filter(_.length > 1).toSeq
    }
    val wordsDF = featureDF.withColumn("words", jiebaUDF(col("text")))
    val result = model.transform(wordsDF)
//    val cleanData = result.filter(col("prediction") === 1.0)
    val cleanData = result
//      .filter(col("prediction") === 1.0)
      // 保留原始列和预测列
      .select((cleaned.columns :+ "prediction").map(col): _*)
//      .select(cleaned.columns.map(col): _*)
    cleanData
  }

    def cleanText(data: DataFrame):DataFrame = {
      data
        // 1. 去掉 #话题#
//        .withColumn("text",
//          regexp_replace(col("text"),
//            "#[^#]+#", ""))
        // 2. 去掉@用户
        .withColumn("text",
          regexp_replace(col("text"),
            "@\\S+", ""))
        .filter(col("text").isNotNull)
        .filter(length(col("text")) > 5)
    }

  def cleanRemark(data: DataFrame): DataFrame = {
    data.filter("reposts_count > 10 OR comments_count > 10 OR attitudes_count > 30")
      .dropDuplicates("id")
  }


  // 观察数据后再决定怎么处理

  // 数据传输进下一组件 hive和mysql



}
