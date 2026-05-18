package com.weibo.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.weibo.utils.{KafkaUtils, WeiboFinalPipeline}
import com.weibo.myconfig.Config
import org.apache.spark.sql.functions.{col, length, regexp_replace, udf, when}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.IntegerType
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
      .enableHiveSupport()
      //      .config("spark.sql.catalogImplementation", "hive") //命名空间
      .config("hive.metastore.uris", "thrift://master1:9083")
      .getOrCreate()

    // 加载模型
    val model = WeiboFinalPipeline.trainModel(spark)
    import spark.implicits._
    var data = KafkaUtils.getKafkaStream(spark, Config.KAFKA_TOPIC)
    data = KafkaUtils.parseWeiboJson(data)

    data = dropColumns(data)
    data = cleanRemark(data)
    data = cleanText(data)
    data = cleanUserAuth(data)
    data = cleanInvalidText(spark, data, model)

    // 将数据输入到处理
    Classify.classByDayAndHour(spark, data)
    // 统计  定时统计 10s一次
    import java.util.{Timer, TimerTask}
    val timer = new Timer(true)

    timer.schedule(new TimerTask() {
      override def run(): Unit = {
        try {
          println("开始定时统计5min一次")
          Classify.classByIndicator(spark)
        } catch {
          case e: Exception =>
            println("统计出错，跳过本次：" + e.getMessage)
        }
      }
    }, 0, 60*1000*6) // 0秒后开始，每6min执行一次
    spark.streams.awaitAnyTermination()
  }

  def dropColumns(data: DataFrame): DataFrame = {
    data.drop(
      "article_url",
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
  //对无效文本进行判断
  def cleanInvalidText(spark: SparkSession, cleaned: DataFrame, model: PipelineModel):DataFrame = {
    // 特征工程
    val featureDF = WeiboFinalPipeline.RuleFeatureBuilder.build(cleaned)
    val df = WeiboFinalPipeline.jieba(featureDF)
    val result = model.transform(df)
    val cleanData = result
      .filter(col("prediction") === 1.0)
      // 保留原始列和预测列
      //      .select((cleaned.columns :+ "prediction").map(col): _*)
      .select(cleaned.columns.map(col): _*)
    cleanData
  }

  def cleanText(data: DataFrame):DataFrame = {
    data
      .withColumn("text",
        regexp_replace(col("text"),
          "@\\S+", ""))
      .filter(!col("text").contains("常识打卡"))
      .filter(!col("text").contains("天猫"))
      .filter(!col("text").contains("京东"))
      .filter(!col("text").contains("淘宝"))
      .filter(!col("text").contains("拼多多"))
      .filter(!col("text").contains("肖战"))
      .filter(!col("text").contains("杨紫"))
      .filter(!col("text").contains("常识翻身打卡计划"))
      .filter(col("text").isNotNull)
      .filter(length(col("text")) > 5)
  }

  // 处理互动度低的并转换数据类型为int
  def cleanRemark(data: DataFrame): DataFrame = {
    data
      .withColumn("reposts_count", col("reposts_count").cast(IntegerType))
      .withColumn("comments_count", col("comments_count").cast(IntegerType))
      .withColumn("attitudes_count", col("attitudes_count").cast(IntegerType))
      .filter("reposts_count > 0 OR comments_count > 0 OR attitudes_count > 0")
      .dropDuplicates("id")


  }
  // 观察数据后再决定怎么处理
  // 数据传输进下一组件 hive和mysql

}
