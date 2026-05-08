package com.weibo.utils

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, IDF, VectorAssembler}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}

object WeiboFinalPipeline {

  def trainModel(spark: SparkSession) = {
    // 读取你的真实CSV
    var df = spark.read.format("csv")
      .option("header", "true")
      .option("encoding","GBK")
      .load("src/main/java/com/weibo/utils/data/data.csv")
    df = df.filter("label is not null AND label != ''")



    // label转数字
    val dfFixed = df.withColumn("label", col("label").cast("double"))
    // 划分训练集和测试集
    val Array(trainDF, testDF) = dfFixed.randomSplit(Array(0.8, 0.2), seed = 42)

    // 特征工程
    val ruleDF = RuleFeatureBuilder.build(trainDF)

    //TODO解决Spark序列化报错

    val jiebaUDF = udf((text: String) => {
      val segmenter = new JiebaSegmenter()  // 内部创建，不被序列化
      if (text == null || text.trim.isEmpty) Seq.empty[String]
      else segmenter.sentenceProcess(text).toArray.map(_.toString).filter(_.length > 1).toSeq
    })
    val wordsDF = ruleDF.withColumn("words", jiebaUDF(col("text")))

    // TF-IDF
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(1000)

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("tfidf_features")


    // 特征融合
    val ruleFeatures = Array(
      "text_length",
       "spam_keyword_count",
      "promotion_count", "is_viral", "engagement_score",
      "spam_density", "promotion_density",
      "engagement", "engagement_ratio"
    )

    val assembler = new VectorAssembler()
      .setInputCols(ruleFeatures :+ "tfidf_features")
      .setOutputCol("final_features")
      .setHandleInvalid("skip")
    // 模型训练
    val lr = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("final_features")
      .setPredictionCol("prediction")
    val pipeline = new Pipeline()
      .setStages(Array(hashingTF, idf, assembler, lr))

    val model = pipeline.fit(wordsDF)
    //测试
    simpleTestAccuracy(spark,model, testDF)
    model
  }

  // ==============================
  // 特征工程
  // ==============================
  object RuleFeatureBuilder {
    def build(df: DataFrame): DataFrame = {
      df
        .withColumn("text_length", length(col("text")))
//        .withColumn("has_hashtag", when(col("text").contains("#"), 1).otherwise(0))
//        .withColumn("has_at", when(col("text").contains("@"), 1).otherwise(0))
//        .withColumn("has_repeat_char", when(col("text").rlike("(.)\\1{4,}"), 1).otherwise(0))
//        .withColumn("has_spam_keyword", when(col("text").rlike("加V|抽奖|秒杀|代理|点击链接"), 1).otherwise(0))
        .withColumn("spam_keyword_count", size(split(col("text"), "加V|抽奖|秒杀|代理|私信|领取|福利|红包|点击|下单|推广|包邮|免费|送|宝子|亲亲|抽|双11")))
//        .withColumn("has_promotion", when(col("text").rlike("关注我|私信|领取福利"), 1).otherwise(0))
        .withColumn("engagement", col("reposts_count") + col("comments_count") + col("attitudes_count"))
        .withColumn("engagement_ratio", col("engagement") / (length(col("text")) + 1))
        .withColumn("promotion_count", (length(col("text")) - length(regexp_replace(col("text"), "关注|转发|评论|点赞|进店|抢购|优惠|又好又便宜", ""))) / 8)
        .withColumn("is_viral", when(col("reposts_count") > 1000, 1).otherwise(0))
        .withColumn("engagement_score", col("reposts_count") + col("comments_count") + col("attitudes_count"))
        .withColumn("spam_density", col("spam_keyword_count") / (length(col("text")) + 1))
        .withColumn("promotion_density", col("promotion_count") / (length(col("text")) + 1))
    }
  }


  // 简单测试正确率
  def simpleTestAccuracy(spark: SparkSession, model: PipelineModel,df:DataFrame): Unit = {
    import spark.implicits._

    // 测试集只需要做：补齐字段 + 类型转换
    val dfFixed = df.withColumn("label", col("label").cast("double"))
//    val dfFinal = dfFixed
//      .withColumn("reposts_count", lit(0))
//      .withColumn("comments_count", lit(0))
//      .withColumn("attitudes_count", lit(0))
    // 构造特征
    val ruleDF = buildFeatures(dfFixed)
    // 直接用训练好的模型预测
    val result = model.transform(ruleDF)

    // 统计准确率
    val total = result.count()
    val correct = result.filter(col("label") === col("prediction")).count()
    val accuracy = correct.toDouble / total

    println("==== 简单模型测试结果 ====")
    println(s"总样本数：$total")
    println(s"预测正确：$correct")
    println(f"正确率：${accuracy * 100}%.2f %%")

    // 误判样本
    println("\n==== 真实有效但被误判为垃圾的样本 ====")
    result
      .filter(col("label") === 1.0 && col("prediction") === 0.0)
      .select("text", "label", "prediction")
      .show(false)
  }

  // 构造特征
  def buildFeatures(df: DataFrame): DataFrame = {
    val featureDF = RuleFeatureBuilder.build(df)
    // 分词
    val jiebaUDF = udf { text: String =>
      val seg = new com.huaban.analysis.jieba.JiebaSegmenter()
      if (text == null || text.trim.isEmpty) Seq.empty[String]
      else seg.sentenceProcess(text).toArray.filter(_ != null).map(_.toString).filter(_.length > 1).toSeq
    }
    val wordsDF = featureDF.withColumn("words", jiebaUDF(col("text")))
    wordsDF
  }


}