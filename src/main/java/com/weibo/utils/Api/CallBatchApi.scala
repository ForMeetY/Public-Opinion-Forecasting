package com.weibo.utils.Api

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.json.JSONArray
import org.json.JSONObject
import scala.collection.mutable
/**
 * @author Xbx
 * @date 2026/5/19 11:42
 */
object CallBatchApi {
  /**
   * 批量调用情感分析接口 /predict_batch
   * @param textList 待分析文本集合
   * @return 文本 -> (标签名,标签id,分数,置信度)
   */
  def batchSentimentPredict(textList: List[String]): Map[String, (String, Int, Double, Double)] = {
    // 接口地址
    val apiUrl = "http://127.0.0.1:8000/predict_batch"
    // 创建http客户端
    val httpClient = HttpClients.createDefault()
    val httpPost = new HttpPost(apiUrl)
    httpPost.setHeader("Content-Type", "application/json;charset=UTF-8")

    // 构造请求json {"texts": ["文本1","文本2"]}
    val reqJson = new JSONObject()
    val textArr = new JSONArray()
    textList.foreach(textArr.put)
    reqJson.put("texts", textArr)

    val entity = new StringEntity(reqJson.toString, "UTF-8")
    httpPost.setEntity(entity)

    // 发送请求并获取响应
    val response = httpClient.execute(httpPost)
    val respStr = EntityUtils.toString(response.getEntity, "UTF-8")
    httpClient.close()

    // 解析返回结果
    val respJson = new JSONObject(respStr)
    val resultArr: JSONArray = respJson.getJSONArray("results")
    val resultMap = mutable.HashMap[String, (String, Int, Double, Double)]()

    for (i <- 0 until resultArr.length()) {
      val item = resultArr.getJSONObject(i)
      val srcText = textList(i)
      val label = item.getString("label")
      val labelId = item.getInt("label_id")
      val score = item.getDouble("score")
      val confidence = item.getDouble("confidence")
      resultMap.put(srcText, (label, labelId, score, confidence))
    }
    resultMap.toMap
  }

}
