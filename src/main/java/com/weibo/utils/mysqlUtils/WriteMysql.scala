package com.weibo.utils.mysqlUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
/**
 * @author Xbx
 * @date 2026/5/9 14:17
 */
// 写入mysql

object WriteMysql {
  private val url = "jdbc:mysql://master1:3306/weibo?useUnicode=true&characterEncoding=utf-8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"
  private val user = "root"
  private val password = "1234"
  def writeTable(data:DataFrame, tableName:String): Unit = {
    data.write
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", tableName)
      .mode(SaveMode.Overwrite)       // 覆盖模式
      .option("truncate", "true")
      .save()
    println("写入成功")

  }
}
