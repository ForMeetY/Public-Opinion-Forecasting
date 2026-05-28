package com.weibo.utils.mysqlUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.weibo.utils.Constant
/**
 * @author Xbx
 * @date 2026/5/9 14:17
 */
// 写入mysql

object WriteMysql {
  private val url = Constant.url
  private val password = Constant.pwd
  private val user = Constant.user
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
    println(tableName+"写入成功")

  }
}
