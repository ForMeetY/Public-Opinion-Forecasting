package com.weibo.utils.hiveUtils

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Xbx
 * @date 2026/5/8 17:53
 */
object ReadHiveUtils {

  def readHiveclassByDayAndHour(spark: SparkSession,db:String,tableName:String): DataFrame = {
    println(s"正在读取表${db}.${tableName}")
    val df = spark.sql(s"select * from ${db}.${tableName}")
    df
  }

}
