package com.weibo.utils

/**
 * @author Xbx
 * @date 2026/5/28 22:52
 */
object Constant {
  final val url = "jdbc:mysql://master1:3306/weibo?useUnicode=true&characterEncoding=utf-8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"
  final val user = "root"
  final val pwd = "1234"

  // 时间  "2026-06-02" 主要控制读取Hive的日期
  final val time = "2026-05-05"

  // 控制用户画像的统计时间
  final val startHour = 23
  final val startMin = 0
  final val endMin = 59
  final val endHour = 23

}
