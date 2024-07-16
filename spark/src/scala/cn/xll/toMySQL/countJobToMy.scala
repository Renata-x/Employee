package cn.xll.toMySQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object countJobToMy {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf()
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local")
    }

    val sparkSession = SparkSession
      .builder()
      .appName("movie bigdata to sql")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val hiveTableDF: DataFrame = sparkSession.table("employee.job_count")

    // 配置 JDBC 连接参数
    val url = "jdbc:mysql://spark01:3306/employee"
    val user = "root"
    val password = "Woshidale123@"
    val jdbcDriver = "com.mysql.jdbc.Driver"

    // 使用 JDBC 选项写入 MySQL
    hiveTableDF.write
      .mode("overwrite") // 或使用 "append"
      .format("jdbc")
      .option("url", url)
      .option("driver", jdbcDriver)
      .option("user", user)
      .option("password", password)
      .option("dbtable", "employee.job_count")
      .save()

    sparkSession.close()
  }

}
