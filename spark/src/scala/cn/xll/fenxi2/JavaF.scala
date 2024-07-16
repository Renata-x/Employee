package cn.xll.fenxi2

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{BooleanType, FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object JavaF {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf()
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local")
    }

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL JDBC Web")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    // 定义 schema
    // 指定 job_salary 列为 FloatType
    val schema = StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("job_name", StringType, nullable = true),
      StructField("work_demand", StringType, nullable = true),
      StructField("company_local", StringType, nullable = true),
      StructField("company_name", StringType, nullable = true),
      StructField("guimo", StringType, nullable = true),
      StructField("job_salary", FloatType, nullable = true), // 这里指定为 FloatType
      StructField("job_salary_fif", BooleanType, nullable = true), // 这里指定为 FloatType
      StructField("demand", StringType, nullable = true)
      // 如果有更多列，继续添加
    ))

    val df = sparkSession.read
      .schema(schema)
      .option("header", "true")
      .csv("hdfs://spark01:8020/EmployeeBd/all.csv")

    // 选择 job_name 等于 'Web' 的数据
    val filteredDf = df.filter(df("job_name") === "Java")


    filteredDf.write.mode(SaveMode.Overwrite).saveAsTable("employee.java")
    filteredDf.printSchema();
    sparkSession.stop()
  }
}
