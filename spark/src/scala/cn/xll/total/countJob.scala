package cn.xll.total

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, monotonically_increasing_id, round}
import org.apache.spark.sql.types.{BooleanType, FloatType, StringType, StructField, StructType}

object countJob {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf()
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local")
    }

    val sparkSession = SparkSession
      .builder()
      .appName("Job Count by Region")
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
    ))

    // 定义 schema 并读取 CSV 文件
    val df = sparkSession.read
      .schema(schema)
      .option("header", "true")
      .csv("hdfs://spark01:8020/EmployeeBd/all.csv")

    val jobCountsByRegion = df.groupBy("company_local", "job_name")
      .count()
      .withColumnRenamed("count", "job_count") // 重命名列以更清晰地表示其含义


    // 直接按照 company_local 和 job_name 排序，并选择需要的列
    val sortedJobCountsByRegion = jobCountsByRegion
      .orderBy("company_local", "job_name") // 确保结果按地区和岗位名称排序
      .select("company_local", "job_name", "job_count")

    // 显示排序后的结果
    sortedJobCountsByRegion.show()


    // 添加从1开始的自增ID列
    val jobCountWithId = sortedJobCountsByRegion.withColumn("id", monotonically_increasing_id() + 1)

    // 选择需要的列
    jobCountWithId.select("id", "company_local", "job_name", "job_count").show()


    // 首先获取 avgSalariesWithId 的列名列表
    val allColumns = jobCountWithId.columns

    // 然后创建一个 select 表达式列表，将 "id" 列放在前面，然后是其他所有列
    val selectExprs = col("id") +: allColumns.filterNot(_ == "id").map(col)

    // 最后使用这些表达式来选择列
    val jobCountWithIdOrdered = jobCountWithId.select(selectExprs: _*)


    // 打印 schema
    jobCountWithIdOrdered.printSchema()
    jobCountWithIdOrdered.show()
    jobCountWithIdOrdered.write.mode(SaveMode.Overwrite).saveAsTable("employee.job_count")

    // 停止 SparkSession
    sparkSession.stop()

  }
}
