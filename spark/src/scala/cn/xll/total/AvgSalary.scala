package cn.xll.total

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, monotonically_increasing_id, round}
import org.apache.spark.sql.types.{BooleanType, FloatType, StringType, StructField, StructType}

object AvgSalary {

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
    ))

    // 定义 schema 并读取 CSV 文件
    val df = sparkSession.read
      .schema(schema)
      .option("header", "true")
      .csv("hdfs://spark01:8020/EmployeeBd/all.csv")

    // 计算平均薪资并保留两位小数
    val avgSalaries = df.groupBy("job_name")
      .agg(
        round(avg("job_salary"), 2).alias("average_job_salary")
      )

    // 添加从1开始的自增ID列
    val avgSalariesWithId = avgSalaries.withColumn("id", monotonically_increasing_id()+1)

    // 选择需要的列
    avgSalariesWithId.select("id", "job_name", "average_job_salary").show()

    // 假定 avgSalariesWithId 是已经包含 id 列的 DataFrame
//    val avgSalariesWithIdOrdered = avgSalariesWithId.select(
//      col("id"), // 使用 col 函数引用列名 "id"
//      col("job_name"), // 继续添加其他需要的列
//      col("average_job_salary") // 这里添加平均薪资列
//      // 如果需要，可以继续添加更多的列
//    )

    // 首先获取 avgSalariesWithId 的列名列表
    val allColumns = avgSalariesWithId.columns

    // 然后创建一个 select 表达式列表，将 "id" 列放在前面，然后是其他所有列
    val selectExprs = col("id") +: allColumns.filterNot(_ == "id").map(col)

    // 最后使用这些表达式来选择列
    val avgSalariesWithIdOrdered = avgSalariesWithId.select(selectExprs: _*)


    // 打印 schema
    avgSalariesWithIdOrdered.printSchema()
    avgSalariesWithIdOrdered.show()
    avgSalariesWithIdOrdered.write.mode(SaveMode.Overwrite).saveAsTable("employee.avg_salary")

    // 停止 SparkSession
    sparkSession.stop()

  }
}
