package cn.xll.total

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, desc, monotonically_increasing_id}
import org.apache.spark.sql.types.{BooleanType, FloatType, StringType, StructField, StructType}

object jobSalary {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf()
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local")
    }

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL JDBC job salary")
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


    // 选择工作岗位和工资字段，然后按工资降序排序
    val jobSalaryDesc = df.select("job_name", "job_salary")
      .orderBy(desc("job_salary")) // 根据工资降序排序sc("max_salary")) // 根据工资降序排序

    // 显示结果
    jobSalaryDesc.show()


    // 添加从1开始的自增ID列
    val jobSalaryWithId = jobSalaryDesc.withColumn("id", monotonically_increasing_id() + 1)

    // 选择需要的列
    jobSalaryWithId.select("id", "job_name", "job_salary").show()


    // 首先获取 avgSalariesWithId 的列名列表
    val allColumns = jobSalaryWithId.columns

    // 然后创建一个 select 表达式列表，将 "id" 列放在前面，然后是其他所有列
    val selectExprs = col("id") +: allColumns.filterNot(_ == "id").map(col)

    // 最后使用这些表达式来选择列
    val jobSalaryWithIdOrdered = jobSalaryWithId.select(selectExprs: _*)


    // 打印 schema
//    jobSalaryWithIdOrdered.printSchema()
    jobSalaryWithIdOrdered.write.mode(SaveMode.Overwrite).saveAsTable("employee.job_order_salary")

    // 停止 SparkSession
    sparkSession.stop()
  }
}
