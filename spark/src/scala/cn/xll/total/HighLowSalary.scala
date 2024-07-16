package cn.xll.total
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, monotonically_increasing_id, round}
import org.apache.spark.sql.types.{BooleanType, FloatType, StringType, StructField, StructType}
object HighLowSalary {
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
    val maxMinSalaries = df.groupBy("company_local", "job_name")
      .agg(
        max("job_salary").alias("max_salary"), // 每个组的最高工资
        min("job_salary").alias("min_salary") // 每个组的最低工资
      )


    // 选择需要的列，并按 company_local 和 job_name 排序
    val sortedMaxMinSalaries = maxMinSalaries
      .select( "company_local", "job_name", "max_salary", "min_salary")
      .orderBy("company_local", "job_name") // 添加排序

    // 添加从1开始的自增ID列
    val maxMinSalariesWithId = sortedMaxMinSalaries.withColumn("id", monotonically_increasing_id() + 1)

    // 选择需要的列
    maxMinSalariesWithId.select("id","company_local", "job_name", "max_salary","min_salary").show()


    // 首先获取 avgSalariesWithId 的列名列表
    val allColumns = maxMinSalariesWithId.columns

    // 然后创建一个 select 表达式列表，将 "id" 列放在前面，然后是其他所有列
    val selectExprs = col("id") +: allColumns.filterNot(_ == "id").map(col)

    // 最后使用这些表达式来选择列
    val maxMinSalariesWithIdOrdered = maxMinSalariesWithId.select(selectExprs: _*)


    // 打印 schema
    maxMinSalariesWithIdOrdered.printSchema()
    maxMinSalariesWithIdOrdered.show()
    maxMinSalariesWithIdOrdered.write.mode(SaveMode.Overwrite).saveAsTable("employee.max_min_salary")

    // 停止 SparkSession
    sparkSession.stop()

  }

}
