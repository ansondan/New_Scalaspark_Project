import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, FloatType}
//import org.apache.spark.sql.{SparkSession, Row}

object Spark_Project {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ScalaSparkTransformations")
      .master("local") // Set master to "local" for local mode
      .getOrCreate()
    val csvFilePath = raw"C:\Users\Consultant\Downloads\sample_data.csv"
    // Read the CSV file
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvFilePath)

    // Show the original DataFrame
    df.show()

    val selectedColumns = df.select("Name", "Age")

    // Show the resulting DataFrame
    selectedColumns.show()

    val filteredDF = df.filter(df("Age") > 30)

    // Show the resulting DataFrame
    filteredDF.show()

    val dfWithNewColumn = df.withColumn("NewSalary", df("Salary") + 10000)

    // Show the resulting DataFrame
    dfWithNewColumn.show()

    val groupedDF = df.groupBy("Age")
      .agg(avg("Salary").alias("AverageSalary"))

    // Show the resulting DataFrame
    groupedDF.show()

    val sortedDF = df.orderBy("Age")

    // Show the resulting DataFrame
    sortedDF.show()

    val dfWithAgeCategory = df.withColumn("AgeCategory", when(col("Age") > 30, "Yes").otherwise("No"))

    // Show the resulting DataFrame
    dfWithAgeCategory.show()

    val dfWithoutSalary = df.drop("Salary")

    // Show the resulting DataFrame
    dfWithoutSalary.show()

    val dfRenamed = df.withColumnRenamed("Name", "Full_name")

    // Show the resulting DataFrame
    dfRenamed.show()

//    val schema = new StructType()
//      .add(StructField("Name", StringType, true))
//      .add(StructField("Age", IntegerType, true))
//      .add(StructField("Salary", FloatType, true))
//
//     Create a collection of Row objects
//    val data = Seq(
//      Row("Mike", 25, 80000)

    //)
    //val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    //df2.show()

    // Show the resulting DataFrame

//    val unionDF = df1.union(df2)
//
//     Show the resulting DataFrame
//    unionDF.show()
  }
}