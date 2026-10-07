import org.apache.spark.sql.SparkSession

case class Employee(
  employeeId: Int,
  name: String,
  department: String,
  salary: Double
)

object Day14App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 14 - DataFrame and Dataset")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    println("===== DAY 14: DATAFRAME AND DATASET =====")

    val employees = Seq(
      Employee(1, "Amit", "IT", 65000),
      Employee(2, "Rahul", "HR", 55000),
      Employee(3, "Sara", "Finance", 75000),
      Employee(4, "Neha", "IT", 85000),
      Employee(5, "John", "Finance", 70000)
    )

    val dataset = spark.createDataset(employees)

    println("\nDATASET:")
    dataset.show(false)

    println("\nDATASET TYPE:")
    println(dataset.getClass.getName)

    println("\nTYPED FILTER:")
    dataset
      .filter(_.salary > 60000)
      .show(false)

    println("\nDATASET TO DATAFRAME:")

    val dataframe = dataset.toDF()

    dataframe.show(false)
    dataframe.printSchema()

    println("\nDATAFRAME OPERATIONS:")

    dataframe
      .groupBy("department")
      .agg(
        org.apache.spark.sql.functions.avg("salary").as("average_salary"),
        org.apache.spark.sql.functions.max("salary").as("maximum_salary")
      )
      .show(false)

    println("\nRDD VS DATAFRAME VS DATASET:")

    println("RDD:")
    println("Low-level distributed collection with less optimization.")

    println("DataFrame:")
    println("Distributed table-like data structure optimized by Catalyst.")

    println("Dataset:")
    println("Typed distributed collection combining type safety with Spark SQL optimization.")

    println("\nTYPE SAFETY:")
    println("Dataset provides compile-time type safety through case classes.")

    println("\nCATALYST:")
    println("Spark SQL uses Catalyst optimizer to optimize DataFrame and Dataset queries.")

    spark.stop()
  }
}
