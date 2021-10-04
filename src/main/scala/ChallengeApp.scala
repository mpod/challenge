package experiments.spark

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.io.{File, FileWriter, PrintWriter}

object ChallengeApp extends App {

  // Helper function for writing to the file
  private def write(fileName: String)(f: PrintWriter => Unit): Unit = {
    val file = new File(fileName)
    val pw = new PrintWriter(new FileWriter(file))
    f(pw)
    pw.close()
  }

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Challenge")
    .getOrCreate()

  // Part 1
  // Task 1
  val rdd = spark.sparkContext.textFile("groceries.csv").flatMap(_.split(","))

  // Task2
  val result12a = rdd.distinct().collect()
  write("out/out_1_2a.txt") { pw => result12a.foreach(pw.println) }
  val result12b = rdd.count()
  write("out/out_1_2b.txt") { pw =>
    pw.println("Count:")
    pw.println(result12b)
  }

  // Task3
  val result3 = rdd.map((_, 1)).reduceByKey(_ + _).sortBy(-_._2).take(5)
  write("out/out_1_3.txt") { pw => result3.foreach(pw.println) }

  // Part2
  // Task 1
  val parqDf = spark.read.parquet("sf-airbnb-clean.parquet")
  parqDf.createOrReplaceTempView("airbnb")

  // Task 2
  val task22sql = spark.sql("select min(price) as min_price, max(price) as max_price, count(*) as row_count from airbnb")
  val result22 = task22sql.rdd.collect().head
  write("out/out_2_2.txt"){ pw =>
    val columns = result22.schema.fields.map(_.name)
    pw.println(columns.mkString(","))
    pw.println(columns.map(result22.getAs[Any]).map(_.toString).mkString(","))
  }

  // Task 3
  val task23sql = spark.sql("select avg(bathrooms) as avg_bathrooms, avg(bedrooms) as avg_bedrooms from airbnb where price > 5000 and review_scores_value = 10")
  val result23 = task23sql.rdd.collect().head
  write("out/out_2_3.txt"){ pw =>
    val columns = result23.schema.fields.map(_.name)
    pw.println(columns.mkString(","))
    pw.println(columns.map(result23.getAs[Any]).map(_.toString).mkString(","))
  }

  // Task 4
  val result24 = spark.sql("select accommodates from airbnb")
    .sort(col("price").asc, col("review_scores_rating").desc)
    .take(1)
    .map { r => r(0).asInstanceOf[Double].toInt }
    .head
  write("out/out_2_4.txt") { pw =>
    pw.println(result24)
  }

  // Part3
  val labels = Map(
    "Iris-setosa" -> 0.0d,
    "Iris-versicolor" -> 1.0d,
    "Iris-virginica" -> 2.0d
  )
  val rdd3 = spark.sparkContext.textFile("iris.csv").map{ row =>
    val parts = row.split(",")
    Row(parts(0).toDouble, parts(1).toDouble, parts(2).toDouble, parts(3).toDouble, labels(parts(4)))
  }
  val schema = new StructType()
    .add(StructField("feature1", DoubleType, nullable = true))
    .add(StructField("feature2", DoubleType, nullable = true))
    .add(StructField("feature3", DoubleType, nullable = true))
    .add(StructField("feature4", DoubleType, nullable = true))
    .add(StructField("label", DoubleType, nullable = true))
  // Create dataframe from data and schema
  val df3 = spark.createDataFrame(rdd3, schema)
  val assembler = new VectorAssembler()
    .setInputCols(Array("feature1", "feature2", "feature3", "feature4"))
    .setOutputCol("features")
  // Transform dataframe to the shape suitable for Logistic Regression
  val df3a = assembler.transform(df3)
  val lr = new LogisticRegression().setFeaturesCol("features")
  val model3 = lr.fit(df3a)
  write("out/out_3_2.txt") { pw =>
    pw.println("class")
    pw.println(model3.predict(Vectors.dense(5.1d, 3.5d, 1.4d, 0.2d)))
    pw.println(model3.predict(Vectors.dense(6.2d, 3.4d, 5.4d, 2.3d)))
  }
}
