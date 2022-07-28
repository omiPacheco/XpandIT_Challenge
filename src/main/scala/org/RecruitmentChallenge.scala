package org
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{asc, col, collect_list, collect_set, first, row_number}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

import java.io.File
import java.util
import scala.collection.mutable.ListBuffer

object RecruitmentChallenge extends App{
  val spark = SparkSession.builder().appName("Recruitment Challenge").config("spark.master","local[*]").getOrCreate()
  import spark.implicits._

  def transformSizeAndPrice(dataFrame: DataFrame): DataFrame = {
    import spark.implicits._
    var sizes_seq = new ListBuffer[(String,Double,Double,Array[String])]()

    val reg_exp_size = "([0-9]+(\\.[0-9]+)?)(M|k|\\+)|(Varies with device)".r
    val reg_exp_price = "\\$([0-9]+(\\.[0-9]+)?)".r

    for (row <- dataFrame.select( "App", "Size","Price","Genres").rdd.collect) {
      val match_result_size = reg_exp_size.findAllIn(row(1).toString)
      val match_result_price = reg_exp_price.findAllIn(row(2).toString)


      var price_dollar: Double = 0.0

      val genres_array = row(3).toString.split(";").toArray

      if(reg_exp_price.matches(row(2).toString)){
        price_dollar = match_result_price.group(1).toDouble
      }else {
        price_dollar = 0.0

      }

      match_result_size.group(3) match {
        case "M" => sizes_seq += ((row(0).toString,
          match_result_size.group(1).toDouble,
          price_dollar*0.9,
          genres_array))
        case "k" => sizes_seq += ((row(0).toString,
          match_result_size.group(1).toDouble/1024,
          price_dollar*0.9,
          genres_array))
        case "+" => sizes_seq += ((row(0).toString,
          /*match_result.group(1).toDouble) duvida sobre como tratar este elemento*/0.001,
          price_dollar*0.9,
          genres_array))
        case anything => sizes_seq += ((row(0).toString,
          0.0,
          price_dollar*0.9,
          genres_array))
      }

    }
    val aux_ds = spark.createDataset(sizes_seq.toSeq).toDF()
      .withColumnRenamed("_1","App")
      .withColumnRenamed("_2","NSize")
      .withColumnRenamed("_3","NPrice")
      .withColumnRenamed("_4","NGenres")
    return aux_ds
  }

  def metrics_aux(dataFrame: DataFrame): DataFrame = {
    val aux_1 = dataFrame.withColumn("Genres",col("Genres").getItem(0))
      .groupBy("Genres")
      .count()

    val aux_2 = dataFrame.withColumn("Genres",col("Genres").getItem(0))
      .withColumn("Rating",col("Rating").cast(DoubleType))
      .groupBy("Genres")
      .avg("Rating","Average_Sentiment_Polarity")
      .withColumnRenamed("avg(Rating)","Average_Rating")
      .withColumnRenamed("avg(Average_Sentiment_Polarity)","Average_Sentiment_Polarity")

    val aux = aux_1.join(aux_2,Seq("Genres"),"inner")
      .withColumnRenamed("Genres","Genre")
      .withColumnRenamed("count","Count")

    return aux
  }


  /**
   * NaN values removed before group to avoid losing data.
   */
  val df1 = spark.read
      .format("csv")
      .option("header","true")
      .load("./datasets/googleplaystore_user_reviews.csv")
      .drop("Translated_Review","Sentiment","Sentiment_Subjectivity")
      .withColumn("Sentiment_Polarity",col("Sentiment_Polarity").cast(DoubleType))
      .na
      .fill(0,Array("Sentiment_Polarity"))
      .groupBy("App")
      .avg("Sentiment_Polarity")
      .withColumnRenamed("avg(Sentiment_Polarity)","Average_Sentiment_Polarity")

    // ------------------------------------------------------------------------------------------------- //

    val apps_df = spark.read
      .option("header","true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("encoding","cp1252")
      .csv("./datasets/googleplaystore.csv")

    // Remove NaNs only after casting to Double
    val best_apps_df = apps_df
      .withColumn("Rating",col("Rating").cast(DoubleType))
      .na
      .fill(0,Array("Rating"))
      .filter(col("Rating") >= 4)
      .sort(col("Rating").desc)

    /* Write the contents of the dataframe, including the headers, and changing the delimiter */
    best_apps_df.write.mode(SaveMode.Overwrite).option("delimiter","§").option("header",true).format("csv").save("./datasets/best_apps")

    /* Get the file where the dataframe was written */
    val directory = new File("./datasets/best_apps/")

    /* Check if it exists and if it is a directory
    *  Then get the .csv file in it
    *  Create a File giving it the desirable name
    *  End by deleting all files from the folder and then the folder itself
    * */
    if(directory.exists && directory.isDirectory){
      val file = directory.listFiles.filter(_.getName.endsWith(".csv")).head

      val dest = new File("./datasets/best_apps.csv")
      if(dest.exists) dest.delete()
      file.renameTo(dest)
      directory.listFiles.map(f => f.delete())
      directory.delete()
    }

  // --------------------------------------------------------------------------------------------- //

  val categories_df = apps_df.groupBy("App")
    .agg(collect_set("Category"))
    .withColumnRenamed("collect_set(Category)","Categories")

  val df3_grouped = apps_df.groupBy("App").agg(max("Rating") as "Rating",
    first("Category") as "Category",
    first("Reviews") as "Reviews",
    first("Size") as "Size",
    first("Installs") as "Installs",
    first("Type") as "Type",
    first("Price") as "Price",
    first("Content Rating") as "Content_Rating",
    first("Genres") as "Genres",
    first("Last Updated") as "Last_Updated",
    first("Current Ver") as "Current_Version",
    first("Android Ver") as "Minimum_Android_Version")

  val df3_unordered = df3_grouped
    .join(categories_df,Seq("App"),"inner")
    .drop(col("Category"))

  val new_col = transformSizeAndPrice(df3_unordered)

  val df3_typed = df3_unordered.join(new_col,Seq("App"),"inner")
    .drop("Size","Genres","Price")
    .withColumnRenamed("NSize","Size")
    .withColumnRenamed("NGenres","Genres")
    .withColumnRenamed("NPrice","Price")

  val columns: Array[String] = df3_typed.columns
  val reorderedColumnNames: Array[String] = Array(columns(0),
    columns(9),
    columns(1),
    columns(2),
    columns(10),
    columns(3),
    columns(4),
    columns(11),
    columns(5),
    columns(12),
    columns(6),
    columns(7),
    columns(8))

  val df3: DataFrame = df3_typed.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

  /* -------------------------------------------------------------------------------------------------- */
  val cleaned_df = df1.join(df3,Seq("App"),"inner")
  cleaned_df.write.mode("overwrite").option("compression","gzip").parquet("./datasets/googleplaystore_cleaned")

  /* ---------------------------------------------------------------------------------------------------- */

  val df4_useful_info = df3.join(df1,Seq("App"),"inner")
    .drop("Categories","App","Reviews","Size","Installs","Type","Price","Content_Rating","Last_Updated","Current_Version","Minimum_Android_Version")

  val df4 = metrics_aux(df4_useful_info)
  df4.write.mode("overwrite").option("compression","gzip").parquet("./datasets/googleplaystore_metrics")
}