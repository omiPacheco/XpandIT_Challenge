package org
import com.sun.org.apache.xerces.internal.impl.xpath.regex.RegularExpression
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{asc, col, collect_list, collect_set, first, row_number}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}

import java.io.File

object RecruitmentChallenge extends App{

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Recruitment Challenge")
      .getOrCreate();

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

    //df1.show(100)

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

  val columns: Array[String] = df3_unordered.columns
  val reorderedColumnNames: Array[String] = Array(columns(0),
    columns(12),
    columns(1),
    columns(2),
    columns(3),
    columns(4),
    columns(5),
    columns(6),
    columns(7),
    columns(8),
    columns(9),
    columns(10),
    columns(11))
  val df3: DataFrame = df3_unordered.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

  val reg_exp = "([0-9]+)(M|k|\\+)|(Varies with device)".r
  df3.withColumn("Size",col("Size"))
}