package org
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{asc, col}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructType}

import java.io.File

object RecruitmentChallenge extends App{

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Recruitment Challenge")
    .getOrCreate();


  val user_reviews_schema = new StructType()
    .add("App",StringType,true)
    .add("Review",StringType,true)
    .add("Sentiment",StringType,true)
    .add("Polarity",DoubleType,true)
    .add("Subjectivity",DoubleType,true)



  val df1_schema = new StructType()
    .add("App",StringType,true)
    .add("Average_Sentiment_Polarity",DoubleType,true)

  val user_reviews_df = spark.read
    .format("csv")
    .option("header","true")
    .schema(user_reviews_schema)
    .load("./datasets/googleplaystore_user_reviews.csv")

  val df1 = user_reviews_df.drop("Review","Sentiment","Subjectivity")
    .groupBy("App")
    .avg("Polarity")
    .na
    .fill(0,Array("avg(Polarity)"))
    .withColumnRenamed("avg(Polarity)","Average_Sentiment_Polarity")



  // ------------------------------------------------------------------------------------------------- //

  val apps_schema = new StructType()
    .add("App",StringType,true)
    .add("Category",StringType,true)
    .add("Rating",DoubleType,true)
    .add("Reviews",LongType,true)
    .add("Size",StringType,true)
    .add("Installs",StringType,true)
    .add("Type",StringType,true)
    .add("Price",DoubleType,true)
    .add("Content_Rating",StringType,true)
    .add("Genres",StringType,true)
    .add("Last Updated",DateType,true)
    .add("Current Version",StringType,true)
    .add("Android Version",StringType,true)

  val apps_df = spark.read
    .format("csv")
    .option("header","true")
    .schema(apps_schema)
    .load("./datasets/googleplaystore.csv")
    .na
    .fill(0,Array("Rating"))
    .filter(col("Rating") >= 4)
    .sort(col("Rating").desc)

  /* Write the contents of the dataframe, including the headers, and changing the delimiter */
  apps_df.write.mode(SaveMode.Overwrite).option("delimiter","§").option("header",true).format("csv").save("./datasets/best_apps")

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

  // ------------------------------------------------------------------------------------------------- //



}