---
author: Daniel Faria
date: 28/07/2022
topic: XpandIT Challenge
---

# XpandIT Spark 2 Recruitment Challenge

## Part 1

Given the simplicity of this task, I decided to take advantage of the functional programming capabilities and return df1 at once following this order.<br>

1.  Read the data keeping the headers
2.  Drop the columns I don't need
3.  Cast the type of the "Sentiment_Polarity" column into Double so I can calculate the average value
4.  Turn null values into 0
5.  Group rows by App name since they should be unique
6.  Calculate the "Average_Sentiment_Polarity
7.  And finally rename the column name

These steps can be seen in the snippet below.

```Scala
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
```

<hr>

## Part 2

For this task I had to search for filtering and sorting methods such as "filter" and "sort". Luckily they were easy to understand and use.

1.  First things first, I read the data from the CSV file <br>
```Scala
val apps_df = spark.read
      .option("header","true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("encoding","cp1252")
      .csv("./datasets/googleplaystore.csv")
```
2.  Then I applied the following spark methods
    1.  withColumn - to cast the Rating as Double
    2.  na.fill - to replace null values as 0
    3.  filter - to select only the apps with Rating greater or equal to 4
    4.  sort - to sort the Apps from best to worse
```Scala
val best_apps_df = apps_df
      .withColumn("Rating",col("Rating").cast(DoubleType))
      .na
      .fill(0,Array("Rating"))
      .filter(col("Rating") >= 4)
      .sort(col("Rating").desc)
```
3.  Finally I had to save the dataframe selecting a custom delimiter as follows
   ```Scala
       best_apps_df.write.mode(SaveMode.Overwrite).option("delimiter","§").option("header",true).format("csv").save("./datasets/best_apps")
   ```

As it was requested to save the dataframe as "best_apps.csv" I took the liberty of using the file system to manipulate the name of the resulting file and its location with the code below.
```Scala
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
```

## Part 3

First I decided to group Apps with the same value and collecting their categories as a set so the values aren't repeated.
```Scala
val categories_df = apps_df.groupBy("App")
    .agg(collect_set("Category"))
    .withColumnRenamed("collect_set(Category)","Categories")
```

Then, and after a lot of research, I found out that I could use the method "first" to my advantage to keep the values of the highest rated App and at the same time renaming to what was requested.

```Scala
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
```

Now I join the Categories with the new dataframe and drop the Category column since the categories are now organized in a different way.

```Scala
val df3_unordered = df3_grouped
    .join(categories_df,Seq("App"),"inner")
    .drop(col("Category"))
```

After this I had to update the values of the "Size" and "Price" columns. This task was rather difficult as I struggled to find a way to change the values of the rows based on conditions.<br>
So what I ended up doing was creating a method $transformSizeAndPrice$ 
```Scala
def transformSizeAndPrice(dataFrame: DataFrame): DataFrame = {...}
```
which was a method that would take advantage of Regular Expressions to verify what kind of value was being read.<br>
The idea is that using a for cycle I go through all rows and try to match the regular expression with the value on the row and then applying then changes and storing that information on a ListBuffer that will later be turned into a DataFrame.
<br><br>
Lets break it down. <br>
1.  Declare de ListBuffer
```Scala
    var sizes_seq = new ListBuffer[(String,Double,Double,Array[String])]()
```
2.  Create the regular expressions
```Scala
val reg_exp_size = "([0-9]+(\\.[0-9]+)?)(M|k|\\+)|(Varies with device)".r
    val reg_exp_price = "\\$([0-9]+(\\.[0-9]+)?)".r
```
3.  Then run the cycle on the selected columns
```Scala
for (row <- dataFrame.select( "App", "Size","Price","Genres").rdd.collect) {...}
```
4. Baed on the match create a tuple and add it to the ListBuffer
```Scala
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
          0.001,
          price_dollar*0.9,
          genres_array))
        case anything => sizes_seq += ((row(0).toString,
          0.0,
          price_dollar*0.9,
          genres_array))
      }
```
5.  And finally turn the ListBuffer into a dataframe, returning it in the end.
```Scala
val aux_ds = spark.createDataset(sizes_seq.toSeq).toDF()
      .withColumnRenamed("_1","App")
      .withColumnRenamed("_2","NSize")
      .withColumnRenamed("_3","NPrice")
      .withColumnRenamed("_4","NGenres")
return aux_ds
```
If you pay close attention you notice that the column "Genres" was selected as well. That is because I took the chance to also turn the "Genres" into an array using the split method as follows
```Scala
val genres_array = row(3).toString.split(";").toArray
```

<br>
Now that I have the types updated I can join them with the main dataframe. Lets also rename the new columns and drop the old ones.

```Scala
val df3_typed = df3_unordered.join(new_col,Seq("App"),"inner")
    .drop("Size","Genres","Price")
    .withColumnRenamed("NSize","Size")
    .withColumnRenamed("NGenres","Genres")
    .withColumnRenamed("NPrice","Price")
```

To finish this task I now have to re-order the columns so it stays as it is supposed to.

```Scala
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
```

## Part 4
This task was rather simple as all I had to do was group information. Having that said I joined both dataframes based on App name and saved the result compressed and as a parquet as was requested.

```Scala
val cleaned_df = df1.join(df3,Seq("App"),"inner")
  cleaned_df.write.mode("overwrite").option("compression","gzip").parquet("./datasets/googleplaystore_cleaned")
```

## Part 5
For this task I began by getting rid of the information I didn't need
```Scala
val df4_useful_info = df3.join(df1,Seq("App"),"inner").drop("Categories","App","Reviews","Size","Installs","Type","Price","Content_Rating","Last_Updated","Current_Version","Minimum_Android_Version")
```

After that I use a method I created 
```Scala
def metrics_aux(dataFrame: DataFrame): DataFrame = {...}
```
which will create two dataframes, one with the count of Apps for a given genre and the other with the average values of Rating and Sentiment_Polarity.<br>
This method will then return the joined result of these dataframes based on the Genre value.

```Scala
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
```

## Final Notes
The implementation of this application is not 100% complete and here is why. <br>
1.  On Part 3 I am required to turn the Last_Updated column into a Date type. After a lot of research I couldn't find a way to do it using the methods I had such as to_date and format_date. I also tried using regular expressions to extract the value of the month day and year and create a Date but that wasn't successful either.
2.  On Part 5 I was required to create a dataframe with all genres individually and get the data associated with them. My issue here was that the Genres were stored in an array such as this one ["Genre_1","Genre_2"] and I wasn't able to find a way to split these two genres into two different rows.

This was an intense week of research and learning and I definitely got even more into the data world. <br>
My past experience with other languages and libraries made a huge impact on my problem solving capabilities related to this challenge and even though I am not 100% happy about what I've done I think I did good and I know that with a bit more time and research I can do much much better. <br>

As for the way I coded for this challange, I didn't really see the need for moduling the code into different classes as I went into it as more of a script. Having that said I believe that a modulated application where the information gets stored in its variables and such, is a more pleasent way of implementing this application but it will possibly consume more memmory.

