package DecisionTest
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spray.json.DefaultJsonProtocol.{jsonFormat3, jsonFormat2,lazyFormat, _}
import spray.json.JsonFormat
import spray.json._

/**
 * Hello world!
 *
 */
object Main extends App {
  println( "Hello World!" )
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Csv table")
      .config("spark.master", "local")
      .getOrCreate()

  /**
    * STEP 1
    */

  val df = spark.read
    .option("header", "true")
    .csv("D:\\Spark\\Stepik\\ScalaTinkoff\\Examples\\src\\main\\resources\\csvDataSet\\data.csv")

    df.show()
/*
  +-----------------+----+----------+-------+
  |             name| age|  birthday| gender|
  +-----------------+----+----------+-------+
  |    John Jonovich|  26|26-01-1995|   male|
    |             Lisa| xyz|26-01-1996| female|
    |             null|  26|25-11-2009|   male|
    |Julia Afanacievna|    |26-01-1995| female|
    |                 |null|26-01-1995|   null|
    |             Pete|    |26-01-1995|       |
  +-----------------+----+----------+-------+*/






  /**
    * STEP 2
    */

  val df_trim = df.select(df.columns.map(m => trim(col(m)).as(m)): _*) // del all spaces

  val clmns_for_filter = df_trim.columns.map(m => {col(m) =!= "" or col(m).isNull}).reduce(_ && _)

  val ds_fltr = df_trim.filter(clmns_for_filter)

  ds_fltr.show

/*
  +-------------+---+----------+------+
  |         name|age|  birthday|gender|
  +-------------+---+----------+------+
  |John Jonovich| 26|26-01-1995|  male|
    |         Lisa|xyz|26-01-1996|female|
    |         null| 26|25-11-2009|  male|
    +-------------+---+----------+------+
*/

  /**
    * STEP 3
    */


  val arguments = spark.read
    .option("multiline", "true")
    .json("D:\\Spark\\Stepik\\ScalaTinkoff\\Examples\\src\\main\\resources\\csvDataSet\\arguments.json")

  arguments.show()

  /*
    +---------------+-----------------+------------+-------------+
    |date_expression|existing_col_name|new_col_name|new_data_type|
    +---------------+-----------------+------------+-------------+
    |           null|             name|  first_name|       string|
    |           null|              age| total_years|      integer|
    |     dd-MM-yyyy|         birthday|       d_o_b|         date|
      +---------------+-----------------+------------+-------------+
  */


  val lst_columns_for_cast = arguments.collect.map(m => {
    m.getAs[String]("date_expression") match {
      case null =>
    col(m.getAs[String]("existing_col_name"))
      .cast(m.getAs[String]("new_data_type"))
      .as(m.getAs[String]("new_col_name"))
      case _ =>
    to_date(col(m.getAs[String]("existing_col_name")),m.getAs[String]("date_expression"))
      .as(m.getAs[String]("new_col_name"))
  }
  })


  val df_cast = ds_fltr.select(lst_columns_for_cast: _*)

  df_cast.show

  /*+-------------+-----------+----------+
  |   first_name|total_years|     d_o_b|
  +-------------+-----------+----------+
  |John Jonovich|         26|1995-01-26|
    |         Lisa|       null|1996-01-26|
    |         null|         26|2009-11-25|
    +-------------+-----------+----------+*/

  df_cast.printSchema()

/*
  root
  |-- first_name: string (nullable = true)
  |-- total_years: integer (nullable = true)
  |-- d_o_b: date (nullable = true)
*/


  /**
    * STEP 4
    */


  case class ValuesCount(vals: String, cnt: Long)
  case class ColUniqValue(Column: String, Unique_Values: Long, Values: List[ValuesCount])


  val lstUniqColumn = df_cast.columns.map(m => {
      val sum_gb = df_cast.filter(col(m).isNotNull)
                      .select(col(m))
                      .withColumn("row_num",lit(1))
                      .groupBy(m)
                      .agg(sum("row_num").as("sum_row"))

       val lst_ValCnt = sum_gb.collect().map(m2 => {
                ValuesCount(m2.getAs(m).toString, m2.getAs[Long]("sum_row"))
        }).toList

  ColUniqValue(m, sum_gb.count, lst_ValCnt)
  }
  )

  implicit val valuesCountFormat : JsonFormat[ValuesCount] = lazyFormat(jsonFormat2(ValuesCount.apply))
  implicit val colUniqValueFormat : JsonFormat[ColUniqValue] = lazyFormat(jsonFormat3(ColUniqValue.apply))

  println(lstUniqColumn.toJson)

    /*[{"Column":"first_name","Unique_Values":2,"Values":[{"vals":"John Jonovich","cnt":1},{"vals":"Lisa","cnt":1}]},
    {"Column":"total_years","Unique_Values":1,"Values":[{"vals":"26","cnt":2}]},
    {"Column":"d_o_b","Unique_Values":3,"Values":[{"vals":"2009-11-25","cnt":1},{"vals":"1995-01-26","cnt":1},{"vals":"1996-01-26","cnt":1}]}]*/

}
