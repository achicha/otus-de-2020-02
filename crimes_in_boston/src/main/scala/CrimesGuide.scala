import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CrimesGuide extends App {

    // data file name
    val crimePath = args(0)
    val offenseCodesPath = args(1)
    val outputPath = args(2)

    // create session
    val spark = SparkSession
        .builder()
        .appName("crimes_in_boston")
        .master("local[*]")
        .getOrCreate();

    import spark.implicits._

    //====================================
    // join into one DataFrame
    val crimeFacts = spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(crimePath)

    val offenseCodes = spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(offenseCodesPath)
        // we need only only name per code. remove others.
        .withColumn(
            "rn",
            row_number() over Window.partitionBy($"code").orderBy($"name")
        )
        .where($"rn" === 1)
        .drop("rn")

    val df = crimeFacts
        .join((offenseCodes), $"CODE" === $"OFFENSE_CODE")
        // remove crimes without district
        .where("district is not null")

    //====================================
    // calculate metrics

    // crimes_total - общее количество преступлений в этом районе
    val crimes_total = df
        .groupBy("district")
        .agg(count("*").alias("crimes_total"))

    // crimes_monthly - медиана числа преступлений в месяц в этом районе
    val crimes_monthly_raw = df
        .groupBy("DISTRICT", "YEAR", "MONTH")
        .count
        .orderBy("DISTRICT", "YEAR", "MONTH")
        .drop("YEAR", "MONTH")
        .createOrReplaceTempView("crimes_monthly_raw")

    val crimes_monthly = spark.sql(
        """
                SELECT
                    DISTRICT
                    ,percentile_approx(count, 0.5) as crimes_monthly
                FROM crimes_monthly_raw
                GROUP BY DISTRICT
                """
        )

    // frequent_crime_types - три самых частых crime_type за всю историю наблюдений в этом районе
    val frequent_crime_types = df
        .select($"DISTRICT", $"NAME")
        .withColumn("_tmp", split($"NAME", " ")) //т.к не везде есть "-", то возмем первое слово ДО пробела.
        .withColumn("crime_type", $"_tmp".getItem(0))
        .drop("NAME","_tmp")
        .groupBy("DISTRICT", "crime_type")
        .count
        .withColumn(
            "rn",
            row_number() over Window.partitionBy($"DISTRICT").orderBy($"count".desc)
        )
        .where($"rn" <= 3)
        .orderBy($"DISTRICT", $"count".desc)
        .drop("rn")
        .groupBy("DISTRICT")
        .agg(concat_ws(", ", collect_list($"crime_type")).alias("frequent_crime_types"))

    // lat and lng: координаты района, рассчитаные по средним
    val districtCoords = df
        .select($"DISTRICT", $"LAT", $"LONG")
        .groupBy("DISTRICT")
        .agg(
            avg($"Lat").as("lat")
            ,avg($"Long").as("lng")
        )

    //====================================
    // print and save results to parquet
    val result = crimes_total
        .join(crimes_monthly,  Seq("DISTRICT"))
        .join(frequent_crime_types,  Seq("DISTRICT"))
        .join(districtCoords,  Seq("DISTRICT"))
        .orderBy("DISTRICT")
        //.show(20, false)
        .coalesce(1).write.parquet(outputPath)
}
