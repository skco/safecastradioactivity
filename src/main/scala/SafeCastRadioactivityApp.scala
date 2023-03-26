import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SafeCastRadioactivityApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SafeCastRadioactivityApp")
      //.master("local[*]")
      .getOrCreate()


    //val TXT:Dataset[Row] = spark.read.text(SafeCastRadioactivitySettings.dataset).limit(100)
    //TXT.show(truncate = false)

    val DF:Dataset[Row] = spark.read
                               .option("header",value=true)
                               .option("delimiter",",")
                               .option("inferschema",value= true)
                               .csv(SafeCastRadioactivitySettings.dataset)
    DF.show()
    DF.write.parquet(SafeCastRadioactivitySettings.HDFSpath+"/safecast/measurments.pq")

  }

}
