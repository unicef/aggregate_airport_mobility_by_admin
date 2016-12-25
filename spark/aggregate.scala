import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val sconf = new SparkConf()
val paramsString = sconf.get("spark.driver.extraJavaOptions")
val paramsSlice = paramsString.slice(2,paramsString.length)
val paramsArray = paramsSlice.split(",")
val arg1 = paramsArray(0)
val collection = paramsArray(1);
val statement = if (collection == "traffic") {
  "select year, week, sum(pax) as cnt, file as file, first_value(origin_iso), origin_id, first_value(dest_iso), dest_id from mobils group by origin_id, dest_id, year, week order by cnt" } else {
  "select year, week, sum(pax) as cnt, file as file, first_value(origin_iso), origin_id, first_value(dest_iso), dest_id from mobils group by origin_id, dest_id, year, week order by cnt"
}
val mobil = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("./temp/transformed/" + arg1)
mobil.registerTempTable("mobils")
println(statement)
val distOrigins = sqlContext.sql(statement)
distOrigins.write.format("com.databricks.spark.csv").save("./temp/processed/" + arg1)
System.exit(0)
