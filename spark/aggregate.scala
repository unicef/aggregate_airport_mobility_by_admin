import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val sconf = new SparkConf()
val paramsString = sconf.get("spark.driver.extraJavaOptions")
val paramsSlice = paramsString.slice(2,paramsString.length)
val paramsArray = paramsSlice.split(",")
val arg1 = paramsArray(0)
val mobil = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("./transformed/" + arg1)
mobil.registerTempTable("mobils")
val distOrigins = sqlContext.sql("select origin_id, dest_id, sum(pax) as cnt from mobils group by origin_id, dest_id order by cnt")
distOrigins.write.format("com.databricks.spark.csv").save("./processed/" + arg1)
System.exit(0)
