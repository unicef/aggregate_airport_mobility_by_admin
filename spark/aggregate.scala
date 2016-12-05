import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val mobil = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("./transformed/mobility.csv")
mobil.registerTempTable("mobils")
val distOrigins = sqlContext.sql("select origin_id, dest_id, sum(pax) as cnt from mobils group by origin_id, dest_id order by cnt")
distOrigins.write.format("com.databricks.spark.csv").save("./processed/mobility.csv")
System.exit(0)
