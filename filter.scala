import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object filter   {

//val kafka_topic_name = "lab04_input_data"
//val kafka_offset = "earliest"
//val output_dir_prefix = "/user/ivan.sviridov/visits/view"
val spark = SparkSession
  .builder()
  .appName(name = "lab04a")
  .master("yarn")
  .getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  import spark.implicits._
  val sc = spark.sparkContext
  val kafka_topic_name = sc.getConf.get("spark.filter.topic_name")
  var kafka_offset = sc.getConf.get("spark.filter.offset")
  val output_dir_prefix = sc.getConf.get("spark.filter.output_dir_prefix")


  val startingOffsets = if (kafka_offset == "earliest" | kafka_offset == "latest")  kafka_offset  else s"""{"${kafka_topic_name}" : {"0" : ${kafka_offset}}}"""

val kafkaOptions = Map(
  "kafka.bootstrap.servers" -> "10.0.0.31:6667",
  "subscribe" -> kafka_topic_name,
  "startingOffsets" -> startingOffsets,
  "maxOffsetsPerTrigger" -> "30"
)

val kafkaDF = spark.read.format("kafka").options(kafkaOptions).load

val ddl = """`event_type` STRING, `category` STRING, `item_id` STRING,  `item_price` INTEGER, `uid` STRING, `timestamp` LONG"""
val schemajson = DataType.fromDDL(ddl).asInstanceOf[StructType]

val parsedDF = kafkaDF.select(from_json('value.cast(StringType),schemajson).alias("value"))
  .select("value.*")
  .withColumn("date", date_format(($"timestamp" / 1000).cast(TimestampType).cast(DateType), "yyyyMMdd")) // + на -
  .withColumn("p_date" , col("date"))


val buyDF = parsedDF.filter(col("event_type") === "buy")
val viewDF = parsedDF.filter(col("event_type") === "view")

buyDF.write.partitionBy("p_date").mode("overwrite").json(s"$output_dir_prefix/buy" )
viewDF.write.partitionBy("p_date").mode("overwrite").json(s"$output_dir_prefix/view")
}