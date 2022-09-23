//%AddJar https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.11/2.4.3/spark-cassandra-connector_2.11-2.4.3.jar
// %AddJar https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-20_2.11/6.8.9/elasticsearch-spark-20_2.11-6.8.9.jar
//%AddJar https://jdbc.postgresql.org/download/postgresql-42.2.12.jre6.jar




import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.cassandra._
import java.net.URLDecoder
import scala.util.{Try, Success, Failure}

object data_mart extends App {
  
val spark = SparkSession
  .builder
  .master("yarn")
  .appName("lab03")
  .getOrCreate()
import spark.implicits._

spark.conf.set("spark.cassandra.connection.host", "10.0.0.31")
spark.conf.set("spark.cassandra.connection.port", "9042")

def parser(url: String) = {
  Try {new java.net.URL(URLDecoder.decode(url, "UTF-8"))}.toOption
  match {
    case Some(_) => new java.net.URL(URLDecoder.decode(url, "UTF-8")).getHost
    case _ => "unable_to_parse"
  }
}
val udf_parser = udf(parser _)



// Extract

var webLogs = spark.read.parquet("/labs/laba03/weblogs.parquet") //hdfs
var shops = spark // elasticsearch
  .read
  .format("org.elasticsearch.spark.sql")
  .option("es.nodes.wan.only", "false")
  .option("es.nodes", "10.0.0.31:9200")
  .option("es.net.http.auth.user", "ivan_sviridov")
  .option("es.net.http.auth.user", "EuvE4TXG")
  .load("visits")
  .toDF
var cassandra = spark  // cassandra
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "clients", "keyspace" -> "labdata"))
  .load()
var postgres = spark // postgresql
  .read
  .format("jdbc")
  .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
  .option("driver", "org.postgresql.Driver")
  .option("user", "ivan_sviridov")
  .option("password", "EuvE4TXG")
  .option("dbtable", "domain_cats")
  .load()

// transform
cassandra = cassandra.withColumn("age_cat", when(col("age") <= 24, "18-24")
  when(col("age").between(25, 34), "25-34")
  when(col("age").between(35, 44), "35-44")
  when(col("age").between(45, 54), "45-54")
  when(col("age") >= 55, ">=55"))
  .drop("age")

shops=shops
  .withColumn("category", concat(lit("shop_"), lower(regexp_replace(col("category"), "-|/s", "_"))))
  .groupBy(col("uid").alias("uid_shops"))
  .pivot(col("category"))
  .count()

postgres = postgres
  .withColumn("category", concat(lit("web_"), lower(regexp_replace(col("category"), "-|/s", "_"))))

webLogs = webLogs
  .filter(col("uid").isNotNull)
  .withColumn("url", explode(col("visits.url")))
  .withColumn("parsed_url", udf_parser(col("url")))
  .withColumn("domain", when(col("parsed_url").startsWith("www."), expr("replace(parsed_url, 'www.','')")).otherwise(col("parsed_url")))
  .select(col("uid").alias("web_uid"), col("domain")).as("l")
  .join(postgres.as("c"), Seq("domain"), "inner")
  .withColumn("web_category", col("c.category"))
  .select("l.web_uid", "web_category")
  .groupBy("l.web_uid")
  .pivot("web_category")
  .agg(count(col("*")))


// load final

val resultDF = cassandra
  .join(broadcast(shops), cassandra("uid") === shops("uid_shops"), "left")
  .join(webLogs, cassandra("uid") === webLogs("web_uid"), "left")
  .drop("web_uid", "uid_shops")

resultDF.write
  .format("jdbc")
  .option("url", "jdbc:postgresql://10.0.0.31:5432/ivan_sviridov")
  .option( "dbtable", "clients")
  .option("driver", "org.postgresql.Driver")
  .option("user", "ivan_sviridov")
  .option("password", "EuvE4TXG")
  .mode("overwrite")
  .save
}



