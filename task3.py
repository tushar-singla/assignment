s3 = spark.read.parquet("s3://omni-channel-etl/prac/learn-spark/sources/TP-initial/*") #reading data from s3 bucket
eapid_data = spark.read.parquet("s3://omni-channel-etl/prac/source/Dimlocale/")  #reading data from s3://omni-channel-etl/prac/source/Dimlocale/ for eapid data
tpid_flat = s3.select(explode("tpid_list").alias("tpid"),"email_address")  #exploding the tpid_list
traveller3 = tpid_flat.join(s3,"email_address",'left_outer') #joining the exploded tpid data with extracted data in step1
traveller3 = traveller3.join(eapid_data,"tpid",'left_outer')  #joining the data obtained in step 4 with eapid data
traveller3 = traveller3.filter("tpid = 29 or tpid = 14").filter("eapid = 0") #filtering the required data


from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType,StringType, DateType
from pyspark.sql.functions import col, udf, explode

zipper = udf(
  lambda a, b, c, d, e, f: list(zip(a, b, c, d, e, f)),
  ArrayType(StructType([
      # Adjust types to reflect data types
      StructField("p", IntegerType()),
      StructField("q", IntegerType()),
      StructField("r", StringType()),
      StructField("s", StringType()),
      StructField("t", StringType()),
      StructField("u", DateType())
  ]))
)

traveller3 = (traveller3
    .withColumn("abrakadabra", zipper("tpid_list", "tuid_list","account_type_list","first_name_list","last_name_list","account_create_date_list"))
    # UDF output cannot be directly passed to explode
    .withColumn("abrakadabra", explode("abrakadabra"))
    .select("expuserid",col("abrakadabra.p").alias("tpid"), col("abrakadabra.q").alias("tuid"),"latest_trip_begin_use_date",col("email_address").alias("email_address"),col("abrakadabra.r").alias("account_type"),col("abrakadabra.s").alias("first_name"),col("abrakadabra.t").alias("last_name"),col("abrakadabra.u").alias("account_create_date"),col("email_address").alias("useremailadr"),"expuser_status_id","eapid"))
traveller3 = traveller3.filter("tpid = 29 or tpid = 14").filter("eapid = 0")
traveller3.show()
traveller3.write.mode("overwrite").parquet("s3://omni-channel-etl/prac/learn-spark/sources/TP/")
