s2 = spark.read.parquet("s3://omni-channel-etl/prac/learn-spark/sources/BEX/") #reading the data from s3 bucket

bex_email = s2.select("email_address").distinct()  #select distinct email addresses
bex_email = bex_email.withColumn("email_address",lower(trim(bex_email.email_address)))   ##*****##

traveller_data = spark.sql("select * from dm.traveler_profile")   ##reading traveller data from dm.traveler_profile
# traveller_data_limit = traveller_data.limit(100)

traveller_data.count()
bexTravellerData = bex_email.join(traveller_data,["email_address"],'left_outer')
bexTravellerData.write.mode("overwrite").parquet("s3://omni-channel-etl/prac/learn-spark/sources/TP-initial/")  ##write the obtained data on s3 bucket
