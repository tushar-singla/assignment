bex_data = spark.read.parquet("s3://omni-channel-etl/prac/learn-spark/sources/BEX/")  ##reading data from bucket s3://omni-channel-etl/prac/learn-spark/sources/BEX/ 

TP_data = spark.read.parquet("s3://omni-channel-etl/prac/learn-spark/sources/TP/")  ##reading traveller data from bucket s3://omni-channel-etl/prac/learn-spark/sources/TP/
TP_data = TP_data.drop("tuid")  ##column'tuid' is needed to drop to avoid duplicate columns after join

customer_base_data = bex_data.join(TP_data,["tpid","eapid","email_address"],'inner')  ##performing join on 'bex_data' and 'TP_data' using columns "tpid","eapid","email_address"
customer_base_data = customer_base_data.dropDuplicates()
customer_base_data.write.mode("overwrite").parquet("s3://omni-channel-etl/prac/learn-spark/customer-base/")  ##storing the above join result in bucket s3://omni-channel-etl/prac/learn-spark/customer-base/
customer_base_data.show()
