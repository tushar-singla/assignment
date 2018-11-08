bex_data = spark.sql("select * from dm.bex_email_customer_base")         #read data from dm.bex_email_customer_base and storing in dataframe t1
bex_data = bex_data.filter("lang_id = 3081 or lang_id = 2057").filter("tpid = 29 or tpid = 14").filter("eapid = 0")        #apply the specified filter
bex_data.write.mode("overwrite").parquet("s3://omni-channel-etl/prac/learn-spark/sources/BEX/")       #write data to bucket 
bex_data.show()
