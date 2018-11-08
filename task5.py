customer_data = spark.read.parquet("s3://omni-channel-etl/prac/learn-spark/customer-base/")

from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType,StringType, DateType
from pyspark.sql.functions import col, udf, explode
customer_data_filter = customer_data.filter((col("first_name")=='?')|(col("first_name")=='unknown')|(col("first_name")=='Unknown')|(col("first_name")=='Not Available')|(col("first_name")=='NotAvailable')|(col("first_name")=='NA')|(col("first_name")=='N/A')|(col("first_name")=='Not Avail')|(col("first_name")=='JUNK')|(col("first_name")=='FIRSTNAME')|(col("first_name")=='LASTNAME')|(col("first_name")=='---')|(col("last_name")=='?')|(col("last_name")=='unknown')|(col("last_name")=='Unknown')|(col("last_name")=='Not Available')|(col("last_name")=='NotAvailable')|(col("last_name")=='NA')|(col("last_name")=='N/A')|(col("last_name")=='Not Avail')|(col("last_name")=='JUNK')|(col("last_name")=='FIRSTNAME')|(col("last_name")=='LASTNAME')|(col("last_name")=='---'))
customer_data_filter.count()

def names(col):
    if col!= None:
	    col = col.replace("?", 'None').replace("unknown",'None').replace("Unknown", 'None').replace("Not Available", 'None').replace("NotAvailable", 'None').replace("NA", 'None').replace("N/A", 'None').replace("Not Avail", 'None').replace("JUNK", 'None').replace("FIRSTNAME", 'None').replace("LASTNAME", 'None').replace("---", 'None')
	    return col

names_udf = udf(names,StringType())
customer_data = customer_data.withColumn("first_name",names_udf(col("first_name")))
customer_data = customer_data.withColumn("last_name",names_udf(col("last_name")))

customer_data.show()
