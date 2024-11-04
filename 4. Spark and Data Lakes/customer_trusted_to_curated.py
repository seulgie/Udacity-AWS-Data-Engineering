import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1730753856894 = glueContext.create_dynamic_frame.from_catalog(database="seulgie", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1730753856894")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1730754360764 = glueContext.create_dynamic_frame.from_catalog(database="seulgie", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1730754360764")

# Script generated for node Join Customer
JoinCustomer_node1730754380924 = Join.apply(frame1=AccelerometerLanding_node1730754360764, frame2=CustomerTrusted_node1730753856894, keys1=["user"], keys2=["email"], transformation_ctx="JoinCustomer_node1730754380924")

# Script generated for node Drop Fields and Duplicates
SqlQuery0 = '''
select distinct customername, email, phone, birthday,
serialnumber, registrationdate, lastupdatedate,
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate
from myDataSource
'''
DropFieldsandDuplicates_node1730756541872 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":JoinCustomer_node1730754380924}, transformation_ctx = "DropFieldsandDuplicates_node1730756541872")

# Script generated for node Amazon S3
AmazonS3_node1730754555830 = glueContext.getSink(path="s3://seulgie-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1730754555830")
AmazonS3_node1730754555830.setCatalogInfo(catalogDatabase="seulgie",catalogTableName="customer_curated")
AmazonS3_node1730754555830.setFormat("json")
AmazonS3_node1730754555830.writeFrame(DropFieldsandDuplicates_node1730756541872)
job.commit()
