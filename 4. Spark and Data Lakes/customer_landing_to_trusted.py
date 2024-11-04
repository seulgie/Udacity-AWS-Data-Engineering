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

# Script generated for node Customer Landing
CustomerLanding_node1730752694233 = glueContext.create_dynamic_frame.from_catalog(database="seulgie", table_name="customer_landing", transformation_ctx="CustomerLanding_node1730752694233")

# Script generated for node Share with Research
SqlQuery0 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
SharewithResearch_node1730752773875 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1730752694233}, transformation_ctx = "SharewithResearch_node1730752773875")

# Script generated for node Customer Trusted
CustomerTrusted_node1730752869101 = glueContext.getSink(path="s3://seulgie-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1730752869101")
CustomerTrusted_node1730752869101.setCatalogInfo(catalogDatabase="seulgie",catalogTableName="customer_trusted")
CustomerTrusted_node1730752869101.setFormat("json")
CustomerTrusted_node1730752869101.writeFrame(SharewithResearch_node1730752773875)
job.commit()
