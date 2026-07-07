import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted Source
AccelerometerTrustedSource_node1783436521138 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedSource_node1783436521138")

# Script generated for node Customer Trusted Source
CustomerTrustedSource_node1783436485036 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrustedSource_node1783436485036")

# Script generated for node Customer Curated Join
SqlQuery2879 = '''
select distinct c.* from c
inner join a 
on c.email = a.user;
'''
CustomerCuratedJoin_node1783436664526 = sparkSqlQuery(glueContext, query = SqlQuery2879, mapping = {"a":AccelerometerTrustedSource_node1783436521138, "c":CustomerTrustedSource_node1783436485036}, transformation_ctx = "CustomerCuratedJoin_node1783436664526")

# Script generated for node Customer Curated Target
EvaluateDataQuality().process_rows(frame=CustomerCuratedJoin_node1783436664526, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783435636421", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCuratedTarget_node1783436846195 = glueContext.getSink(path="s3://data-lake-stedi/curated/customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCuratedTarget_node1783436846195")
CustomerCuratedTarget_node1783436846195.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCuratedTarget_node1783436846195.setFormat("json")
CustomerCuratedTarget_node1783436846195.writeFrame(CustomerCuratedJoin_node1783436664526)
job.commit()