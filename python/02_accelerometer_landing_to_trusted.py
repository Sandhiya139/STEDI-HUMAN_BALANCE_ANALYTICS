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

# Script generated for node Accelerometer Landing Source
AccelerometerLandingSource_node1783435705198 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer", transformation_ctx="AccelerometerLandingSource_node1783435705198")

# Script generated for node Customer Trusted Source
CustomerTrustedSource_node1783435718599 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrustedSource_node1783435718599")

# Script generated for node Accelerometer Join
SqlQuery3177 = '''
select a.* from a 
inner join c on a.user = c.email;
'''
AccelerometerJoin_node1783435800666 = sparkSqlQuery(glueContext, query = SqlQuery3177, mapping = {"c":CustomerTrustedSource_node1783435718599, "a":AccelerometerLandingSource_node1783435705198}, transformation_ctx = "AccelerometerJoin_node1783435800666")

# Script generated for node Accelerometer Trusted Target
EvaluateDataQuality().process_rows(frame=AccelerometerJoin_node1783435800666, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783435636421", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrustedTarget_node1783436121855 = glueContext.getSink(path="s3://data-lake-stedi/trusted/accelerometer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrustedTarget_node1783436121855")
AccelerometerTrustedTarget_node1783436121855.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrustedTarget_node1783436121855.setFormat("json")
AccelerometerTrustedTarget_node1783436121855.writeFrame(AccelerometerJoin_node1783435800666)
job.commit()
