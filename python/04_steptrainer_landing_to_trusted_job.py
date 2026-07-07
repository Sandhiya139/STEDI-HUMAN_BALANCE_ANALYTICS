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

# Script generated for node StepTrainer Landing Source
StepTrainerLandingSource_node1783437282546 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="steptrainer", transformation_ctx="StepTrainerLandingSource_node1783437282546")

# Script generated for node Customer Curated Source
CustomerCuratedSource_node1783437323767 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCuratedSource_node1783437323767")

# Script generated for node StepTrainer Filter
SqlQuery3309 = '''
select s.* from s inner join c on s.serialnumber = c.serialnumber;
'''
StepTrainerFilter_node1783437369909 = sparkSqlQuery(glueContext, query = SqlQuery3309, mapping = {"c":CustomerCuratedSource_node1783437323767, "s":StepTrainerLandingSource_node1783437282546}, transformation_ctx = "StepTrainerFilter_node1783437369909")

# Script generated for node StepTrainer Trusted Target
EvaluateDataQuality().process_rows(frame=StepTrainerFilter_node1783437369909, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783435636421", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrustedTarget_node1783437489101 = glueContext.getSink(path="s3://data-lake-stedi/trusted/steptrainer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrustedTarget_node1783437489101")
StepTrainerTrustedTarget_node1783437489101.setCatalogInfo(catalogDatabase="stedi",catalogTableName="steptrainer_trusted")
StepTrainerTrustedTarget_node1783437489101.setFormat("json")
StepTrainerTrustedTarget_node1783437489101.writeFrame(StepTrainerFilter_node1783437369909)
job.commit()
