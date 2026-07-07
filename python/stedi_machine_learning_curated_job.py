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
AccelerometerTrustedSource_node1783437904422 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedSource_node1783437904422")

# Script generated for node StepTrainer Trusted Source
StepTrainerTrustedSource_node1783438013403 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="steptrainer_trusted", transformation_ctx="StepTrainerTrustedSource_node1783438013403")

# Script generated for node Machine Learning Join
SqlQuery3088 = '''
select a.*, t.serialnumber, t.sensorreadingtime, t.distancefromobject 
from a inner join t 
on a.timestamp = t.sensorreadingtime;
'''
MachineLearningJoin_node1783438079839 = sparkSqlQuery(glueContext, query = SqlQuery3088, mapping = {"t":StepTrainerTrustedSource_node1783438013403, "a":AccelerometerTrustedSource_node1783437904422}, transformation_ctx = "MachineLearningJoin_node1783438079839")

# Script generated for node Machine Learning Curated Target
EvaluateDataQuality().process_rows(frame=MachineLearningJoin_node1783438079839, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783435636421", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCuratedTarget_node1783438296674 = glueContext.getSink(path="s3://data-lake-stedi/curated/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCuratedTarget_node1783438296674")
MachineLearningCuratedTarget_node1783438296674.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCuratedTarget_node1783438296674.setFormat("json")
MachineLearningCuratedTarget_node1783438296674.writeFrame(MachineLearningJoin_node1783438079839)
job.commit()