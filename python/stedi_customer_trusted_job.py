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

# Script generated for node Customer Landing Source
CustomerLandingSource_node1783433801578 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer", transformation_ctx="CustomerLandingSource_node1783433801578")

# Script generated for node Research Consent Filter
SqlQuery3267 = '''
select * from customer 
where sharewithresearchasofdate is not null;

'''
ResearchConsentFilter_node1783433845013 = sparkSqlQuery(glueContext, query = SqlQuery3267, mapping = {"customer":CustomerLandingSource_node1783433801578}, transformation_ctx = "ResearchConsentFilter_node1783433845013")

# Script generated for node Customer Trsuted Target
EvaluateDataQuality().process_rows(frame=ResearchConsentFilter_node1783433845013, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783433728206", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrsutedTarget_node1783433889358 = glueContext.getSink(path="s3://data-lake-stedi/trusted/customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrsutedTarget_node1783433889358")
CustomerTrsutedTarget_node1783433889358.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrsutedTarget_node1783433889358.setFormat("json")
CustomerTrsutedTarget_node1783433889358.writeFrame(ResearchConsentFilter_node1783433845013)
job.commit()