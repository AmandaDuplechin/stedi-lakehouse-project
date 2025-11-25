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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1764026181758 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1764026181758")

# Script generated for node customer_trusted
customer_trusted_node1764026153950 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1764026153950")

# Script generated for node customer_curated_join
SqlQuery0 = '''
SELECT DISTINCT c.*
FROM c
JOIN a
ON c.email = a.user

'''
customer_curated_join_node1764026207311 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":accelerometer_trusted_node1764026181758, "c":customer_trusted_node1764026153950}, transformation_ctx = "customer_curated_join_node1764026207311")

# Script generated for node customer_curated_target
EvaluateDataQuality().process_rows(frame=customer_curated_join_node1764026207311, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1764026116372", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_target_node1764026256065 = glueContext.getSink(path="s3://duplechin-d609-001/curated-zone/customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_target_node1764026256065")
customer_curated_target_node1764026256065.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_target_node1764026256065.setFormat("glueparquet", compression="snappy")
customer_curated_target_node1764026256065.writeFrame(customer_curated_join_node1764026207311)
job.commit()