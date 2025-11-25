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
accelerometer_trusted_node1764028243059 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1764028243059")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1764028217206 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1764028217206")

# Script generated for node ml_curated_join
SqlQuery0 = '''
SELECT s.*, a.x, a.y, a.z
FROM s
JOIN a
ON s.sensorreadingtime = a.timestamp

'''
ml_curated_join_node1764028267275 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s":step_trainer_trusted_node1764028217206, "a":accelerometer_trusted_node1764028243059}, transformation_ctx = "ml_curated_join_node1764028267275")

# Script generated for node ml_curated_target
EvaluateDataQuality().process_rows(frame=ml_curated_join_node1764028267275, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1764028150891", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
ml_curated_target_node1764028309436 = glueContext.getSink(path="s3://duplechin-d609-001/curated-zone/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="ml_curated_target_node1764028309436")
ml_curated_target_node1764028309436.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
ml_curated_target_node1764028309436.setFormat("glueparquet", compression="snappy")
ml_curated_target_node1764028309436.writeFrame(ml_curated_join_node1764028267275)
job.commit()