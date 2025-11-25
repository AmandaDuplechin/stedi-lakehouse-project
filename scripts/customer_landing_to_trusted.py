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

# Script generated for node customer_landing_s3
customer_landing_s3_node1764063866163 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://duplechin-d609-001/landing-zone/customer/"], "recurse": True}, transformation_ctx="customer_landing_s3_node1764063866163")

# Script generated for node filter_research_customers
SqlQuery0 = '''
SELECT *
FROM myDataSource
WHERE sharewithresearchasofdate IS NOT NULL
'''
filter_research_customers_node1764020849752 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_landing_s3_node1764063866163}, transformation_ctx = "filter_research_customers_node1764020849752")

# Script generated for node customer_trusted_target
EvaluateDataQuality().process_rows(frame=filter_research_customers_node1764020849752, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1764020659346", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_target_node1764021779899 = glueContext.getSink(path="s3://duplechin-d609-001/trusted-zone/customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_target_node1764021779899")
customer_trusted_target_node1764021779899.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customer_trusted_target_node1764021779899.setFormat("glueparquet", compression="snappy")
customer_trusted_target_node1764021779899.writeFrame(filter_research_customers_node1764020849752)
job.commit()