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

# Script generated for node accelerometer_landing
accelerometer_landing_node1783399915830 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="accelerometerlanding", transformation_ctx="accelerometer_landing_node1783399915830")

# Script generated for node customer_trusted
customer_trusted_node1783399917373 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="customer_trusted", transformation_ctx="customer_trusted_node1783399917373")

# Script generated for node SQL Query
SqlQuery3135 = '''
select distinct c_trusted.* from c_trusted
join myDataSource on c_trusted.email = myDataSource.user
'''
SQLQuery_node1783399960861 = sparkSqlQuery(glueContext, query = SqlQuery3135, mapping = {"myDataSource":accelerometer_landing_node1783399915830, "c_trusted":customer_trusted_node1783399917373}, transformation_ctx = "SQLQuery_node1783399960861")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783399960861, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783398940045", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783400064969 = glueContext.getSink(path="s3://stedi-human-data-analytics-bucket/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1783400064969")
AmazonS3_node1783400064969.setCatalogInfo(catalogDatabase="stedi2",catalogTableName="customer_curated")
AmazonS3_node1783400064969.setFormat("json")
AmazonS3_node1783400064969.writeFrame(SQLQuery_node1783399960861)
job.commit()