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
accelerometer_landing_node1783399380325 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="accelerometerlanding", transformation_ctx="accelerometer_landing_node1783399380325")

# Script generated for node customer_trusted
customer_trusted_node1783399382451 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="customer_trusted", transformation_ctx="customer_trusted_node1783399382451")

# Script generated for node SQL Query
SqlQuery2940 = '''
select myDataSource.* from myDataSource
join c_trusted on myDataSource.user = c_trusted.email
'''
SQLQuery_node1783399422496 = sparkSqlQuery(glueContext, query = SqlQuery2940, mapping = {"myDataSource":accelerometer_landing_node1783399380325, "c_trusted":customer_trusted_node1783399382451}, transformation_ctx = "SQLQuery_node1783399422496")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783399422496, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783398940045", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1783399531639 = glueContext.getSink(path="s3://stedi-human-data-analytics-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1783399531639")
customer_trusted_node1783399531639.setCatalogInfo(catalogDatabase="stedi2",catalogTableName="accelerometer_trusted")
customer_trusted_node1783399531639.setFormat("json")
customer_trusted_node1783399531639.writeFrame(SQLQuery_node1783399422496)
job.commit()