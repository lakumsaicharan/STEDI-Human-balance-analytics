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

# Script generated for node customer_curated
customer_curated_node1783400696934 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="customer_curated", transformation_ctx="customer_curated_node1783400696934")

# Script generated for node step_trainer_landing
step_trainer_landing_node1783400696008 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="step_trainerlanding", transformation_ctx="step_trainer_landing_node1783400696008")

# Script generated for node SQL Query
SqlQuery2933 = '''
select myDataSource.* from myDataSource
join c_curated on myDataSource.serialnumber = c_curated.serialnumber
'''
SQLQuery_node1783400743281 = sparkSqlQuery(glueContext, query = SqlQuery2933, mapping = {"myDataSource":step_trainer_landing_node1783400696008, "c_curated":customer_curated_node1783400696934}, transformation_ctx = "SQLQuery_node1783400743281")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783400743281, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783398940045", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1783400856477 = glueContext.getSink(path="s3://stedi-human-data-analytics-bucket/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1783400856477")
step_trainer_trusted_node1783400856477.setCatalogInfo(catalogDatabase="stedi2",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1783400856477.setFormat("json")
step_trainer_trusted_node1783400856477.writeFrame(SQLQuery_node1783400743281)
job.commit()
