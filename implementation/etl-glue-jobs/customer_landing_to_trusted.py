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

# Script generated for node customer_landing
customer_landing_node1783398948404 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="customerlanding", transformation_ctx="customer_landing_node1783398948404")

# Script generated for node SQL Query
SqlQuery2643 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
SQLQuery_node1783398962553 = sparkSqlQuery(glueContext, query = SqlQuery2643, mapping = {"myDataSource":customer_landing_node1783398948404}, transformation_ctx = "SQLQuery_node1783398962553")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783398962553, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783398940045", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1783398968516 = glueContext.getSink(path="s3://stedi-human-data-analytics-bucket/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1783398968516")
customer_trusted_node1783398968516.setCatalogInfo(catalogDatabase="stedi2",catalogTableName="customer_trusted")
customer_trusted_node1783398968516.setFormat("json")
customer_trusted_node1783398968516.writeFrame(SQLQuery_node1783398962553)
job.commit()