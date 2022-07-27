"""
GLUE Job (legacy) v3
Extract data from s3://hackathon-batch2/total_files_v2.parquet to Redshift.
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ["TempDir", "JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
## @type: DataSource
## @args: [database = "dientc3-hackathon", table_name = "s3_total_files_v2_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="dientc3-hackathon",
    table_name="s3_total_files_v2_csv",
    transformation_ctx="datasource0",
)
## @type: ApplyMapping
## @args: [mapping = [("match_id", "long", "match_id", "long"), ("game_mode", "long", "gamemode_id", "int"), ("radiant_win", "boolean", "radiant_win", "boolean"), ("start_time", "long", "start_time", "timestamp")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        ("match_id", "long", "match_id", "bigint"),
        ("game_mode", "long", "gamemode_id", "int"),
        ("radiant_win", "boolean", "radiant_win", "boolean"),
        ("start_time", "long", "start_time", "timestamp"),
    ],
    transformation_ctx="applymapping1",
)
## @type: SelectFields
## @args: [paths = ["start_time", "radiant_win", "gamemode_id", "match_id"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(
    frame=applymapping1,
    paths=["start_time", "radiant_win", "gamemode_id", "match_id"],
    transformation_ctx="selectfields2",
)
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "anltd-database", table_name = "myredshift_dev_public_matches", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(
    frame=selectfields2,
    choice="MATCH_CATALOG",
    database="anltd-database",
    table_name="myredshift_dev_public_matches",
    transformation_ctx="resolvechoice3",
)
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(
    frame=resolvechoice3, choice="make_cols", transformation_ctx="resolvechoice4"
)
## @type: DataSink
## @args: [database = "anltd-database", table_name = "myredshift_dev_public_matches", redshift_tmp_dir = TempDir, transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(
    frame=resolvechoice4,
    database="anltd-database",
    table_name="myredshift_dev_public_matches",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="datasink5",
)
job.commit()
