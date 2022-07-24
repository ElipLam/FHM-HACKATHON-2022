"""
GLUE Job (legacy) v3
Extract data from s3://hackathon-batch2/heros.csv to Redshift.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import round

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ["TempDir", "JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="anltd-database", table_name="heroes_csv", transformation_ctx="datasource0"
)

df = datasource0.toDF()
df = df.withColumn("winrate_all", round(df["prowin"] / df["propick"], 4))
new_frame = DynamicFrame.fromDF(df, glueContext, "df")

applymapping1 = ApplyMapping.apply(
    frame=new_frame,
    mappings=[
        ("heroid", "int", "heroid", "int"),
        ("heroname", "string", "heroname", "string"),
        ("prowin", "string", "prowin", "string"),
        ("propick", "int", "propick", "int"),
        ("winrate_all", "double", "winrate_all", "double"),
    ],
    transformation_ctx="applymapping1",
)

selectfields2 = SelectFields.apply(
    frame=applymapping1,
    paths=["heroname", "prowin", "heroid", "propick", "winrate_all"],
    transformation_ctx="selectfields2",
)

# resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "anltd-database", table_name = "hackathondev_public_heros", transformation_ctx = "resolvechoice3")

# resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(
    frame=selectfields2,
    database="anltd-database",
    table_name="hackathondev_public_heros",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="datasink5",
)
job.commit()
