"""
GLUE Job (legacy) v3
Extract data from s3://hackathon-batch2/players_heros.csv to Redshift.
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
## @type: DataSource
## @args: [database = "dientc3-hackathon", table_name = "s3_players_heros_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="dientc3-hackathon",
    table_name="s3_players_heros_csv",
    transformation_ctx="datasource0",
)
df = datasource0.toDF()
df = df.withColumn("win_rate_pick", round(df["win"] / df["games"], 4))
new_frame = DynamicFrame.fromDF(df, glueContext, "df")

## @type: ApplyMapping
## @args: [mapping = [("against_games", "long", "anemy_pick_amounts", "int"), ("with_games", "long", "team_pick_amounts", "int"), ("games", "long", "pick_amounts", "int"), ("hero_id", "long", "hero_id", "int"), ("with_win", "long", "team_win_amounts", "int"), ("file", "long", "player_id", "int"), ("against_win", "long", "anemy_win_amounts", "int")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(
    frame=new_frame,
    mappings=[
        ("against_games", "long", "anemy_pick_amounts", "int"),
        ("with_games", "long", "team_pick_amounts", "int"),
        ("games", "long", "pick_amounts", "int"),
        ("hero_id", "long", "hero_id", "int"),
        ("with_win", "long", "team_win_amounts", "int"),
        ("file", "long", "player_id", "int"),
        ("against_win", "long", "anemy_win_amounts", "int"),
        ("win_rate_pick", "double", "win_rate_pick", "double"),
    ],
    transformation_ctx="applymapping1",
)
## @type: SelectFields
## @args: [paths = ["player_id", "team_pick_amounts", "anemy_win_amounts", "team_win_amounts", "hero_id", "pick_amounts", "win_rate_pick", "anemy_pick_amounts"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(
    frame=applymapping1,
    paths=[
        "player_id",
        "team_pick_amounts",
        "anemy_win_amounts",
        "team_win_amounts",
        "hero_id",
        "pick_amounts",
        "anemy_pick_amounts",
        "win_rate_pick",
    ],
    transformation_ctx="selectfields2",
)
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "anltd-database", table_name = "myredshift_dev_public_player_hero", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(
    frame=selectfields2,
    choice="MATCH_CATALOG",
    database="anltd-database",
    table_name="myredshift_dev_public_player_hero",
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
## @args: [database = "anltd-database", table_name = "myredshift_dev_public_player_hero", redshift_tmp_dir = TempDir, transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(
    frame=resolvechoice4,
    database="anltd-database",
    table_name="myredshift_dev_public_player_hero",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="datasink5",
)
job.commit()
