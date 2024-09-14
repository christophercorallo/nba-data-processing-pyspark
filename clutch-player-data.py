from pyspark.sql import SparkSession
from pyspark.sql.functions import split, when, col, lit, trim
from pyspark import SparkConf
import os
import psycopg2
import psycopg2.extras as extras

# Create a Spark session with your AWS Credentials
conf = (
    SparkConf()
    .setAppName("MY_APP") # 
    .set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.7.3")
    .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY"])
    .set("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .set("spark.sql.shuffle.partitions", "4") # default is 200 partitions which is too many for local
    .setMaster("local[*]")
)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# load csv data from S3 into a spark dataframe
df = spark.read.option("header",True).format('csv').load(os.environ["BUCKET_PATH"])

# calculate how much time is left in seconds in the quarter
# split pctimestring column into minutes and seconds and add them as columns to the dataframe
df = df.withColumn("minutes", split(df["pctimestring"], ":").getItem(0).cast("int"))
df = df.withColumn("seconds", split(df["pctimestring"], ":").getItem(1).cast("int"))

# calculate the total seconds
df = df.withColumn("seconds_left_in_game", df["minutes"] * 60 + df["seconds"])

# only select plays in the last 5 mins of the game, and where a point is scored
# scoremargin field is not null when there is a score change meaning somebody score
# refer to eventmsgtype mapping table for last boolean
df = df.where((df.period == 4) & (df.seconds_left_in_game < 300) & (df.scoremargin.isNotNull()) & (df.eventmsgtype.isin (1,3)))

# define function for 3PT regex
# must match exactly " 3PT " as if a player has 3 total points it will read "... (3 PTS) ..."
def regex_3PT(description_col):
    return trim(col(description_col)).contains(" 3PT ")

# add points scored column
df = df.withColumn(
    "points_scored",
    when(col("eventmsgtype") == 3, lit(1)) # 1PT for made free throw
    .when(
        regex_3PT("homedescription") | regex_3PT("visitordescription"), lit(3)
    )
    .otherwise(lit(2)) # every other basket will be 2PTS
)

# cast points scored to int
df = df.withColumn("points_scored", df.points_scored.cast('int'))
# group players by points scored, label column clutch_points
clutch_players = df.groupBy("player1_name").sum("points_scored").select(col("player1_name"), col("sum(points_scored)").alias("clutch_points"))
# show and sort by descending order
clutch_players.orderBy("clutch_points", ascending = False).show()

def connect_to_postgres():
    # connect to postgres database using psycopg
    conn = psycopg2.connect(database = os.environ["DATABASE"], 
                    user = os.environ["USERNAME"],
                    host= 'localhost',
                    password = os.environ["PASSWORD"],
                    port = 5432,
                    options="-c search_path=nba_data")
    return conn # return connection, NOT CURSOR

def write_data_to_postgres(con,song_df):
    # initialize cursor
    cur = con.cursor()
    # generate query to insert data
    tableName = 'clutch_points'
    dataset = [tuple(x) for x in song_df.to_numpy()]
    cols = "player_name, clutch_points"
    query  = "INSERT INTO %s(%s) VALUES %%s" % (tableName, cols)
    
    # insert data
    try:
        extras.execute_values(cur,query,dataset)
        con.commit()
    finally:
        # close cursor
        cur.close()

clutch_players.select("player1_name","clutch_points").write.format("jdbc")\
    .option("url", os.environ["MY_URL"]) \
    .option("driver", "org.postgresql.Driver").option("dbtable", os.environ["MY_TABLE"]) \
    .option("user", os.environ["USERNAME"]).option("password", os.environ["PASSWORD"]).save()

print("HOORAY")