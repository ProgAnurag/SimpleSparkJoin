import pyspark as ps
from pyspark.sql.functions import col

# Starting the spark session and spark context
spark = (ps.sql.SparkSession
         .builder
         .master('local')
         .appName('pyspark_example')
         .getOrCreate()
        )
sc = spark.sparkContext


#setting some spark configration to True
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set( "spark.sql.caseSensitive", "true" );

# Reading the txt file, using CSV api to infer the headers into dataframe
df_User = spark.read.csv(r'user.txt',
                            header=True,
                            quote='"',
                            sep=",",
                            inferSchema=True)

df_Transaction = spark.read.csv(r'Transaction.txt',
                             header=True,
                             quote='"',
                             sep=",",
                             inferSchema=True)

# Collecting the rows and printing them
df_user_rows = df_User.collect()
df_Transaction_rows = df_Transaction.collect()


print("******** USER DATAFRAME **********\n")
print(df_user_rows)
print("******** TRANSACTION DATAFRAME **********\n")
print(df_Transaction_rows)


# Joining both the dataframe on common column
Joined_df = df_User.join(df_Transaction, col('UserID') == col('ID'), 'left').persist()
joined_df_res = Joined_df.collect()

# Printing the joined Dataframe
print("*Joined tables*\n")
print(joined_df_res)

# Give DF temporary SQL namespace to be able to use Spark SQL
Joined_df.createOrReplaceTempView('Joined_Table')

# Writing a simple select statement
joined_df_query = """ SELECT      *
                   FROM        Joined_Table
               """
# Printing out the output and storing in another temp view
join_1_out = spark.sql(joined_df_query)
join_1_out.createOrReplaceTempView('join_1_out')
join_1_out.show(100)

print(join_1_out)









