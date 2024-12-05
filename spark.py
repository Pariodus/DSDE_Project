from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FilterChulalongkornUniversity").getOrCreate()

# Load the CSV file into a Spark DataFrame
file_path = "3_organize/faculty.csv"  # Update with the actual path
df = spark.read.csv(file_path, header=True, inferSchema=True)

filter_chula = df.where(df['Bibrecord Organization'] == 'Chulalongkorn University')
