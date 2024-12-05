# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("FilterChulalongkornUniversity").getOrCreate()

# # Load the CSV file into a Spark DataFrame
# file_path = "3_organize/faculty.csv"  # Update with the actual path
# df = spark.read.csv(file_path, header=True, inferSchema=True)

# filter_chula = df.where(df['Bibrecord Organization'] == 'Chulalongkorn University')
# count_faculty = filter_chula.groupBy('Faculty').count().orderBy("count", ascending=False)

# joined_df = filter_chula.join(count_faculty, on="Faculty")

# result_df = joined_df.select("Year", "Bibrecord Country", "Bibrecord City", "Faculty", "count").distinct()

# output_path = "3_organize/result_faculty_counts.csv"  # Update with the desired output path
# result_df.write.csv(output_path, header=True)

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("FilterChulalongkornUniversity").getOrCreate()

# Load the CSV file into a Spark DataFrame
file_path = "3_organize/faculty.csv"  # Update with the actual path
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Filter rows for "Chulalongkorn University"
filter_chula = df.where(df['Bibrecord Organization'] == 'Chulalongkorn University')

# Count faculty occurrences and order by count
count_faculty = filter_chula.groupBy('Faculty').count().orderBy("count", ascending=False)

# Join the count_faculty DataFrame with filter_chula on the Faculty column
joined_df = filter_chula.join(count_faculty, on="Faculty")

# Select the specified columns and make the rows unique
result_df = joined_df.select("Year", "Bibrecord Country", "Bibrecord City", "Faculty", "count").distinct()
