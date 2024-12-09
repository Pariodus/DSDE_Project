import pandas as pd

# Example DataFrames
df1 = pd.read_csv('2_scraping/filter_transpose.csv')
df2 = pd.read_csv('4_data_prep(spark)/spark.csv')
# Concatenate the DataFrames

concatenated_data = pd.concat([df1, df2])
concatenated_data.dropna(inplace=True)

# Save or display the concatenated DataFrame
concatenated_data.to_csv('5_merge/complete_data.csv', index=False)
print(concatenated_data)
