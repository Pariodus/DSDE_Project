import pandas as pd
import os

# Correct file path
file_path = r'2_scaping/transposed_thesis_summary.csv'  # Update this path

# Check if the file exists
if not os.path.exists(file_path):
    print("File not found. Please check the file path.")
else:
    # Load the CSV
    data = pd.read_csv(file_path)

    # Reshape the data
    reshaped_data = data.melt(
        var_name="Year",
        id_vars=["Faculty Names"],
        value_name="Count"
    ).rename(columns={"Faculty Names": "Faculty"})

    # Filter for years between 2013 and 2017
    reshaped_data["Year"] = reshaped_data["Year"].astype(int)  # Ensure 'Year' is integer
    filtered_data = reshaped_data[
        (reshaped_data["Year"] >= 2013) & (reshaped_data["Year"] <= 2017)
    ][["Year", "Faculty", "Count"]]
    

    # Save or display the filtered data
    filtered_data.to_csv('2_scaping/filter_transpose.csv', index=False)
    print(filtered_data.head())
