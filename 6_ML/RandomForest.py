import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import numpy as np
import matplotlib.pyplot as plt

# Specify the correct file path to your CSV file
# file_path = r'5_merge/complete_data.csv'  # Replace with your actual file path
file_path = r'4_data_prep(spark)/spark.csv'
data = pd.read_csv(file_path)

# Summarize the data by year
data_summary = data.groupby('Year')['Count'].sum().reset_index()

# Prepare data for Random Forest Regression
X = data_summary['Year'].values.reshape(-1, 1)
y = data_summary['Count'].values

# Fit a Random Forest Regressor model
rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
rf_model.fit(X, y)

# Predict future values
future_years = np.arange(data_summary['Year'].max() + 1, data_summary['Year'].max() + 11).reshape(-1, 1)
future_predictions = rf_model.predict(future_years)

# Visualize the results
plt.figure(figsize=(10, 6))
plt.scatter(X, y, color='blue', label='Actual Data')
plt.plot(X, rf_model.predict(X), color='red', label='Random Forest Fit')
plt.scatter(future_years, future_predictions, color='green', label='Predicted Data')
plt.xlabel('Year')
plt.ylabel('Total Count of Theses')
plt.title('Random Forest: Thesis Count Prediction')
plt.legend()
plt.grid(True)
plt.show()

# Combine the predictions with years for display
future_data = pd.DataFrame({
    'Year': future_years.flatten(),
    'Predicted Count': future_predictions
})

# Display the predictions
print(future_data)