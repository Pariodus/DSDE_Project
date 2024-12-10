import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns
import numpy as np
from sklearn.ensemble import RandomForestRegressor


st.set_page_config(page_title="CU Thesis 2013-2023", layout="wide")
st.title('CU Thesis 2013-2023')

# Load and prepare data
@st.cache_data
def load_data():
    data = pd.read_csv('5_merge/data_locate.csv')
    return data

# Load data
data = load_data()
data = data.sort_values(by=["Year","Faculty"])
# Sidebar filters
st.sidebar.header('Filters')

fauculty_option = ['All Faculty']
fauculty_option.extend(data['Faculty'].tolist())

selected_faculty = st.sidebar.selectbox("Select Faculty", options=fauculty_option)
year = st.sidebar.selectbox("Select Year", options=['All Year', 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023])


# Filter data based on selected faculty and year
filtered_data = data.sort_values(by=["Year","Faculty"])

# Filter by faculty if one is selected
if selected_faculty != 'All Faculty':
    filtered_data = filtered_data[filtered_data['Faculty'] == selected_faculty]

# Filter by year if a specific year is selected
if year != 'All Year':
    filtered_data = filtered_data[filtered_data['Year'] == year]

# Group by Faculty and Year, summing the 'Count' for each group
faculty_year_counts = filtered_data.groupby(['Year', 'Faculty'], as_index=False)['Count'].sum()

faculty_year_counts = faculty_year_counts.sort_values(by=["Year","Faculty"])

# Map style selection
map_style = st.sidebar.selectbox(
    'Select Base Map Style',
    options=['Dark', 'Light', 'Road', 'Satellite'],
    index=0
)

# Define map style dictionary
MAP_STYLES = {
    'Dark': 'mapbox://styles/mapbox/dark-v10',
    'Light': 'mapbox://styles/mapbox/light-v10',
    'Road': 'mapbox://styles/mapbox/streets-v11',
    'Satellite': 'mapbox://styles/mapbox/satellite-v9'
}

# Price Distribution
st.header('Thesis Heatmap in CU')

# Create cluster layer
# Define a layer to display on a map
heatmap_layer = pdk.Layer(
    "HeatmapLayer",
    data,
    pickable=True,
    get_position=['longitude', 'latitude'],
)

# Set the viewport location
view_state = pdk.ViewState(
    longitude=data['longitude'].mean(),
    latitude=data['latitude'].mean(),
    zoom=14.5,
    pitch=0
)


# Render
render_cluster = pdk.Deck(layers=[heatmap_layer], initial_view_state=view_state, map_style=MAP_STYLES[map_style], tooltip={"text": "{Faculty}\n{Count}"})

# Create and display the map
st.pydeck_chart(render_cluster)



col1, col2 = st.columns(2)
# Create the histogram plot
with col1:
    fig_bar = px.bar(
        faculty_year_counts,  
        x='Faculty',    
        y='Count',        
        color='Year',
        # title=f'Distribution of Listings for {selected_faculty} in {year}' if selected_faculty != 'All Faculty' and year != 'All Year' 
        #     else f'Distribution of Listings for {selected_faculty if selected_faculty != "All Faculty" else "All Faculties"} in {year if year != "All Year" else "All Years"}',
        labels={'Faculty': 'Faculty', 'Count': 'Number of Listings', 'Year': 'Year'},
        height=700
    )
    st.write(f'### Distribution of Listings for {selected_faculty} in {year}' if selected_faculty != 'All Faculty' and year != 'All Year' 
            else f'### Distribution of Listings for {selected_faculty if selected_faculty != "All Faculty" else "All Faculties"} in {year if year != "All Year" else "All Years"}')
    # Show the plot in Streamlit
    st.plotly_chart(fig_bar)
with col2:
    # cluster_stats = df.groupby('Cluster', observed=True)[feature_names].agg(['mean', 'std']).round(2)
    # Group by Faculty and Year, summing the 'Count' for each group
    filtered_data['Year'] = data['Year'].replace(',', '', regex=True).astype(float).astype(int).astype(str)
    faculty_year_counts = filtered_data.groupby(['Faculty', 'Year'], as_index=False)['Count'].sum()

    # Show grouped data in a table
    st.write(f"### Grouped Data by Faculty and Year (Sum of Counts)")
    st.dataframe(faculty_year_counts)


# Hotspot Analysis
st.header('List of Thesis numbers in Years')
fig_eiei = px.line(
    data,  # Use the grouped data with faculty counts
    x='Year',
    y='Count',      # x-axis will be the Faculty# y-axis will be the count of listings per faculty
    color='Faculty',
    # title=f'Distribution of Listings for {selected_faculty} in {year}' if selected_faculty != 'All Faculty' and year != 'All Year' 
    #     else f'Distribution of Listings for {selected_faculty if selected_faculty != "All Faculty" else "All Faculties"} in {year if year != "All Year" else "All Years"}',
    labels={'Count': 'Number of Listings','Year': 'Year','Faculty': 'Faculty'},
    height=700
)

# Show the plot in Streamlit
st.plotly_chart(fig_eiei)

st.header('Random Forest Prediction of Thesis Counts')
data_summary = data.groupby('Year')['Count'].sum().reset_index()

# Convert Year back to integers for prediction
data_summary['Year'] = data_summary['Year'].astype(int)

X = data_summary['Year'].values.reshape(-1, 1)
y = data_summary['Count'].values

rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
rf_model.fit(X, y)

future_years = np.arange(data_summary['Year'].max() + 1, data_summary['Year'].max() + 11).reshape(-1, 1)
future_predictions = rf_model.predict(future_years)

fig, ax = plt.subplots(figsize=(10, 6))
ax.scatter(X, y, color='blue', label='Actual Data')
ax.plot(X, rf_model.predict(X), color='red', label='Random Forest Fit')
ax.scatter(future_years, future_predictions, color='green', label='Predicted Data')
ax.set_xlabel('Year')
ax.set_ylabel('Total Count of Theses')
ax.set_title('Random Forest: Thesis Count Prediction')
ax.legend()
ax.grid(True)
plt.tight_layout()

st.pyplot(fig)

future_data = pd.DataFrame({
    'Year': future_years.flatten(),
    'Predicted Count': future_predictions
})


# Heatmap of Thesis Numbers
st.header('Heatmap of Thesis Counts by Faculty and Year')

# Aggregate data for the heatmap
locate_data = pd.read_excel("5_merge/faculty_location.xlsx")
df = pd.merge(data, locate_data, on="Faculty")
heatmap_data = df.groupby(['Faculty', 'Year'])['Count'].sum().unstack(fill_value=0)

fig, ax = plt.subplots(figsize=(12, 8))
sns.heatmap(heatmap_data, annot=True, fmt="d", cmap="YlGnBu", linewidths=0.5, ax=ax)
ax.set_title("Heatmap of Thesis Counts by Faculty and Year")
ax.set_xlabel("Year")
ax.set_ylabel("Faculty")
plt.xticks(rotation=45)
plt.tight_layout()

st.pyplot(fig)