import folium
import pandas as pd
from folium.plugins import MarkerCluster

# Load CSV file
csv_file = (
    "lakes.csv"  # Make sure to replace 'coordinates.csv' with your actual CSV file path
)
data = pd.read_csv(csv_file)
# Create a map centered around North America with a simple tileset
map_north_america = folium.Map(
    location=[48.1667, -100.1667], zoom_start=3.5, tiles="CartoDB positron"
)

# Create a MarkerCluster object
marker_cluster = MarkerCluster().add_to(map_north_america)

# Plot each coordinate as a simple circle within the marker cluster
for index, row in data.iterrows():
    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=5,  # Adjust the size of the circle here
        color="red",
        fill=True,
        fill_color="red",
    ).add_to(marker_cluster)


# Save the map to an HTML file
map_north_america.save("simple_map_of_north_america.html")
