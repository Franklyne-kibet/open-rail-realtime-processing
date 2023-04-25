import dashboard.dashboard as st
from cassandra.cluster import Cluster
import time

# Connect to Cassandra
cluster = Cluster(['172.19.0.5'])
session = cluster.connect('train_service')

# Define the query
query = "SELECT * FROM service_performance"

# Define the Streamlit app
st.title('Real-time data from Cassandra')

# Define a function to execute the query and return the results
@st.cache(ttl=60)
def get_data():
    rows = session.execute(query)
    return rows

# Define a function to display the data in the Streamlit UI
def show_data(data):
    for row in data:
        st.write(row)

# Continuously update the data every 5 seconds
while True:
    data = get_data()
    show_data(data)
    time.sleep(5)
