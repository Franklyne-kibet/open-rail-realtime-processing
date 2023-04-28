# Import required libraries
import dash
import psycopg2
import plotly.graph_objs as go
from dash import html
from dash import dcc
from dash.dependencies import Output, Input
from datetime import datetime,timedelta

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="172.19.0.7",
    database="train_service",
    user="root",
    password="root"
)

# Set up the Dash app layout
app = dash.Dash(__name__)
app.layout = html.Div([
    dcc.Graph(id='example-graph'),
    dcc.Interval(id='interval-component', interval=1000, n_intervals=0)
])

# Define the callback function to update the chart
@app.callback(Output('example-graph', 'figure'), [Input('interval-component', 'n_intervals')])
def update_graph(n):
    # Query the database for the latest data
    cursor = conn.cursor()
    query = "SELECT * FROM train_schedule ORDER BY sequence_number DESC LIMIT 100"
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()

    # Format the data for the chart
    actual_arrival = []
    actual_departure = []
    scheduled_arrival = []
    late_trains = [] # new variable to store the late trains
    for row in result:
        actual_arrival.append(row[3]) # changing the index to match the new sample data
        actual_departure.append(row[4]) # changing the index to match the new sample data
        scheduled_arrival.append(row[7]) # changing the index to match the new sample data
        if row[3] and row[7] and row[3] > row[7]: # if actual arrival is greater than scheduled arrival, it is late
            late_trains.append(row[0]) # append the unique_id to the late_trains list
    
    # Create the Plotly chart object
    trace1 = go.Scatter(x=actual_arrival, y=list(range(len(actual_arrival))), mode='markers', name='Actual Arrival')
    trace2 = go.Scatter(x=actual_departure, y=list(range(len(actual_departure))), mode='markers', name='Actual Departure')
    trace3 = go.Scatter(x=scheduled_arrival, y=list(range(len(scheduled_arrival))), mode='markers', name='Scheduled Arrival')
    
    # Add a trace for the late trains
    trace4 = go.Scatter(
        x=[scheduled_arrival[i] for i in range(len(actual_arrival))
        if actual_arrival[i] is not None and scheduled_arrival[i] is not None
        and actual_arrival[i] > scheduled_arrival[i]],
        y=[i for i in range(len(actual_arrival))
        if actual_arrival[i] is not None and scheduled_arrival[i] is not None
        and actual_arrival[i] > scheduled_arrival[i]],
        mode='markers',
        name='Late Arrival'
    )
    
    data = [trace1, trace2, trace3, trace4]
    
    # Adjust the x-axis range to show only the last hour of train schedules
    x_range = [datetime.now().replace(microsecond=0, second=0, minute=0, hour=0) + timedelta(hours=i) for i in range(24)]
    layout = go.Layout(title='Train Schedule', yaxis=dict(title='Train Number'), xaxis=dict(range=[x_range[-1], x_range[0]], tickformat='%I:%M %p'))
    
    chart = {'data': data, 'layout': layout}
    return chart

# Run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True)

# Close the database connection
conn.close()