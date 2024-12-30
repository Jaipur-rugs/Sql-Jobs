import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import pyodbc
from datetime import datetime, timedelta
from flask import Flask, render_template_string, abort
import os

app = Flask(__name__)

# Configuration settings
app.config['PORT'] = int(os.getenv('PORT', 3002))

def get_db_connection():
    """Establish a database connection."""
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=192.168.0.41;'
        'DATABASE=JRCPL;'
        'Trusted_Connection=yes;'
    )
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        app.logger.error(f"Database connection error: {e}")
        abort(500, description="Database connection error")

def fetch_job_data():
    """Fetch job data from the database."""
    query = '''
    SELECT 
        j.job_id,
        j.name AS job_name,
        j.enabled AS job_enabled,
        s.schedule_id,
        s.name AS schedule_name,
        s.freq_type,
        s.freq_interval,
        CONVERT(VARCHAR, h.run_date, 103) AS run_date_formatted,
        STUFF(
            STUFF(
                RIGHT('000000' + CAST(h.run_time AS VARCHAR(6)), 6),
                3, 0, ':' 
            ),
            6, 0, ':' 
        ) AS run_time_formatted,
        h.run_duration,
        STUFF(
            STUFF(
                RIGHT('000000' + CAST(h.run_duration AS VARCHAR(6)), 6),
                3, 0, ':' 
            ),
            6, 0, ':' 
        ) AS run_duration_formatted,
        CASE h.run_status
            WHEN 1 THEN 'Success'
            WHEN 2 THEN 'Failure'
            WHEN 3 THEN 'Retry'
            WHEN 4 THEN 'Canceled'
            ELSE 'Unknown'
        END AS run_status_description,
        h.message,
        js.next_run_date,
        js.next_run_time
    FROM
        msdb.dbo.sysjobs j
        LEFT JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
        LEFT JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
        LEFT JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
    WHERE 
        s.freq_subday_type = 1 
        AND ((h.run_date > FORMAT(DATEADD(DAY, -2, GETDATE()), 'yyyyMMdd') AND h.run_time > FORMAT(GETDATE(), 'HHmmss')) OR h.run_date > FORMAT(DATEADD(DAY, -1, GETDATE()), 'yyyyMMdd'))
    ORDER BY
        s.schedule_id DESC, h.run_date DESC, h.run_time DESC;
    '''
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    # print(rows[0])
    conn.close()
    return columns, rows

def time_to_minutes(time_str):
    """Convert time string to minutes since midnight."""
    curr_time = datetime.now()
    hours, minutes, seconds = map(int, time_str.split(':'))
    time = hours * 60 + minutes + seconds / 60
    # print(curr_time.hour)
    if curr_time.hour <= hours:
        if time > 660:
            time = time - 1440
    return time

def time_to_seconds(duration_str):
    """Convert duration string to total seconds."""
    hours, minutes, seconds = map(int, duration_str.split(':'))
    return hours * 3600 + minutes * 60 + seconds

def generate_tick_labels(start_time, end_time, step_minutes):
    """Generate tick labels for the y-axis."""
    labels = []
    current_time = start_time
    while current_time <= end_time:
        labels.append((datetime(1900, 1, 1) + timedelta(minutes=current_time)).strftime("%H:%M"))
        current_time += step_minutes
    return labels

def time_display_hover(time):
    if time < 0:
        time = time + 1440
    hr = int(time // 60)  # Calculate hours
    min = int(time % 60)  # Calculate minutes
    return f"{hr}:{min:02}"


def determine_step_interval(current_time, last_6_hours_start):
    """Determine the step interval for tick marks based on the time range."""
    elapsed = current_time - last_6_hours_start
    if elapsed < 60:
        return 1  # Show ticks every minute
    elif elapsed < 120:
        return 5  # Show ticks every 5 minutes
    elif elapsed < 360:
        return 10  # Show ticks every 10 minutes
    else:
        return 15  # Show ticks every 15 minutes

@app.route('/')
def index():
    try:
        columns, rows = fetch_job_data()
        
        job_data = {
            'Job': [row[1] for row in rows],
            'Start': [time_to_minutes(row[8]) for row in rows],
            'Duration': [time_to_seconds(row[10]) / 60 for row in rows],  # Convert duration to minutes
            'Status': [row[11] for row in rows]
        }

        df = pd.DataFrame(job_data)
        df['End'] = df['Start'] + df['Duration']
        
        status_colors = {
            'Success': 'green',
            'Failure': 'red',
            'Retry': 'orange',
            'Canceled': 'grey',
            'Unknown': 'white'
        }
        df['Color'] = df['Status'].map(status_colors)
        
        print(df)
        # Adjust width based on duration for better visibility
        max_duration = df['Duration'].max()
        width_adjustment = 0.1  # Minimum width for better visibility
        
        fig = go.Figure()

        for status in df['Status'].unique():
            df_status = df[df['Status'] == status]
            fig.add_trace(go.Bar(
                x=df_status['Job'].apply(lambda x: x if len(x) <= 20 else x[:17] + '...'),
                y=df_status['Duration'],
                base=df_status['Start'],
                marker_color=df_status['Color'],
                text=df_status['Duration'].apply(lambda d: f'{d:.1f} min'),
                textposition='inside',
                hovertext='Job Name: ' + df_status['Job'] + '<br>Start: ' + df_status['Start'].apply(time_display_hover) + '<br>Duration: ' + df_status['Duration'].apply(lambda d: f'{d:.1f} min'),
                hoverinfo='text',
                # customdata=df_status['Job'],  # Full job names for hover
                # hovertemplate='Job Name: ' + df_status['Job'] + '<br>Start: ' + df_status['Start'].apply(time_display_hover) + '<br>Duration: ' + df_status['Duration'].apply(lambda d: f'{d:.1f} min'),
                name=status,
                width= 0.3 #[max(width_adjustment, (duration / max_duration)) for duration in df_status['Duration']]  # Adjust width
            ))

        now = datetime.now()
        current_time_in_minutes = now.hour * 60 + now.minute
        last_6_hours_start = current_time_in_minutes - 1440
        last_3_hours_start = current_time_in_minutes - 180

        step_interval = determine_step_interval(current_time_in_minutes, last_6_hours_start)
        tick_vals = list(range(last_6_hours_start, current_time_in_minutes + 1, step_interval))
        tick_text = generate_tick_labels(last_6_hours_start, current_time_in_minutes, step_interval)

        fig.update_layout(
            width=1200,
            height=600,
            template='plotly_dark',
            xaxis=dict(
                title='Job Names',
                fixedrange=True,  # Disable dragging and zooming on x-axis
                tickangle=0,
                tickmode='array',
                tickvals=df['Job'].apply(lambda x: x if len(x) <= 20 else x[:17] + '...'),
                ticktext=df['Job'].apply(lambda x: x if len(x) <= 7 else x[:7] + '...')
            ),
            yaxis=dict(
                title='Time of Day (Minutes)',
                range=[last_3_hours_start, current_time_in_minutes],  # Show only the last 6 hours
                tickmode='array',
                tickvals=tick_vals,
                ticktext=tick_text,
                fixedrange=False  # Allow zooming and panning on y-axis
            ),
            barmode='stack',
            bargap=0.2,  # Gap between bars
            dragmode = "pan",
            margin=dict(l=50, r=50, t=30, b=80),  # Adjusted margins
            autosize=False
        )



        graph_html = pio.to_html(fig, full_html=False)

        html_template = '''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
            <title>Job Status Visualization</title>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
            <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                html, body {
                    margin: 0;
                    padding: 0;
                    height: 100%;
                    width: 100%;
                    overflow: hidden;
                    touch-action: none;
                }
                .full-screen {
                    position: fixed;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    background-color: #f0f0f0;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    box-sizing: border-box;
                }
                .content {
                    text-align: center;
                    font-family: Arial, sans-serif;
                    color: #333;
                }
                .scroll-container {
                    overflow-x: auto;
                    width: 100%;
                }
            </style>
        </head>
        <body  > 
            <div class="full-screen bg-dark">
                <div class="content w-100">
                    <div class="scroll-container">
                        <div id="plotly-graph"><h1 class="text-white bg-dark" >Job Status Visualization</h1>{{ graph_html|safe }}</div>
                    </div>
                </div>
            </div>
            <script>
                document.addEventListener('wheel', function(event) {
                    if (event.ctrlKey) {
                        event.preventDefault();
                    }
                }, { passive: false });
                document.addEventListener('gesturestart', function(event) {
                    event.preventDefault();
                });
            </script>
            <script>
                document.addEventListener('DOMContentLoaded', function() {
                    var graphDiv = document.getElementById('plotly-graph');

                    var maxRange = {{ current_time_in_minutes }};
                    var minRange = {{ last_6_hours_start }};

                    function updateAxisLimits() {
                        var layout = graphDiv.layout;
                        var yAxisRange = layout.yaxis.range;

                        if (yAxisRange[0] < minRange) {
                            layout.yaxis.range[0] = minRange;
                        }
                        if (yAxisRange[1] > maxRange) {
                            layout.yaxis.range[1] = maxRange;
                        }

                        Plotly.relayout(graphDiv, layout);
                    }

                    graphDiv.on('plotly_relayout', updateAxisLimits);
                });
            </script>

        </body>
        </html>
        '''

        return render_template_string(html_template, graph_html=graph_html)
    except Exception as e:
        app.logger.error(f"Error rendering page: {e}")
        abort(500, description="Internal Server Error")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=app.config['PORT'])
