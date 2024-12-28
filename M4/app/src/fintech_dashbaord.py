from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd

# TRANSFORMED_DATA_PATH = '../data/fintech_transformed.csv' # For local testing
TRANSFORMED_DATA_PATH = "/opt/airflow/data/fintech_transformed.csv"
df = pd.read_csv(TRANSFORMED_DATA_PATH)

# ------------------- Q1 -------------------
sorted_grades = sorted(df['grade'].unique())
fig = px.violin(
    df,
    x='grade',
    y='loan_amount_sqrt_normalized',
    title="Loan Amount Distribution by Letter Grades",
    labels={'grade': 'Grade',
            'loan_amount_sqrt_normalized': 'Loan Amount (normalized)'},
    color='grade',
    box=True,
    points="all",
    category_orders={'grade': sorted_grades}
)
fig.update_traces(meanline_visible=True)

# ------------------- Q3 -------------------
df['issue_date'] = pd.to_datetime(df['issue_date'])
df['year'] = df['issue_date'].dt.year
df['month'] = df['issue_date'].dt.month_name()
years = sorted(df['year'].unique())

# ------------------- Q4 -------------------
loan_amount_states = df.groupby(['state_name', 'state'])[
    'loan_amount'].mean().sort_values(ascending=False)
loan_amount_states = loan_amount_states.reset_index()
loan_amount_states['loan_amount_mean'] = loan_amount_states['loan_amount'].round(
    2)
fig_map = px.choropleth(
    data_frame=loan_amount_states,
    locations='state',
    locationmode='USA-states',
    color='loan_amount_mean',
    color_continuous_scale='Reds',
    scope='usa',
    title='Average Loan Amount by State',
    labels={'loan_amount_mean': 'Average Loan Amount'},
    hover_name='state_name',
    hover_data={'state': False, 'loan_amount_mean': ':.0f'}
)
#  view the map with bar chart
# fig_bar_avg = px.bar(
#     loan_amount_states,
#     x='state_name',
#     y='loan_amount_mean',
#     title="Average Loan Amount by State",
#     labels={'state_name': 'State', 'loan_amount_mean': 'Average Loan Amount'},
#     # Show average loan amount with 2 decimal places
#     hover_data={'loan_amount_mean': ':.2f'},
# )

# ------------------- Q5 -------------------
# Calculate the count and percentage distribution of loan grades
loan_grade_dist = df['grade'].value_counts(
    normalize=True) * 100
loan_grade_dist_df = loan_grade_dist.reset_index()
loan_grade_dist_df.columns = ['grade', 'Percentage']
loan_grade_dist_df = loan_grade_dist_df.sort_values('grade')
fig_bar = px.bar(
    loan_grade_dist_df,
    x='grade',
    y='Percentage',
    title="Percentage Distribution of Loan Grades",
    labels={'grade': 'Loan Grade', 'Percentage': 'Percentage (%)'},
    hover_data={'Percentage': ':.2f'},  # Show percentage with 2 decimal places
)

# Initialize the app
app = Dash(__name__)

# App Layout
app.layout = html.Div([

    # Header
    html.Div([
        html.H1("Fintech Data Dashboard", className="main-title"),
        html.P("Created by: Omar Ahmed Saad (ID: 52-4509)",
               className="sub-title"),
    ], className="header"),

    # Grid Layout for Question 3 and Question 5
    html.Div([
        # Question 3: Trend of Loan Issuance Over the Months
        html.Div([
            html.H3("What is the trend of loan issuance over the months for each year?",
                    className="card-title"),
            # Dropdown for selecting years
            dcc.Dropdown(
                options=[{'label': str(year), 'value': year}
                         for year in years],
                value=years[0],  # Default to the first year
                id='year-dropdown',
                placeholder="Select a year..."
            ),

            # Line plot for loan issuance trend per month
            dcc.Graph(id='loan-issuance-trend',
                      config={"displayModeBar": True}),
        ], className="card"),
        # Question 5: Percentage Distribution of Loan Grades
        html.Div([
            html.H3("What is the percentage distribution of loan grades in the dataset?",
                    className="card-title"),
            dcc.Graph(
                id='loan-grade-bar',
                figure=fig_bar
            ),
        ], className="card"),
    ], className="grid-container"),
    # Question 1: Loan Amount Distribution by Letter Grades
    html.Div([
        html.H3("What is the distribution of loan amounts across different grades?",
                className="card-title"),
        dcc.Graph(
            id='q1-graph',
            figure=fig,  # Directly add the figure here without using callbacks
            config={"displayModeBar": False}  # Simplify the display
        ),
    ], className="card"),
        # Question 4: Average Loan Amount by State
    # Question 4: Average Loan Amount by State map
    html.Div([
        html.H3("Which states have the highest average loan amount?",
                className="card-title"),
        dcc.Graph(
            id='avg-loan-bar-chart',
            figure=fig_map,
        ),
    ], className="card"),
    # Question 2: Loan Amount vs. Annual Income
    html.Div([
        html.H3("How does the loan amount relate to annual income across states?",
                className="card-title"),

        # Dropdown for selecting states
        dcc.Dropdown(
            options=[{'label': 'All', 'value': 'all'}] + [{'label': state,
                                                           'value': state} for state in df['state_name'].unique()],
            value='all',  # Default to "All" option
            id='state-dropdown',
            placeholder="Select a state..."
        ),

        # Scatter plot for Loan Amount vs Annual Income
        dcc.Graph(id='loan-income-scatter', config={"displayModeBar": True}),
    ], className="card"),
    #  q4: Average Loan Amount by State (map & bar chart)
    # html.Div([
    #     html.H3("Q4: Which states have the highest average loan amount?",
    #             className="card-title"),
    #     html.Div([  # Choropleth Map
    #          dcc.Graph(
    #              id='choropleth-map',
    #              figure=fig_map,
    #          )
    #          ], style={'width': '50%', 'display': 'inline-block'}),

    #     html.Div([  # Bar Chart
    #         dcc.Graph(
    #             id='bar-chart',
    #             figure=fig_bar_avg,
    #         )
    #     ], style={'width': '50%', 'display': 'inline-block'}),
    #     # Flexbox for side-by-side
    # ], style={'display': 'flex', 'justify-content': 'space-between'}, className="card"),

], className="main-container")


# ------------------- Q2 -------------------
@callback(
    Output('loan-income-scatter', 'figure'),
    Input('state-dropdown', 'value'),
)
def update_scatter(state):
    # If 'all' is selected, don't filter the data
    if state == 'all':
        filtered_df = df
    else:
        filtered_df = df[df['state_name'] == state]

    # Ensure that 'loan_status_enc' is treated as categorical (string)
    filtered_df['loan_status'] = filtered_df['loan_status'].astype(str)
    colors = ['blue', 'red', 'green', 'orange', 'purple', 'yellow']
    # Create the scatter plot
    fig = px.scatter(
        filtered_df,
        x='loan_amount', y='annual_inc',
        color='loan_status',
        title=f"Loan Amount vs Annual Income - {
            state if state != 'all' else 'All States'}",
        labels={'loan_amount': 'Loan Amount',
                'annual_inc': 'Annual Income'},
        color_discrete_map={
            str(i): colors[i] for i in filtered_df['loan_status_enc'].unique()}
    )
    return fig

# ------------------- Q3 -------------------


@callback(
    Output('loan-issuance-trend', 'figure'),
    Input('year-dropdown', 'value')
)
def update_line_plot(selected_year):
    filtered_df = df[df['year'] == selected_year]

    # Group by month and count the number of loans issued
    loan_count_per_month = filtered_df.groupby(
        'month').size().reset_index(name='loan_count')

    all_months = ['January', 'February', 'March', 'April', 'May', 'June',
                  'July', 'August', 'September', 'October', 'November', 'December']

    # Merge with the grouped data, setting missing months to 0
    loan_count_per_month = loan_count_per_month.set_index(
        'month').reindex(all_months, fill_value=0).reset_index()
    loan_count_per_month.rename(columns={'index': 'month'}, inplace=True)

    # Sort
    loan_count_per_month['month'] = pd.Categorical(
        loan_count_per_month['month'], categories=all_months, ordered=True)
    loan_count_per_month = loan_count_per_month.sort_values('month')

    fig = px.line(
        loan_count_per_month,
        x='month',
        y='loan_count',
        title=f"Loan Issuance Trend in {selected_year}",
        labels={'loan_count': 'Number of Loans', 'month': 'Month'},
        markers=True
    )
    return fig


if __name__ == '__main__':
     app.run_server(debug=True, host='0.0.0.0', port=8050)