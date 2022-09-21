"""
    assets/ folder created in the root directory of the project, contain CSS and Javascript files. Dash serves any file
    included in this folder.
"""
import base64
import time
import warnings
from datetime import timedelta
from io import BytesIO
from itertools import chain

import dash
import dash_bootstrap_components as dbc
import dash_cytoscape as cyto
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from dash import dash_table as dt
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
from dash.dependencies import State, ALL
from elasticsearch.exceptions import ElasticsearchWarning

from functions import extreme_dates, docs_per_periode, data_table, iterate_whole_es, docs_per_source, plot_wordcloud, \
    significant_words, data_for_map_chart, tokens_size, data_for_bubble_chart, count_articles, cytoscape_data, \
    locations_processing

start_time = time.time()

# Ignore ElasticsearchWarning
warnings.simplefilter(action='ignore', category=ElasticsearchWarning)

# Index of the Elasticsearch document used in this project
index_name = "livrons_journaux"

# Create an instance of the Dash class.
# Setting suppress_callback_exceptions to True will search for dynamically inserted elements in app.layout, which their
# ids are referenced in callbacks but are not present in the app layout at run time.
app = dash.Dash(__name__, suppress_callback_exceptions=True)
server = app.server

# ################################# CYTOSCAPE STYLESHEET ##############################################################
# Default stylesheet for cytoscape graph components (nodes and edges)
default_stylesheet = [
    {
        "selector": 'node',
        'style': {
            "opacity": 0.85,

            "label": "data(name)",
            "text-opacity": 0.85,
            "font-size": 14
        }
    },
    {
        "selector": 'edge',
        'style': {
            "curve-style": "bezier",
            "opacity": 0.85
        }
    }
]


# ################################# LIVE GRAPH DATA ###################################################################
def static_vars(**kwargs):
    """
        function to declare a static variable
    """

    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func

    return decorate


# Get data from Elasticsearch
live_graph_data = docs_per_periode(extreme_dates(index_name)[0], extreme_dates(index_name)[1], "day", index_name)
live_graph_dates = live_graph_data['date']
nb_articles = live_graph_data['nb']

# Data transmitted to live graph, updatable every 500ms
database = {
    'date': [],
    'nb': []
}


def update_database(value):
    """
        function that adds a new value to the graph data at each call
        :param value: index of a line in live_graph_dates dataframe
    """
    database['date'].append(live_graph_dates[value])
    database['nb'].append(nb_articles[value])


# ############################################## APPLICATION LAYOUT ###################################################
# Title of the application
app.title = "Article analysis"

# Define the layout property of the app object, it's a tree of dash components:  Dash HTML components provide python
# wrappers for HTML elements, and Dash Core components provide python abstractions for creating interactive user
# interfaces
app.layout = html.Div((

    # header of the app
    html.Div([
        html.H1(children="Article Analysis Dashboard", className="header-title"),
        html.P(
            children="Analyse the evolution of the number of articles published in each period of time, and their "
                     "compositions in words (persons, organizations and places) through several types of graphs.",
            className="header-description"
        ),
    ],
        className="header"
    ),

    html.Div([
        html.Div([

            # radioItems (line graph/bar graph) + Restart button
            html.Div([
                dcc.RadioItems(
                    id='menu_radioItems',
                    options=[
                        {'label': 'Line chart  ', 'value': 'Scatter'},
                        {'label': 'Bar chart', 'value': 'Bar'},
                    ],
                    value='Scatter',
                    inline=True,
                    className='menu-title'
                ),
                html.Button('Restart', id='restart_button', style={'margin-left': '1170px'}),
            ], className='row flex-display'),

            # live graph
            dcc.Interval(id='interval', interval=500),
            dcc.Graph(id='live_graph', config={'displayModeBar': False}, style={'height': '240px'}),

        ], className='create_container2', style={'width': '1510px', 'height': '250px', 'background-color': '#FFFFFF'}),
    ], className="row flex-display"),

    html.Div([
        html.Div([
            # Dropdown element for time periods
            html.Div([
                html.Div(children="Time period", className="menu-title"),
                dcc.Dropdown(
                    id="interval-time",
                    options=[
                        {"label": interval_time, "value": interval_time}
                        for interval_time in ["day", "week", "month", "year"]
                    ],
                    value="week",
                    clearable=True,
                    style={
                        'height': '40px',
                        'width': '287px'
                    },
                ),
            ], className="menu"),

            # Date range
            html.Div([
                html.Div(children="Date range", className="menu-title"),
                dcc.DatePickerRange(
                    id="date-range",
                    display_format='MMM Do, YYYY',
                    min_date_allowed=extreme_dates(index_name)[0],
                    max_date_allowed=extreme_dates(index_name)[1],
                    start_date=extreme_dates(index_name)[0],
                    end_date=extreme_dates(index_name)[1],
                    style={
                        'height': '40px',
                        'width': '287px'
                    },
                ),
            ], className="menu"),

            # Input label for search term + Submit button
            html.Div([
                html.Div(children="Search term", className="menu-title"),
                dbc.Input(
                    id="filter",
                    placeholder='Enter a word...',
                    value="",
                    type='text',
                    style={
                        'height': '40px',
                        'width': '287px'
                    },
                ),
                html.Button('OK', id='submit-val', n_clicks=0,
                            style={'margin': " 3px 0px 3px 205px", 'color': "#09142F"}),
            ], className="menu", style={'height': '100px'}),

            # Label for the number of articles
            html.Div(id='nb_articles', className='menu', style={'height': '35px', 'padding-top': '15px'}),

        ], className='create_container2',
            style={'width': '500px', 'height': '400px', 'background-color': '#FFFFFF', 'padding': '0'}),

        # Bar chart
        html.Div([
            dcc.Loading(dcc.Graph(id='bar_chart',
                                  config={'displayModeBar': 'hover'}, style={'height': '370px'}),
                        type='dot'),

        ], className='create_container2 three columns', style={'height': '400px'}),

        # Line chart
        html.Div([
            dcc.Loading(
                dcc.Graph(id='line_chart', config={'displayModeBar': 'hover'}, style={'height': '350px'}),
                type='dot'
            ),

        ], className='create_container2 three columns', style={'height': '400px'}),
    ], className="row flex-display"),

    html.Div((
        # Pie chart
        html.Div([
            dcc.Loading(
                dcc.Graph(id='pie_chart', config={'displayModeBar': 'hover'}, style={'height': '350px'}
                          ),
                type='dot'),

        ], className='create_container2 two columns', style={'height': '400px', 'width': '600px'}),

        # Data table
        html.Div([
            dt.DataTable(id='datatable',
                         columns=[{'name': i, 'id': i} for i in ["Title", "Date", "Time", "Link"]],
                         sort_action="native",
                         sort_mode="multi",
                         style_cell={'textAlign': 'left',
                                     'min-width': '90px',
                                     'backgroundColor': '#E8F1F4',
                                     'border-bottom': '0.01rem solid #808283',
                                     },
                         style_as_list_view=True,
                         style_header={
                             'textAlign': 'center',
                             'backgroundColor': '#09142F',
                             'font-weight': 'bold',
                             'color': '#FFFFFF'
                         },
                         style_data={'textOverflow': 'hidden', 'color': 'black'},
                         fixed_rows={'headers': True},
                         style_table={'max-height': '300px', 'min-width': '800px'}
                         ),
        ], className='create_container2 two columns', style={'min-width': '900px', 'min-height': '400px'})
    ), className="row flex-display"),

    html.Div((
        # Map chart
        html.Div([
            dcc.Loading(
                dcc.Graph(id='map_chart', config={'displayModeBar': 'hover'}),
                type='dot'
            ),
        ], className='create_container2 two columns', style={'width': '800px', 'height': '450px', 'padding': '0px'}),

        # Word cloud
        html.Div([
            dcc.Loading(
                html.Img(id="word_cloud"),
                type='dot'
            ),
        ], className='create_container2 two columns', style={'height': '450px', 'width': '700px',
                                                             'background-color': '#FFFFFF', 'padding-right': '0px'}),
    ), className="row flex-display"),

    # Range slider
    html.Div((
        html.Div([
            html.Div(children='Select Week(s)', className='menu-title'),
            html.Div(id='range_slider_container', children=[])
        ], className='create_container2 two columns',
            style={'width': '1510px', 'height': '150px', 'background-color': '#FFFFFF'}),
    ), className="row flex-display"),

    # Word graph
    html.Div((
        html.Div([
            dcc.Loading(
                dcc.Graph(id='bubble_chart', config={'displayModeBar': 'hover'}, style={'height': '370px'}),
                type='dot'
            ),
        ], className='create_container2 two columns', style={'width': '1510px', 'height': '400px'}),
    ), className="row flex-display"),

    # Network graph part 
    html.Div([
        html.P(children="Network graph", className="header-title", style={'font-size': '20px'})
    ], className='header', style={'height': '60px'}),

    html.Div([
        html.Div([
            html.Div([
                html.Div([
                    html.P(children="Date of a day", className="menu-title"),
                    dcc.DatePickerSingle(
                        id='date-picker-single',
                        display_format='MMM Do, YYYY',
                        min_date_allowed=extreme_dates(index_name)[0],
                        max_date_allowed=extreme_dates(index_name)[1],
                        date=extreme_dates(index_name)[1]
                    )], className="menu",
                ),

                html.Div([
                    html.P(children="Choose nodes type", className="menu-title"),
                    dcc.Dropdown(
                        id='dropdown-update-elements',
                        value=[0],
                        multi=True,
                        options=[
                            {'label': data, 'value': i}
                            for i, data in
                            enumerate(['organizations → persons', 'locations → organizations', 'persons → locations'])
                        ]
                    )], className="menu", style={'height': "175px"},
                )
            ], className='create_container2',
                style={'width': '500px', 'height': '400px', 'background-color': '#FFFFFF', 'padding': '0'}),

            html.Div([
                html.P(children="Node information", className="menu-title"),
                dcc.Markdown(id='tap-node-json-output', style={'overflow-y': 'scroll', 'height': 'calc(100% - 25px)'}),
            ], className='create_container2 three columns', style={'height': '300px'}),

            html.Div([
                html.P(children="Edge information", className="menu-title"),
                dcc.Markdown(id='tap-edge-json-output')
            ], className='create_container2 three columns', style={'height': '300px'})
        ], className='row flex-display'),

        dcc.Loading(
            cyto.Cytoscape(
                id='cytoscape',
                style={
                    'height': '95vh',
                    'width': '100%'
                },
                layout={
                    'name': 'breadthfirst'
                }
            ), type='dot'),
    ])

), id="mainContainer", style={"display": "flex", "flex-direction": "column"})


# ############################################# CALLBACKS ###########################################################
# Callbacks make the app react to user interactions; Dash's callback functions are regular python functions with an
# app.callback decorator. A callback links inputs and outputs of the app. When the input changes, a callback function
# is triggered, and the function performs predetermined operations.

# LIVE GRAPH : the graph data are unchangeable, and they are displayed one by one in real time (every 500ms), and the
# launching of the data can be restarted with the "Restart" button. The type of graph changes according to the selected
# value of RadioItems (bar or line).
@app.callback(
    Output('live_graph', 'figure'),
    Output('restart_button', 'n_clicks'),
    [
        Input('menu_radioItems', 'value'),
        Input('interval', 'n_intervals'),
        Input('restart_button', 'n_clicks'),
    ]
)
@static_vars(counter=0)  # initialize the static variable "counter" to 0
def my_callback(graph_type, n_intervals, n_clicks):
    # add the first value of index 0 to the graph database then increment counter
    update_database(my_callback.counter)
    my_callback.counter += 1

    figure = {
        'data': []
    }

    # By clicking the Restart button or by reaching the end of the original database, the graph database becomes empty
    # and the counter is set to 0
    if len(live_graph_data) == my_callback.counter or n_clicks:
        my_callback.counter = 0
        database['date'] = []
        database['nb'] = []

    # Each time data is added, the data part of the graph is created according to the requested type
    if graph_type == 'Bar':
        for x in ['nb']:
            figure['data'].append(
                go.Bar(
                    y=database[x][-40:],
                    x=database['date'][-40:],
                    marker=dict(color='#09142F'),
                    hoverinfo='text',
                ),
            )
    else:
        for x in ['nb']:
            figure['data'].append(
                go.Scatter(
                    y=database[x][-40:],
                    x=database['date'][-40:],
                    mode='markers+lines',
                    line=dict(width=3, color='#09142F'),
                    marker=dict(size=6, symbol='circle', color='#09142F'),
                    hoverinfo='text',
                ),
            )

    # The y-axis is set to the maximum value of the data field passed to it or to 200 if this is greater
    if len(database['nb']) == 0:
        maximum = 0
    else:
        maximum = max(database['nb'])

    # Layout part of the graph
    figure['layout'] = go.Layout(
        margin=dict(t=30, r=40, l=50, b=50),

        xaxis=dict(showline=False,
                   showgrid=False,
                   showticklabels=True,
                   linecolor='#808283',
                   tickfont=dict(size=11, color='#808283')
                   ),

        yaxis=dict(range=[0, max(200, maximum)],
                   color='#808283',
                   showline=False,
                   showgrid=True,
                   showticklabels=True,
                   linecolor='#808283',
                   tickfont=dict(size=11, color='#808283')
                   ),
    )
    return figure, 0


# NB ARTICLES : this callback returns the number of articles published between the 2 dates of datePickerRange
@app.callback(Output('nb_articles', 'children'),
              [Input('date-range', 'start_date')],
              [Input('date-range', 'end_date')]
              )
def update_label(start_date, end_date):
    return html.P('Number of articles : ' + str(count_articles(index_name, start_date, end_date)) + ' article(s)',
                  className='menu-title')


# BAR CHART : depends on the dates of datePickerRange and the period chosen in the Dropdown, and it returns a histogram
# that represents the number of articles published each period of time previously chosen
@app.callback(Output('bar_chart', 'figure'),
              [Input('interval-time', 'value')],
              [Input('date-range', 'start_date')],
              [Input('date-range', 'end_date')]
              )
def update_graph(interval, start_date, end_date):
    # Get data from Elasticsearch and return the graph figure composed of the data and the layout
    data = docs_per_periode(start_date, end_date, interval, index_name)
    return {
        'data': [go.Bar(
            x=data['date'],
            y=data['nb'],
            marker=dict(color='#F18B8C'),
            hoverinfo='text',
            hovertext='<b>Date</b>: ' + data['date'].astype(str) + '<br>' +
                      '<b>Number of articles </b>: ' + [f'{x}' for x in data['nb']] + '<br>'

        )],

        'layout': go.Layout(
            paper_bgcolor='#E8F1F4',
            plot_bgcolor='#E8F1F4',
            title={
                'text': 'Number of articles published each ' + str(interval),
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            titlefont={
                'color': 'black',
                'size': 13},

            margin=dict(t=30, r=40, l=50, b=50),

            xaxis=dict(color='#808283',
                       showline=False,
                       showgrid=False,
                       showticklabels=True,
                       linecolor='#808283',
                       tickfont=dict(size=11, color='#808283')
                       ),

            yaxis=dict(color='#808283',
                       showline=False,
                       showgrid=True,
                       showticklabels=True,
                       linecolor='#808283',
                       tickfont=dict(size=11, color='#808283')
                       ),
        )
    }


# LINE CHART : depends on the dates of datePickerRange and the period chosen in the Dropdown, and it returns a line
# graph that represents the number of articles published each period of time previously chosen
@app.callback(Output('line_chart', 'figure'),
              [Input('interval-time', 'value')],
              [Input('date-range', 'start_date')],
              [Input('date-range', 'end_date')]
              )
def update_graph(interval, start_date, end_date):
    # Get data from Elasticsearch and return the graph figure composed of the data and the layout
    data = docs_per_periode(start_date, end_date, interval, index_name)
    return {
        'data': [go.Scatter(
            x=data['date'],
            y=data['nb'],
            mode='markers+lines',
            line=dict(width=3, color='#3F8AAA'),
            marker=dict(size=6, symbol='circle', color='#3F8AAA'),
            hoverinfo='text',
            hovertext='<b>Date</b>: ' + data['date'].astype(str) + '<br>' +
                      '<b>Number of articles </b>: ' + [f'{x}' for x in data['nb']] + '<br>'

        )],

        'layout': go.Layout(
            paper_bgcolor='#E8F1F4',
            plot_bgcolor='#E8F1F4',
            title={
                'text': 'Number of articles published each ' + str(interval),
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            titlefont={
                'color': 'black',
                'size': 13},

            margin=dict(t=30, r=40, l=50, b=30),

            xaxis=dict(color='#808283',
                       showline=False,
                       showgrid=False,
                       showticklabels=True,
                       linecolor='#808283',
                       tickfont=dict(size=11, color='#808283')
                       ),

            yaxis=dict(color='#808283',
                       showline=False,
                       showgrid=True,
                       showticklabels=True,
                       linecolor='#808283',
                       tickfont=dict(size=11, color='#808283')
                       ),
        )

    }


# DATA TABLE: depends on the two dates of datePickerRange and the search term, and it returns in 4 columns (date of
# publication, time of publication, title of the article, and its link) a table that groups the articles published
# between the 2 dates and containing the searched term in their title.
@app.callback(
    Output('datatable', 'data'),
    Input('date-range', 'start_date'),
    Input('date-range', 'end_date'),
    Input('submit-val', 'n_clicks'),
    State('filter', 'value')
)
def display_table(start_date, end_date, n_clicks, word):
    # if no words are entered, all articles published during the requested period are returned.
    if word == "":
        body = {
            "query": {
                "bool": {
                    "must": {
                        "range": {"published": {"gte": start_date, "lte": end_date}}
                    }
                }
            },
            "sort": [
                {
                    "published": {
                        "order": "asc"
                    }
                }
            ],
            "_source": ["title", "published", "link"]
        }
    else:
        body = {"query": {
            "bool": {
                "must": [
                    {
                        "range": {"published": {"gte": start_date, "lte": end_date}}
                    }, {
                        "multi_match": {
                            "query": word,
                            "fields": [
                                "title"
                            ]
                        }
                    }
                ]
            }
        },
            "sort": [
                {
                    "published": {
                        "order": "asc"
                    }
                }
            ],
            "_source": ["title", "published", "link"]
        }

    # Then get data from Elasticsearch
    df = pd.DataFrame(iterate_whole_es(index_name, 10000, data_table, body))
    return df.to_dict('records')


# PIE CHART : depends only on the two dates of datePickerRange, and it returns a pie chart that represents the
# number/percentage of articles published for each source
@app.callback(Output('pie_chart', 'figure'),
              [Input('date-range', 'start_date')],
              [Input('date-range', 'end_date')])
def update_graph(start_date, end_date):
    # Get data from Elasticsearch and return the graph figure composed of the data and the layout
    data = docs_per_source(start_date, end_date, index_name)

    return {
        'data': [go.Pie(labels=data['sources'],
                        values=data['count'],
                        marker=dict(colors=['#0F6E9A', '#09142F', '#F18B8C', '#D2CFC7']),
                        hoverinfo='label+value+percent',
                        textinfo='percent',
                        textfont=dict(size=13),
                        hole=.7,
                        rotation=45
                        )],

        'layout': go.Layout(
            plot_bgcolor='#E8F1F4',
            paper_bgcolor='#E8F1F4',
            title={
                'text': 'Percentage of articles published for each source',
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            titlefont={
                'color': 'black',
                'size': 13},
            legend={
                'orientation': 'h',
                'xanchor': 'center', 'x': 0.5, 'y': -0.1},
        ),
    }


# MAP CHART : depends only on the two dates of datePickerRange, and it returns a map containing the places mentioned in
# the articles published between the 2 dates.
@app.callback(Output("map_chart", "figure"),
              [Input('date-range', 'start_date')],
              [Input('date-range', 'end_date')])
def update_graph(start_date, end_date):
    # Get data from Elasticsearch and return the figure of the graph composed by data and layout
    body = {"query": {
        "bool": {
            "must": [{"range": {
                "published": {
                    "gte": start_date,
                    "lte": end_date
                }}
            }]
        }
    },
        "_source": ["ner_loca_title"]}

    data = locations_processing(iterate_whole_es(index_name, 10000, data_for_map_chart, body))
    fig = px.scatter_geo(data,
                         hover_name="Location",
                         size=data["Frequency"] * 10,
                         lat="Latitude",
                         lon="Longitude",
                         hover_data=['Frequency', 'Latitude', 'Longitude'],
                         color="Frequency",
                         width=800,
                         height=450
                         )

    '''fig = px.density_mapbox(
        data,
        hover_name="Location",
        lat="Latitude",
        lon="Longitude",
        hover_data=['Frequency', 'Latitude', 'Longitude'],
        range_color=[0, 200],
        zoom=2,
        radius=50,
        opacity=.5,
        mapbox_style='open-street-map',
        width=800,
        height=450)

    fig.add_scattermapbox(lat=data["Latitude"],
                          lon=data["Longitude"],
                          hoverinfo="lat+lon",
                          marker_size=5,
                          marker_color='rgb(0, 0, 0)',
                          showlegend=False
                          )'''
    fig.update_layout(
        paper_bgcolor='#E8F1F4',
        title={
            'text': 'Locations mentioned between ' + str(start_date) + ' and ' + str(end_date),
            'y': 0.93,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        titlefont={
            'color': 'black',
            'size': 13}
    )
    return fig


# WORD CLOUD : depends only on the two dates of datePickerRange, and it returns a cloud of the most significant words
# in the articles published between the two dates.
@app.callback(Output('word_cloud', 'src'),
              [Input('word_cloud', 'id')],
              [Input('date-range', 'start_date')],
              [Input('date-range', 'end_date')])
def update_image(aa, start_date, end_date):
    # generate an image
    img = BytesIO()

    # Get data from Elasticsearch
    df = significant_words(start_date, end_date, index_name)

    # transform data into PNG
    plot_wordcloud(data=df).save(img, format='PNG')
    return 'data:image/png;base64,{}'.format(base64.b64encode(img.getvalue()).decode())


# RANGE SLIDER : Returns a rangeSlider delimited by the dates of DatePickerRange, and graduated in weeks
@app.callback(Output("range_slider_container", "children"),
              [Input('date-range', 'start_date')],
              [Input('date-range', 'end_date')],
              [State('range_slider_container', 'children')])
def update_slider(start_date, end_date, children):
    # Extract data
    start_date = pd.to_datetime(start_date).date()
    end_date = pd.to_datetime(end_date).date()

    # Calculate the number of days between the two dates
    T = (end_date - start_date).days

    # If T does not exceed the number of days in a week, we put a single step of T days
    if T >= 7:
        steps = 7
    else:
        steps = T

    # Create a list of dates
    date_list = pd.date_range(start=start_date, end=end_date)

    # Define the form and content of range slider labels
    marks = {i: {'label': date_list[i].strftime('%d.%m.%y'),
                 'style': {'writing-mode': 'vertical-rl', 'text-orientation': 'use-glyph-orientation'}}
             for i in range(0, T, steps)}

    if T % steps:
        d1 = {T: {'label': date_list[T].strftime('%d.%m.%y'),
                  'style': {'writing-mode': 'vertical-rl', 'text-orientation': 'use-glyph-orientation'}}}
        marks = dict(chain.from_iterable(d.items() for d in (marks, d1)))

    return dcc.RangeSlider(id={"type": "range_slider", "index": "myIndex"},
                           min=0,
                           max=T,
                           step=steps,
                           pushable=1,
                           allowCross=False,
                           value=[T - (steps + T % steps), T],
                           # value=[0, steps],
                           marks=marks)


# BUBBLE CHART : depends only on the values returned by rangeSlider, and it returns the first 15 most frequent words
# per day, from bottom to top in descending order of number of occurrences
@app.callback(Output("bubble_chart", "figure"),
              [Input({"type": "range_slider", "index": ALL}, 'value')],
              [Input('date-range', 'start_date')],
              [Input('date-range', 'end_date')])
def update_graph(dates, start_date, end_date):
    start_date = pd.to_datetime(start_date).date()
    end_date = pd.to_datetime(end_date).date()
    date_list = pd.date_range(start=start_date, end=end_date)

    # If no period is selected in rangeSlider, wait for the next choice
    while len(dates) == 0:
        continue

    # return the real dates selected, since the two values returned by rangeSlider are the scale number and not its
    # label content
    dates = [date_list[dates[0][0]], date_list[dates[0][1]]]

    # Get data from Elasticsearch and return the figure of the graph composed by data and layout
    body = {"query": {
        "bool": {
            "must":
                {
                    "range": {"published": {"gte": dates[0], "lte": dates[1] + timedelta(days=1)}}
                }
        }
    },
        "_source": ["published", "pos_tag_title", "pos_tag_message"]
    }

    df = tokens_size(pd.DataFrame(iterate_whole_es(index_name, 10000, data_for_bubble_chart, body)))

    return {'data': [go.Scatter(
        x=df['date'],
        y=df['rang'],
        mode='text',
        text=df['token'],
        textfont=dict(family='monospace',
                      size=df['fake_score'],
                      color='#09142F'),
        hoverinfo='text',
        hovertext='<b>Date</b>: ' + df['date'].astype(str) + '<br>' +
                  '<b>Word</b>: ' + df['token'].astype(str) + '<br>' +
                  '<b>Mentioned</b>: ' + [f'{x}' for x in df['real_score']] + ' times <br>'

    )],
        'layout': go.Layout(
            paper_bgcolor='#E8F1F4',
            plot_bgcolor='#E8F1F4',

            title={
                'text': 'Most frequent words per each day between ' + str(dates[0].date()) + ' and ' + str(
                    dates[1].date()),
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            titlefont={
                'color': 'black',
                'size': 13},

            xaxis=dict(color='#808283',
                       showline=False,
                       showgrid=False,
                       showticklabels=True,
                       linecolor='#808283',
                       tickfont=dict(size=11, color='#808283')
                       ),
            margin=dict(t=45, r=40, l=50, b=50),
            yaxis=dict(range=[0, 16],
                       color='#808283',
                       showline=False,
                       showgrid=False,
                       showticklabels=False,
                       linecolor='#808283',
                       tickfont=dict(size=11, color='#808283')
                       ),
        )
    }


# CYTOSCAPE : depends on the selected date and the nature of the nodes to display (organization and/or person and/or
# place), and it returns a network graph
@app.callback(Output('cytoscape', 'elements'),
              [Input('date-picker-single', 'date')],
              [Input('dropdown-update-elements', 'value')])
def display_data(date_value, dropdown_value):
    # if a date is selected, it returns the NERs previously saved in a csv file that match the inputs data
    if date_value is not None:
        date_value = pd.Timestamp(date_value)
        elements = cytoscape_data(str(date_value), str(date_value + timedelta(days=1)), "csv_files/NERs.csv",
                                  "csv_files/links.csv", dropdown_value)
        return elements


# Returns the information of a selected node in the graph
@app.callback(Output('tap-node-json-output', 'children'),
              [Input('cytoscape', 'tapNode')])
def display_tap_node(data):
    output = ""
    if data is None:
        output = "No node selected."
    else:
        for element in data['edgesData']:
            output = output + "* _Source node_ : **" + element['source'] + '**\n'
            output = output + "* _Target node_ : " + element['target'] + '\n'
            output = output + "> \n"
    return output


# returns the information of a selected edge in the graph, and if the source and target nodes are organizations, it
# also returns the wikipedia link to the web page of that organization
@app.callback(Output('tap-edge-json-output', 'children'),
              [Input('cytoscape', 'tapEdge')])
def display_tap_edge(data):
    if data is None:
        output = "No edge selected."
    else:
        output = "* **_Source node_ : **" + data['sourceData']['name'] + \
                 " \n * _Type_ : " + data['sourceData']['classes'] + '\n'

        if data['sourceData']['classes'] == 'organization' and not isinstance(data['sourceData']['link'], type(None)):
            output = output + " * _Wikipedia link_ : " + data['sourceData']['link'] + '\n'

        output = output + "* ** _Target node_ **: " + data['targetData']['name'] + \
                 " \n * _Type_ : " + data['targetData']['classes'] + '\n'

        if data['targetData']['classes'] == 'organization' and not isinstance(data['sourceData']['link'], type(None)):
            output = output + " * _Wikipedia link_ : " + data['targetData']['link'] + '\n'

        output = output + "* ** _They are mentioned together in_ : " + str(data['data']['weight']) + " article(s) **"
    return output


# allows to have a change of color for the followings and followers nodes at the selection
@app.callback(Output('cytoscape', 'stylesheet'),
              Input('cytoscape', 'tapNode'))
def generate_stylesheet(node):
    if not node:
        return default_stylesheet

    stylesheet = [{
        'selector': 'edge',
        'style': {
            'opacity': 0.2,
            "curve-style": "bezier",
        }
    }, {
        "selector": 'node[id = "{}"]'.format(node['data']['id']),
        "style": {
            'background-color': '#676963',
            "border-color": "#676963",
            "border-width": 2,
            "border-opacity": 1,
            "opacity": 1,

            "label": "data(name)",
            "color": "#676963",
            "text-opacity": 1,
            "font-size": 14,
            'z-index': 9999
        }
    }]

    for edge in node['edgesData']:
        if edge['source'] == node['data']['id']:
            stylesheet.append({
                "selector": 'node[id = "{}"]'.format(edge['target']),
                "style": {
                    'background-color': '#09142F',
                    'opacity': 0.9,

                    "label": "data(name)",
                    "color": "#09142F",
                    "text-opacity": 1,
                    "font-size": 14,
                    'z-index': 9999
                }
            })
            stylesheet.append({
                "selector": 'edge[id= "{}"]'.format(edge['id']),
                "style": {
                    "mid-target-arrow-color": '#09142F',
                    "mid-target-arrow-shape": "vee",
                    "line-color": '#09142F',
                    'opacity': 0.9,
                    'z-index': 5000
                }
            })

        if edge['target'] == node['data']['id']:
            stylesheet.append({
                "selector": 'node[id = "{}"]'.format(edge['source']),
                "style": {
                    'background-color': '#F18B8C',
                    'opacity': 0.9,
                    'z-index': 9999,

                    "label": "data(name)",
                    "color": "#F18B8C",
                    "text-opacity": 1,
                    "font-size": 14
                }
            })
            stylesheet.append({
                "selector": 'edge[id= "{}"]'.format(edge['id']),
                "style": {
                    "mid-target-arrow-color": '#F18B8C',
                    "mid-target-arrow-shape": "vee",
                    "line-color": '#F18B8C',
                    'opacity': 1,
                    'z-index': 5000
                }
            })

    return stylesheet


# Run the application. The debug=True parameter from app.run_server enables the hot-reloading option in the application,
# which means that the app can reload automatically once the code changed without restart the server.
if __name__ == '__main__':
    app.run_server(debug=True)

print(">>> Execution time of app.py :", time.time() - start_time)

