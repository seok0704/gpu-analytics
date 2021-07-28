from json import loads
from logging import exception
import time

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_table
from dash_table.DataTable import DataTable

import plotly.graph_objs as go
from dash.dependencies import Input, Output
import flask
import pandas as pd
import plotly.express as px
import psycopg2
from bs4 import BeautifulSoup
import numpy as np

# Update connection string information 
host = "gpu-dashboard.postgres.database.azure.com"
dbname = "postgres"
user = ""
password = "!"
sslmode = "require"
# Construct connection string
conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

def connect_postgreSQL(conn_string):
    
    conn = psycopg2.connect(conn_string) 
    print("successfully Connected")
    
    return conn, conn.cursor()

def run_query(conn,query):
    return pd.read_sql(query, con=conn)

get_price = '''
        SELECT p.card_id, p.datetime, p.merchant_name, p.price, g.chipset_id, c.chipset_name, g.manufacturer
        FROM card_prices p
        INNER JOIN gpu_info g ON
        g.card_id = p.card_id
        INNER JOIN chipsets c ON
        c.chipset_id = g.chipset_id
        '''


get_gpu_info = '''
        SELECT g.*, c.chipset_name
        FROM gpu_info g
        INNER JOIN chipsets c ON
        c.chipset_id = g.chipset_id
        '''

get_benchmark_info = '''
        SELECT gb.*, c.chipset_name
        FROM gpu_benchmark gb
        INNER JOIN chipsets c ON
        c.chipset_id = gb.chipset_id
        '''

get_top_6_cpu =  '''
        SELECT gb.chipset_id
        FROM gpu_benchmark gb
        INNER JOIN chipsets c ON
        c.chipset_id = gb.chipset_id
        ORDER BY gb.score desc
        LIMIT 6
        '''

conn, cursor = connect_postgreSQL(conn_string)

price_per_card = run_query(conn, get_price)
price_per_card['datetime'] = price_per_card['datetime'].apply(lambda x: time.strftime('%Y-%m-%d', time.localtime(x))) 
#Remove outliers/error
price_per_card = price_per_card[price_per_card['price']<10000]

spec_per_card = run_query(conn, get_gpu_info)
benchmark_per_chip = run_query(conn, get_benchmark_info).sort_values(by=['score'], ascending=False)

chipsets = run_query(conn, ('select * from chipsets'))

top_6 = run_query(conn, get_top_6_cpu)

server = flask.Flask(__name__)
app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP], server = server)


def side_bar():
    menu = html.Div(
        className='col-lg-2 p-3 mb-2 bg-secondary text-white text-center', 
        children=[
            html.Div(
                children=[            
                    dcc.Markdown('## GPU Dashboard'),
                    dcc.Markdown('''---''')
                ]
            ),
            html.Div(
                className='btn-group-vertical', 
                children=[
                    dcc.Link('Overview', href='/overview', className = 'btn btn-secondary'),
                    dcc.Link('Chipset Analysis', href='/chipset', className = 'btn btn-secondary'),
                ]
            ),
        ]
    )
    return menu

overview = html.Div(
    className='container-fluid',  
    children=[  
        html.Div(
		    className='row', 
            children=[
            
                side_bar(),
    
                # Plot Elements     
                html.Div(
                    className='col-lg-10 p-3 mb-2 bg-light text-dark',
                    children=[  
                    
                        #Header
                        html.Div(
                            children=[
                            dcc.Markdown('''### __Chipset Comparison Report__''', className='text-center'),
                                                    
                        ]),

                        #Top Menus
                        html.Div(
                            className='row mb-4 ml-2 mr-2',
                            children=[
                                html.Div(
                                    dcc.Dropdown(
                                    id='chipset_ids',
                                    options=[{'label': c[0], 'value': c[1]} for c in zip(chipsets.chipset_name, chipsets.chipset_id)							   
							         ],
                                    value=list(top_6['chipset_id']),
                                    multi= True), className='col-lg-12 mb-2'),
                                ]
                            ),

                        #Price History graph
                        html.Div(
                            className='row mb-4 ml-2 mr-2',
                            children=[
                                html.Div(dcc.Graph(id='update_gpu_history'), className='col-lg-8'),
                                html.Div(dcc.Graph(id='update_gpu_mrsp'), className='col-lg-4')
                            ]
                        ),

                        #MSRP Price
                        html.Div(
                            className='row mb-4 ml-2 mr-2',
                            children=[
                                html.Div(dcc.Graph(id='update_gpu_vfm'), className='col-lg-4'),
                                html.Div(dcc.Graph(id='update_gpu_score'), className='col-lg-8')
                            ]
                        ),
                            ]
                        ),                     
                    ]
                )                   
	]
)

chipset = html.Div(
    className='container-fluid',  
    children=[  
        html.Div(
		    className='row', 
            children=[
            
                side_bar(),
    
                #Chart     
                html.Div(
                    className='col-lg-10 p-3 mb-2 bg-light text-dark',
                    children=[  
                    
                        #Header
                        html.Div(
                            children=[
                            dcc.Markdown('''### __Chipset Dashboard__''', className='text-center'),
                                                    
                        ]),

                        #Dropdown list
                        html.Div(
                            className='row mb-4 ml-2 mr-2',
                            children=[
                                html.Div(
                                    dcc.Dropdown(
                                    id='chipset_id',
                                    options=[{'label': c[0], 'value': c[1]} for c in zip(chipsets.chipset_name, chipsets.chipset_id)							   
							         ],
                                    value= list(top_6['chipset_id'])[0],
                                    multi= False
                                    
                                ), className='col-lg-12 mb-2'),
                                ]
                            ),

                        #GPU Price Per Merchant
                        html.Div(
                            className='row mb-4 ml-2 mr-2',
                            children=[
                                html.Div(dcc.Graph(id='update_gpu_merchant'), className='col-lg-8'),
                                html.Div(dcc.Graph(id='update_gpu_merchant_box'), className='col-lg-4')
                            ]
                        ),

                        #Price Per Manufacturer
                        html.Div(
                            className='row mb-4 ml-2 mr-2',
                            children=[
                                html.Div(dcc.Graph(id='update_gpu_manufacturer_box'), className='col-lg-4'),
                                html.Div(dcc.Graph(id='update_gpu_manufacturer'), className='col-lg-8')
                            ]
                        ),
                        
                        #Data Table
                        html.Div(
                            className='row mb-4 ml-2 mr-2',
                            children=[
                                html.H1("List of GPU",className='ml-3' ),
                                html.Div(id='datatable_interactivity',children=[], className='col-lg-12'),
                            ]
                        ),                        

                                
                            ]
                        ),                     
                    ]
                )                   
	]
)



app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

@app.callback(
    Output(component_id='update_gpu_history', component_property='figure'),
    [Input(component_id='chipset_ids', component_property='value')]
)
def update_gpu_history(chipset_ids):

    #Filter data based on inputs    
    plots = {}
    for chipset_id in chipset_ids:
        try:
            df = price_per_card[price_per_card['chipset_id'] == chipset_id]
            name_1 = df['chipset_name'].values[0]
            data_1 = df.groupby(['datetime'])['price'].mean()		   
            plots[str(chipset_id)] = go.Scatter(name = str(name_1), x=data_1.index, y=data_1.values, \
                opacity=0.7, hoverinfo="x+y+name")
        except Exception as e:
            print("No price for chipset_id {}".format(chipset_id))
            pass

	#Plot itself
    return {
            'data':[trace for key, trace in plots.items()],
            'layout': {
                'yaxis': {'title':'Average Price (USD)'},
                'title': 'GPU Price History',
                'legend': {'orientation':'h'},
                'hoverlabel': {'namelength': 30},
                'margin': {'r': 20, 'pad': 5}
            }
    }           

@app.callback(
    Output(component_id='update_gpu_mrsp', component_property='figure'),
    [Input(component_id='chipset_ids', component_property='value')]
)
def update_gpu_mrsp(chipset_ids):

    #Filter data based on inputs    
    plots = {}
    for chipset_id in chipset_ids:
        try:
            df = benchmark_per_chip[benchmark_per_chip['chipset_id'] == chipset_id]
            x = df['chipset_name'].values[0]
            y = df['msrp_price'].values[0]		   
            plots[str(chipset_id)] = go.Bar(x = [x], y=[y], opacity=0.7, hoverinfo="x+y")

        except Exception as e:
            print("No price for chipset_id {}".format(chipset_id))
            pass
	#Plot itself
    return {
            'data':[trace for key, trace in plots.items()],
            'layout': {
                'yaxis': {'title':'Average Price (USD)'},
                'title': 'Manufacturer Suggested Retail Price',
                'showlegend': False,
                'hoverlabel': {'namelength': 30}
                }
    }           


@app.callback(
    Output(component_id='update_gpu_score', component_property='figure'),
    [Input(component_id='chipset_ids', component_property='value')]
)
def update_gpu_score(chipset_ids):

    #Filter data based on inputs    
    plots = {}
    for chipset_id in chipset_ids:
        try:
            df = benchmark_per_chip[benchmark_per_chip['chipset_id'] == chipset_id]
            x = df['chipset_name'].values[0]
            y = df['score'].values[0]		   
            plots[str(chipset_id)] = go.Bar(x = [y], y=[x], opacity=0.7, hoverinfo="x+y", orientation='h')

        except Exception as e:
            print("No price for chipset_id {}".format(chipset_id))
            pass
	#Plot itself
    return {
            'data':[trace for key, trace in plots.items()],
            'layout': {
                'title': '3DMark Graphics Score',
                'showlegend': False,
                'hoverlabel': {'namelength': 30},
                'margin' : {'l' : 200}
                }
    }           

@app.callback(
    Output(component_id='update_gpu_vfm', component_property='figure'),
    [Input(component_id='chipset_ids', component_property='value')]
)
def update_gpu_vfm(chipset_ids):

    #Filter data based on inputs    
    plots = {}
    for chipset_id in chipset_ids:
        try:
            df = benchmark_per_chip[benchmark_per_chip['chipset_id'] == chipset_id]
            x = df['chipset_name'].values[0]
            y = df['value_for_money'].values[0]		   
            plots[str(chipset_id)] = go.Bar(x = [x], y=[y], opacity=0.7, hoverinfo="x+y")

        except Exception as e:
            print("No price for chipset_id {}".format(chipset_id))
            pass

	#Plot itself
    return {
            'data':[trace for key, trace in plots.items()],
            'layout': {
                'yaxis': {'title':'Score'},
                'title': 'Value For Money',
                'showlegend': False,
                'hoverlabel': {'namelength': 30},
                }
    }           


@app.callback(
    Output(component_id='update_gpu_merchant', component_property='figure'),
    [Input(component_id='chipset_id', component_property='value')]
)
def update_gpu_merchant(chipset_id):

    #Filter data based on inputs    
    plots = {}
    try:
        df = price_per_card[price_per_card['chipset_id'] == chipset_id]
        
        for merchant in df.merchant_name.unique():
            df_filter = df[df['merchant_name']==merchant]

            name_1 = df_filter['merchant_name'].values[0]
            data_1 = df_filter.groupby(['datetime'])['price'].mean()	
            plots[str(name_1)] = go.Scatter(name = str(name_1), x=data_1.index, y=data_1.values, \
                opacity=0.7, hoverinfo="x+y+name")
    

        #Plot itself
        return {
                'data':[trace for key, trace in plots.items()],
                'layout': {
                    'yaxis': {'title':'Average Price (USD)'},
                    'title': 'GPU Price Per Merchant',
                    'legend': {'orientation':'h'},
                    'hoverlabel': {'namelength': 30},
                    'margin': {'r': 20, 'pad': 5}
                }
        }           
    except Exception as e:
        return 

@app.callback(
    Output(component_id='update_gpu_merchant_box', component_property='figure'),
    [Input(component_id='chipset_id', component_property='value')]
)
def update_gpu_merchant_box(chipset_id):

    #Filter data based on inputs    
    plots = {}
    try:

        df = price_per_card[price_per_card['chipset_id'] == chipset_id]
        
        for merchant in df.merchant_name.unique():
            df_filter = df[df['merchant_name']==merchant]

            data_1 = df_filter['price']
            plots[str(merchant)] = go.Box(name = str(merchant), y=data_1.values, \
                opacity=0.7, hoverinfo="x+y+name")
    

        #Plot itself
        return {
                'data':[trace for key, trace in plots.items()],
                'layout': {
                    'yaxis': {'title':'Average Price (USD)'},
                    'title': 'GPU Price Per Merchant',
                    'legend': {'orientation':'h'},
                    'hoverlabel': {'namelength': 30},
                    'margin': {'r': 20, 'pad': 5}
                }
        }        
    except Exception as e:
        return  


@app.callback(
    Output(component_id='update_gpu_manufacturer_box', component_property='figure'),
    [Input(component_id='chipset_id', component_property='value')]
)
def update_gpu_manufacturer_box(chipset_id):

    #Filter data based on inputs    
    plots = {}
    try:
        df = price_per_card[price_per_card['chipset_id'] == chipset_id]
        
        for manufacturer in df.manufacturer.unique():
            df_filter = df[df['manufacturer']==manufacturer]

            data_1 = df_filter['price']
            plots[str(manufacturer)] = go.Box(name = str(manufacturer), y=data_1.values, \
                opacity=0.7, hoverinfo="x+y+name")
    

        #Plot itself
        return {
                'data':[trace for key, trace in plots.items()],
                'layout': {
                    'yaxis': {'title':'Average Price (USD)'},
                    'title': 'GPU Price Per Manufacturer',
                    'legend': {'orientation':'h'},
                    'hoverlabel': {'namelength': 30},
                    'margin': {'r': 20, 'pad': 5}
                }
        }           
    except Exception as e:
        return  


@app.callback(
    Output(component_id='update_gpu_manufacturer', component_property='figure'),
    [Input(component_id='chipset_id', component_property='value')]
)
def update_gpu_manufacturer(chipset_id):
    try:
        #Filter data based on inputs    
        plots = {}

        df = price_per_card[price_per_card['chipset_id'] == chipset_id]
        
        for merchant in df.manufacturer.unique():
            df_filter = df[df['manufacturer']==merchant]

            name_1 = df_filter['manufacturer'].values[0]
            data_1 = df_filter.groupby(['datetime'])['price'].mean()	
            plots[str(name_1)] = go.Scatter(name = str(name_1), x=data_1.index, y=data_1.values, \
                opacity=0.7, hoverinfo="x+y+name")
    

        #Plot itself
        return {
                'data':[trace for key, trace in plots.items()],
                'layout': {
                    'yaxis': {'title':'Average Price (USD)'},
                    'title': 'GPU Price Per Manufacturer',
                    'legend': {'orientation':'h'},
                    'hoverlabel': {'namelength': 30},
                    'margin': {'r': 20, 'pad': 5}
                }
        }        
    except Exception as e:
        return  

@app.callback(
    Output(component_id='datatable_interactivity', component_property='children'),
    [Input(component_id='chipset_id', component_property='value')]
)
def datatable_interactivity(chipset_id):
    try:
        #Filter data based on inputs    
        df = spec_per_card[spec_per_card['chipset_id'] == chipset_id]

        df = df[['name','chipset_name','manufacturer','memory','core_clock','boost_clock','color','length','rating']]

        df.columns = ['Name','Chipset Name','Manufacturer','Memory','Core Clock','Boost Clock','Color','Length','Rating']

        df = df.replace('null',np.NaN)


        datatable = dash_table.DataTable(
            id ='datatable_interactivity',
            columns=[{"name": i, "id": i} for i in df.columns],
            data = df.to_dict('rows'),
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            selected_columns=[],
            selected_rows=[],
            page_action="native",
            page_current= 0,
            page_size= 10,

            )

        #Plot itself
        return datatable
                
    except Exception as e:
        return  




@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/' or pathname == '/overview':
        return overview			
    if pathname =='/chipset':
        return chipset
       
if __name__ == '__main__':
	app.server.run(debug=True, threaded=True)
