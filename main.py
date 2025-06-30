import datetime

from dash import Dash, html, dcc, Output, Input, State, callback, Patch, MATCH, ALL, no_update, ctx
import dash_bootstrap_components as dbc
import json

# CALLBACK MODULES
import globals
import datasource_managment
import extraction
import labeling
import navigation
import ocpn_visualization
import startup
import object_to_object
import filter
import attributes
import reuse_object

# Initialize Dash
app = Dash(external_stylesheets=[dbc.themes.LITERA])

db_type = 'OracleEBS'
ip, port, service, user, pw  = '', '', '', '', ''
path = r"C:\Users\Test\Desktop\running-example.sqlite"
try:
    with open(f"login.json") as f_login:
        db_schema = json.load(f_login)
        db_type = db_schema['db-type']
        if db_type == 'OracleEBS':
            ip = db_schema['ip']
            port = db_schema['port']
            service = db_schema['service']
            user = db_schema['user']
            pw = db_schema['pw']
        elif db_type == 'SQLite':
            path = globals.dict_credentials['database']
except FileNotFoundError:
    print(f"No login File Available!")

store_configuration_modal = dbc.Modal([
    dbc.ModalBody('Configuration has been written to ./configuration.json!'),
    dbc.ModalFooter(dbc.Button('Close',
                               id={'type': 'close-modal', 'intend': 'store-configuration'},
                               n_clicks=0)),
], id={'type': 'modal', 'intend': 'store-configuration'}, is_open=False)

extract_ocel_modal = dbc.Modal([
    dbc.ModalBody('Object-Centric Event Log has been written to ./ocel.xml!'),
    dbc.ModalFooter(dbc.Button('Close',
                               id={'type': 'close-modal', 'intend': 'export'},
                               n_clicks=0)),
], id={'type': 'modal', 'intend': 'export'}, is_open=False)


store_ocpn_modal = dbc.Modal([
    dbc.ModalBody('Object-Centric Petri net has been written to ./ocpn.json!'),
    dbc.ModalFooter(dbc.Button('Close',
                               id={'type': 'close-modal', 'intend': 'ocpn'},
                               n_clicks=0)),
], id={'type': 'modal', 'intend': 'ocpn'}, is_open=False)

app.layout = \
    dbc.Row([
        # left side
        dbc.Col([
            html.Div([
                html.Div([
                    dbc.Row([
                        dbc.Col([
                            dbc.RadioItems(['OracleEBS', 'SQLite'], value=db_type, id='db-type', inline=False),
                            dbc.Button('Login to Database', id='login', n_clicks=0,
                                       style={"margin-bottom": "0.25rem"}, color='success')
                        ], width=2),
                        dbc.Col([
                            html.Div([
                                dbc.Input(placeholder='Enter path to SQLite database here.', type='text',
                                          id='path-sqlite', value=path),
                            ], id='div-sqlite-login', hidden=True),
                            html.Div([
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Label('Ip', html_for='db-ip'),
                                        dbc.Input(id='db-ip', type='text', value=ip)
                                    ]),
                                    dbc.Col([
                                        dbc.Label('Port', html_for='db-port'),
                                        dbc.Input(id='db-port', type='text', value=port)
                                    ]),
                                    dbc.Col([
                                        dbc.Label('Service Name', html_for='service-name'),
                                        dbc.Input(id='service-name', type='text', value=service)
                                    ]),
                                ]),
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Label('User', html_for='db-user'),
                                        dbc.Input(id='db-user', type='text', value=user)
                                    ]),
                                    dbc.Col([
                                        dbc.Label('Password', html_for='db-pw'),
                                        dbc.Input(id='db-pw', type='password', value=pw)
                                    ]),
                                ])
                            ], id='div-oracle-login', hidden=False),
                        ]),
                    ]),
                ], id='div-db-credentials'),
                html.Div([
                    dbc.Row([
                        dbc.Col(dbc.Button('Begin Schema Extraction', id='schema-from-db', n_clicks=0,
                                           style=globals.BUTTONS_STYLE), width=3),
                        dbc.Col(dbc.Button('Load Schema From File', id='schema-from-file', n_clicks=0,
                                           style=globals.BUTTONS_STYLE), width=3)
                    ], align='center')
                ], id='div-schema-extraction', hidden=True)
            ]),
            html.Div([
                dbc.Tabs([
                    dbc.Tab(label='Data Sources', tab_id='tab-data-source'),
                    dbc.Tab(label='Object-to-Object', tab_id='tab-o2o')
                ], id='tabs-mode', active_tab='tab-data-source')], id='tabs', hidden=True),
            html.Div([
                html.Div([
                    dbc.Row([
                        dbc.Col(dbc.Label('Begin with:', html_for='add-direction'), width=1),
                        dbc.Col(
                            dbc.RadioItems(['Timestamp', 'Object'], value='Timestamp', id='add-direction', inline=True)
                            , width=3),
                        dbc.Col(dbc.Button('Add an additional data source', id='add-datasource', n_clicks=0,
                                           style=globals.BUTTONS_STYLE), width=8)
                    ], justify='start', align='center'),
                    html.Div([
                        dcc.Upload(html.Div(['Drag and Drop or ', html.A('Select Configuration File')]),
                                   id='upload-configuration', style=globals.UPLOAD_STYLE)
                    ], id='div-import-configuration', hidden=False)
                ], id='div-add', hidden=True),
                html.Div([], id='div-iteration', hidden=False),
                html.Div([], id='labeling-div', hidden=True),
                html.Div([], id='iteration-overview', hidden=True)
            ], id='data-source-tabs-div'),
            html.Div([
                html.Div([
                    object_to_object.o2o_div_buttons
                ], id='o2o-div'),
                html.Div([], id='o2o-enforce-store-div'),
                html.Div([], id='o2o-store-div')
            ], id='o2o-tabs-div', hidden=True),
            html.Div([
                dbc.Button('Store Extractor Configuration', id='store-configuration', n_clicks=0,
                           style=globals.BUTTONS_STYLE, color='success'),
                store_configuration_modal
            ], id='div-store-configuration', hidden=True)
        ], id='left-side', style={"overflow": "scroll", "height": "96vh"}, width=6),
        # right side
        dbc.Col([
            # lower half
            html.Div([
                dbc.Row(
                    dbc.Col([
                        dcc.Upload(html.Div(['Drag and Drop or ', html.A('Select OCPN File')]),
                                   id='upload-data', style=globals.UPLOAD_STYLE)
                    ]), align='center', justify='center'),  # , width=4
            ], id='div-upload', hidden=False),
            dbc.Row(
                dbc.Col([
                    html.Div([
                        # dcc.Graph(id='provided-ocpn'),
                    ], id='div-provided-ocpn', hidden=False),
                ])
            ),
            html.Hr(),
            # lower half
            dbc.Row(
                dbc.Col([
                    html.Div([
                        html.Pre(id='ocpn-difference', hidden=False),
                        dbc.Row([
                            dbc.Col(dbc.Label('Number used of Events:', html_for='update-max-rows'), align='center', width=2),
                            dbc.Col(dbc.Input(id='update-max-rows', type='number', value=10000, size='sm'),
                                    align='center', width=2),
                            dbc.Col(dbc.Button('Update OCPN', id='update-ocpn', n_clicks=0, disabled=False,
                                               style=globals.BUTTONS_STYLE), width=2),
                            dbc.Col(dbc.Button('Safe OCPN', id='safe-ocpn', n_clicks=0, disabled=False,
                                               style=globals.BUTTONS_STYLE, color='secondary')),
                            store_ocpn_modal
                        ], justify='start'),
                        html.Div([
                            # dcc.Graph(id='extracted-ocpn'),
                        ], id='div-extracted-ocpn', hidden=False),
                        # html.Div([
                        dbc.Row([
                            dbc.Checklist(options=[{'label': 'Filter Timeframe:', 'value': True}], value=[], id='use-timeframe-filter', switch=True),
                            dcc.DatePickerRange(id='timeframe-filter', initial_visible_month=datetime.datetime.now().date(), disabled=True),
                            dbc.Button('Export OCEL', id='export', n_clicks=0, disabled=False,
                                       style=globals.BUTTONS_STYLE, color='success'),
                            extract_ocel_modal,
                        ])  # store extractor
                    ], id='lower-right-side', hidden=True)
                ]), align='end'),
            dcc.Store(id='screen-size', storage_type='memory', ),
            html.P(id='placeholder')
        ], id='right-side', width=6)
    ], style={"margin": "1rem 1rem"})

# Run App
if __name__ == '__main__':
    app.run(debug=True)
