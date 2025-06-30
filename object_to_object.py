from dash import html, dcc, Output, Input, State, callback, ALL, MATCH, no_update, ctx, Patch
import dash_bootstrap_components as dbc
import json
import globals
from globals import get_label_value_base
from navigation import get_table_column_options
from datasource_managment import filter_modal

o2o_div_buttons = html.Div([dbc.Button('Add an Object-to-Object Relation',
                                       id='set-o2o', style=globals.BUTTONS_STYLE),
                            dbc.Button('Enforce an Object-to-Object Relation',
                                       id='enforce-o2o', style=globals.BUTTONS_STYLE)])


@callback(Output('o2o-div', 'children'),
          Output('o2o-div', 'hidden'),
          Input('set-o2o', 'n_clicks'),
          State({'type': 'store', 'iteration': ALL}, 'data'),
          prevent_initial_call=True)
def initiate_o2o(n_clicks, iteration_data_all):
    if not n_clicks:
        return no_update

    object_set = set()

    for iteration in iteration_data_all:
        if not iteration['removed']:
            (timestamp_table, timestamp_column, timestamp_data,
             event_tables, event_columns, use_event_columns, replacement_event_label, event_labels, event_filter,
             event_attributes, object_type_label, objects_list_tables, objects_list_columns, use_object_columns,
             replacement_object_label, object_filters, object_attributes) = globals.iteration_data_to_inputs(iteration)
            for i, o in enumerate(object_type_label):
                object_set.add(o)
    object_options = list(object_set)

    o2o_form = [
        dbc.Row([
            dbc.Col([
                dbc.Label('Object Type 1:'),
                dcc.Dropdown(options=object_options, id={'type': 'existing-object-dropdown', 'object': 1},
                             multi=False),
            ]),
            dbc.Col([
                dbc.Label('Object Type 2:'),
                dcc.Dropdown(options=object_options, id={'type': 'typing-dropdown', 'label': '', 'object': 2},
                             multi=False),
                dcc.Store(id={'type': 'typing-dropdown-helper', 'label': '', 'object': 2},
                          storage_type='memory', data=''),
            ]),
            dbc.Col(dbc.Button('Add', id={'type': 'add-o2o'}, style=globals.BUTTONS_STYLE))
        ])
    ]

    return o2o_form, False


def check_valid_tree(tables):
    table_names = [t['table'] for t in tables]
    root = ''
    for tab in tables:
        if 'keypair' in tab.keys():
            pk_name, pk_table, pk_columns, fk_name, fk_table, fk_columns = globals.dict_keypair[tab['keypair']]
            if pk_table not in table_names or fk_table not in table_names:
                if not root:
                    root = tab['table']
                else:
                    return False
        else:
            if not root:
                root = tab['table']
            else:
                return False
    return True


def update_table_ids(tables, o1_columns_value, o2_columns_value):
    # since we get a valid tree we can find root and update the root sub path in each element
    root_table = tables[0]
    len_shortest_path = len(tables[0]['path'])

    for table in tables:
        if len(table['path']) < len_shortest_path:
            root_table = table
            len_shortest_path = len(table['path'])

    old_path = root_table['path']
    t_id = globals.df_tables.index[globals.df_tables['Table'] == root_table['table']].to_list()[0]
    new_path = [str(t_id)]
    root_table['path'] = new_path
    for tab in tables:
        tab['path'] = new_path + tab['path'][len(old_path):]
    for col in o1_columns_value + o2_columns_value:
        col['table-path'] = new_path + col['table-path'][len(old_path):]

    return tables, o1_columns_value, o2_columns_value


@callback(Output('o2o-div', 'children', allow_duplicate=True),
          Output({'type': 'add-o2o'}, 'n_clicks'),
          Input({'type': 'add-o2o'}, 'n_clicks'),
          State({'type': 'existing-object-dropdown', 'object': 1}, 'value'),
          State({'type': 'typing-dropdown', 'label': '', 'object': 2}, 'value'),
          State({'type': 'store', 'iteration': ALL}, 'data'),
          prevent_initial_call=True)
def add_o2o(n_clicks, object_type_1, object_type_2, iteration_data_all):
    if not n_clicks:
        return no_update

    o1_columns_value = []
    o2_columns_value = []
    tables = []
    required_tables = []
    table_candidates = []

    for iteration in iteration_data_all:  # use latest data source for default selection
        if not iteration['removed']:
            (timestamp_table, timestamp_column, timestamp_data,
             event_tables, event_columns, use_event_columns, replacement_event_label, event_labels, event_filter,
             object_attributes,
             object_type_label, objects_list_tables, objects_list_columns, use_object_columns,
             replacement_object_label, object_filters, object_attributes) = globals.iteration_data_to_inputs(iteration)
            if object_type_1 in object_type_label and object_type_2 in object_type_label:
                for i, o in enumerate(object_type_label):
                    if o == object_type_1:
                        o1_columns_value.extend([json.loads(c) for c in objects_list_columns[i]])
                        tables.extend([json.loads(t) for t in objects_list_tables[i]])
                        required_tables.extend([json.loads(c)['table'] for c in objects_list_columns[i]])
                    elif o == object_type_2:
                        o2_columns_value.extend([json.loads(c) for c in objects_list_columns[i]])
                        tables.extend([json.loads(t) for t in objects_list_tables[i]])
                        required_tables.extend([json.loads(c)['table'] for c in objects_list_columns[i]])
                # get event table candidates
                table_candidates.append(json.loads(timestamp_table))
                if event_tables:
                    # order event_tables
                    loaded_event_tables = [json.loads(t) for t in event_tables]
                    while loaded_event_tables:  # add all event tables as candidates
                        for t in loaded_event_tables:
                            pk_name, pk_table, pk_columns, fk_name, fk_table, fk_columns = globals.dict_keypair[
                                t['keypair']]
                            table_candidates_names = [t['table'] for t in table_candidates]
                            if pk_table in table_candidates_names or fk_table in table_candidates_names:
                                table_candidates.append(t)  # join exists so we can add this as valid candidate
                                loaded_event_tables.remove(t)
                if o1_columns_value and o2_columns_value:
                    break

    set_required_tables = set(required_tables)
    # assemble required tables to build tree
    while not (set_required_tables.issubset(set([t['table'] for t in tables])) and check_valid_tree(tables)):
        tables = [table_candidates.pop()] + tables
    # now we have a valid table tree in tables
    # update path identifier
    tables, o1_columns_value, o2_columns_value = update_table_ids(tables, o1_columns_value, o2_columns_value)

    table_options = []
    for t in tables:
        nr_rows = globals.df_tables['NumRows'][globals.df_tables['Table'] == t['table']].values[0]
        table_options.append({
            'label': f"{t['table']}{' with ' + ' '.join(t['info']) if any(t['info']) else ''}"
                     f"{' - ' + str(nr_rows) + ' Rows' if nr_rows else ''}",
            'value': json.dumps(t)})
    o1_column_options = [{'label': f"{c['table']}.{c['column']} - "
                                   f"Datatype: {globals.dict_table_columns[c['table']][1]}",
                          'value': json.dumps(c)} for c in o1_columns_value]
    o2_column_options = [{'label': f"{c['table']}.{c['column']} - "
                                   f"Datatype: {globals.dict_table_columns[c['table']][1]}",
                          'value': json.dumps(c)} for c in o2_columns_value]
    info = (f"{{1}} is a placeholder for the object of '{object_type_1}' object type, {{2}} for '{object_type_2}'. "
            f"Do not use placeholder to set the same qualifier for all relations.")

    o2o_form = html.Div([
        html.Div([
            dbc.Label(f"Object Type '{object_type_1}' Columns:"),
            dcc.Dropdown(options=o1_column_options, value=[json.dumps(c) for c in o1_columns_value],
                         id={'type': 'column-o2o', 'object': f"{object_type_1}1"}, multi=True, disabled=True),
            dbc.Button(
                'Set Filter',
                id={'type': 'open-o2o-filter', 'object': 1},
                disabled=False, style=globals.BUTTONS_STYLE),
            dcc.Store(id={'type': 'filter-store', 'object': 1}, storage_type='memory', data=''),
            dbc.Label('Tables Realizing Object Relationships:'),
            dcc.Dropdown(options=table_options, value=[json.dumps(t) for t in tables], id={'type': 'table-o2o'},
                         multi=True),
            dbc.Label(f"Object Type '{object_type_2}' Columns:"),
            dcc.Dropdown(options=o2_column_options, value=[json.dumps(c) for c in o2_columns_value],
                         id={'type': 'column-o2o', 'object': f"{object_type_2}2"}, multi=True, disabled=True),
            dbc.Button(
                'Set Filter',
                id={'type': 'open-o2o-filter', 'object': 2},
                disabled=False, style=globals.BUTTONS_STYLE),
            dcc.Store(id={'type': 'filter-store', 'object': 2}, storage_type='memory', data=''),
            filter_modal,
        ]),
        dbc.Row([
            dbc.Col([
                dbc.Label('Object-To-Object Relation Qualifier:'),
                dbc.Input(value=f"{{1}} - {{2}}", type='text', disabled=False,
                          id={'type': 'qualifier-label'}),
                dbc.FormText(info, color='primary')
            ])
        ]),
        dbc.Row([
            dbc.Col(dbc.Button('Cancel', id={'type': 'cancel-o2o'}, style=globals.BUTTONS_STYLE, color='danger')),
            dbc.Col(dbc.Button('Finish', id={'type': 'finish-o2o'}, style=globals.BUTTONS_STYLE))
        ])
    ])

    return o2o_form, 0


@callback(Output('o2o-div', 'children', allow_duplicate=True),
          Input({'type': 'cancel-o2o'}, 'n_clicks'),
          prevent_initial_call=True)
def cancel_o2o(n_clicks):
    if not n_clicks:
        return no_update

    return o2o_div_buttons


@callback(Output({'type': 'table-o2o'}, 'options'),
          Output({'type': 'column-o2o', 'object': ALL}, 'options'),
          Output({'type': 'column-o2o', 'object': ALL}, 'disabled'),
          Input({'type': 'table-o2o'}, 'value'),
          )  # no prevent_initial_call=True to fill initial table options
def update_o2o_form(table_values):
    updated_table_options = []
    column_options = []

    if table_values:
        for i, json_table in enumerate(table_values):
            t_options, c_options = get_table_column_options(json_table)
            updated_table_options.extend(t_options)
            column_options.extend(c_options)

            table = json.loads(json_table)
            if 'keypair' not in table:  # append root table value
                nr_rows = globals.df_tables['NumRows'][globals.df_tables['Table'] == table['table']].values[0]
                updated_table_options.append(
                    {'label': f"{table['table']}"
                              f"{' with ' + ' '.join(table['info']) if any(table['info']) else ''}"
                              f"{' - ' + str(nr_rows) + ' Rows' if nr_rows else ''}",
                     'value': json_table})
    else:
        updated_table_options = get_label_value_base('objects')

    return updated_table_options, [column_options for i in ctx.outputs_list[1]], [True if not column_options else False
                                                                                  for i in ctx.outputs_list[2]]


@callback(Output('o2o-store-div', 'children'),
          Output('o2o-div', 'children', allow_duplicate=True),
          Input({'type': 'finish-o2o'}, 'n_clicks'),
          State({'type': 'table-o2o'}, 'value'),
          State({'type': 'column-o2o', 'object': ALL}, 'value'),
          State({'type': 'qualifier-label'}, 'value'),
          State({'type': 'filter-store', 'object': 1}, 'data'),
          State({'type': 'filter-store', 'object': 2}, 'data'),
          prevent_initial_call=True)
def save_o2o(n_clicks, o2o_tables, o2o_columns, qualifier, o1_filter, o2_filter):
    if not n_clicks:
        return no_update
    o2o_id = globals.next_o2o_id
    globals.next_o2o_id += 1

    o2o_data = {'object_type_1': ctx.states_list[1][0]['id']['object'][:-1],
                'object_type_2': ctx.states_list[1][1]['id']['object'][:-1],
                'o2o_tables': o2o_tables, 'o2o_columns_1': o2o_columns[0], 'o2o_columns_2': o2o_columns[1],
                'qualifier': qualifier, 'removed': False,
                'o1_filter': o1_filter, 'o2_filter': o2_filter}

    o2o_summary = globals.o2o_dict_to_summary(o2o_data)

    o2o_div = html.Div([
        html.Hr(),
        html.Pre(o2o_summary, id={'type': 'o2o-info', 'o2o': o2o_id}),
        html.Div([
            dbc.Button('Remove', id={'type': 'o2o-remove', 'o2o': o2o_id}, n_clicks=0,
                       style=globals.BUTTONS_STYLE, color='danger'),
        ], id={'type': 'o2o-show-options', 'o2o': o2o_id}, hidden=False),
        dcc.Store(id={'type': 'o2o-store', 'o2o': o2o_id}, storage_type='memory', data=o2o_data)
    ], id={'type': 'o2o-saved', 'o2o': o2o_id})

    patched_overview = Patch()

    patched_overview.append(o2o_div)

    return patched_overview, o2o_div_buttons


@callback(Output({'type': 'o2o-store', 'o2o': MATCH}, 'data'),
          Output({'type': 'o2o-saved', 'o2o': MATCH}, 'hidden'),
          Input({'type': 'o2o-remove', 'o2o': MATCH}, 'n_clicks'),
          State({'type': 'o2o-store', 'o2o': MATCH}, 'data'),
          prevent_initial_call=True)
def remove_o2o(n_clicks, o2o_data):
    if not n_clicks:
        return no_update

    o2o_data['removed'] = True
    return o2o_data, True


@callback(Output('o2o-div', 'children', allow_duplicate=True),
          Output('o2o-div', 'hidden', allow_duplicate=True),
          Input('enforce-o2o', 'n_clicks'),
          State({'type': 'o2o-store', 'o2o': ALL}, 'data'),
          prevent_initial_call=True)
def start_enforce_relation(n_clicks, all_o2o_data):
    if not n_clicks:
        return no_update

    o2o_relations_options = []
    for o2o_data in all_o2o_data:
        if not o2o_data['removed']:
            object_type_1 = o2o_data['object_type_1']
            object_type_2 = o2o_data['object_type_2']
            o2o_relations_options.append({'label': f"{object_type_1} - {object_type_2}",
                                          'value': json.dumps([object_type_1, object_type_2])})

    o2o_div = html.Div([
        'Any object that is of the related object types is discarded if it is not related to a dominant object.',
        dbc.Row([
            dbc.Col([
                dbc.Label('Object-to-Object Relation:'),
                dcc.Dropdown(options=o2o_relations_options, id={'type': 'enforced-o2o'}, multi=False)
            ]),
            dbc.Col([
                dbc.Label('Dominant Object Type:'),
                dcc.Dropdown(options=[], id={'type': 'dominant-object'}, multi=False, disabled=True),
            ])
        ]),
        dbc.Row([
            dbc.Col(dbc.Button('Cancel', id={'type': 'cancel-o2o'}, style=globals.BUTTONS_STYLE, color='danger')),
            dbc.Col(dbc.Button('Finish', id={'type': 'finish-enforce-o2o'}, style=globals.BUTTONS_STYLE))
        ])
    ])

    return o2o_div, False


@callback(Output({'type': 'dominant-object'}, 'options'),
          Output({'type': 'dominant-object'}, 'disabled'),
          Input({'type': 'enforced-o2o'}, 'value'),
          prevent_initial_call=True)
def fill_dominant(values):
    options = [val for val in json.loads(values)]
    return options, False


@callback(Output('o2o-enforce-store-div', 'children'),
          Output('o2o-div', 'children', allow_duplicate=True),
          Input({'type': 'finish-enforce-o2o'}, 'n_clicks'),
          State({'type': 'enforced-o2o'}, 'value'),
          State({'type': 'dominant-object'}, 'value'),
          # State('o2o-enforce-store-div', 'children'),
          prevent_initial_call=True)
def store_enforce_o2o(n_clicks, enforced_o2o, dominant_object):
    if not n_clicks:
        return no_update

    o2o_id = globals.next_o2o_id
    globals.next_o2o_id += 1

    # load json here since in store object dict of list is valid
    dict_enforced_o2o = {'enforced_o2o': json.loads(enforced_o2o), 'dominant_object': dominant_object, 'removed': False}

    enforced_o2o_summary = globals.enforced_o2o_dict_to_summary(dict_enforced_o2o)

    o2o_div = html.Div([
        html.Hr(),
        html.Pre(enforced_o2o_summary, id={'type': 'o2o-info', 'o2o': o2o_id}),
        html.Div([
            dbc.Button('Remove', id={'type': 'o2o-remove', 'o2o': o2o_id}, n_clicks=0,
                       style=globals.BUTTONS_STYLE, color='danger'),
        ], id={'type': 'o2o-show-options', 'o2o': o2o_id}, hidden=False),
        dcc.Store(id={'type': 'o2o-enforce-store', 'o2o': o2o_id}, storage_type='memory', data=dict_enforced_o2o)
    ], id={'type': 'o2o-saved', 'o2o': o2o_id})

    patched_enforce = Patch()

    patched_enforce.append(o2o_div)

    return patched_enforce, o2o_div_buttons
