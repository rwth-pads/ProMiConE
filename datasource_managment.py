from dash import html, dcc, Output, Input, State, callback, Patch, MATCH, ALL, no_update, ctx
import dash_bootstrap_components as dbc
import json
import base64

import globals
from globals import get_label_value_base
from navigation import get_table_column_options

existing_object_modal = dbc.Modal([
    dbc.ModalTitle('Select previously used Object Type.'),
    dbc.ModalHeader(
        dcc.Dropdown(options=[], id={'type': 'existing-object-modal'}, multi=False, style={'width': '20em'})),
    dbc.ModalBody('', id={'type': 'info-object-modal'}),
    dbc.ModalFooter(dbc.Button('Confirm',
                               id={'type': 'confirm-object-modal'},
                               n_clicks=0, disabled=True)),
], id={'type': 'object-modal'}, is_open=False, size='xl')

filter_modal = dbc.Modal([
    dbc.ModalHeader(dbc.ModalTitle('Setting Filters:'), ),
    dbc.ModalBody(id={'type': 'filter-modal-body'}),
    dbc.ModalFooter(id={'type': 'filter-modal-footer'}),
], id={'type': 'filter-modal'}, is_open=False, fullscreen=True)

attributes_modal = dbc.Modal([
    dbc.ModalHeader(dbc.ModalTitle('Selecting Attribute Columns:'), ),
    dbc.ModalBody([
    ], id={'type': 'attribute-body'}),
    dbc.ModalFooter([
    ], id={'type': 'attribute-footer'}),
], id={'type': 'attribute-modal'}, is_open=False, size='xl')

timestamp_modal = dbc.Modal([
    dbc.ModalHeader(dbc.ModalTitle('Provide an SQL Statement:'), ),
    dbc.ModalBody([
    ], id={'type': 'timestamp-body'}),
    dbc.ModalFooter([
    ], id={'type': 'timestamp-footer'}),
], id={'type': 'timestamp-modal'}, is_open=False, size='xl')


def get_object_form(object_id, table_options=None):
    is_filled = False
    if table_options is None or not table_options:
        table_options = []
    else:
        is_filled = True

    is_first = False
    if object_id == 0:
        is_first = True

    object_form = \
        html.Div([
            dbc.Row([
                dbc.Col(html.Div(f"Identify Object {object_id}"), width='auto'),
                dbc.Col(dbc.Input(placeholder='Set label for Object Type',
                                  id={'type': 'object-label', 'subject': 'objects', 'index': object_id},
                                  type='text', disabled=(is_first and not is_filled)))
            ], align='center'),
            dcc.Dropdown(options=table_options,
                         id={'type': 'tables', 'subject': 'objects',
                             'index': object_id},
                         value=[],
                         multi=True,
                         disabled=(is_first and not is_filled)),
            dbc.Row([
                dbc.Col(
                    dbc.RadioItems(options=[{'label': 'All columns contain the same object (not object type)',
                                             'value': False, 'disabled': (is_first and not is_filled)},
                                            {'label': 'Select columns:',
                                             'value': True, 'disabled': (is_first and not is_filled)}],
                                   value=True,
                                   id={'type': 'radio-columns', 'subject': 'objects',
                                       'index': object_id}),
                    width=6
                ),
                dbc.Col(
                    dbc.Input(placeholder='Object Label',
                              id={'type': 'single-label', 'subject': 'objects', 'index': object_id},
                              type='text', disabled=True)
                )
            ]),
            dcc.Dropdown(options=[],
                         id={'type': 'columns', 'subject': 'objects',
                             'index': object_id},
                         value=[],
                         multi=True,
                         disabled=is_first),
            dbc.Button(
                'Set Filter',
                id={'type': 'open-filter', 'subject': 'objects', 'index': object_id},
                disabled=is_first, style=globals.BUTTONS_STYLE),
            dcc.Store(id={'type': 'filter-store', 'subject': 'objects', 'index': object_id}, storage_type='memory',
                      data=''),
            dbc.Button(
                'Add Attributes',
                id={'type': 'open-attributes', 'subject': 'objects', 'index': object_id},
                disabled=is_first, style=globals.BUTTONS_STYLE),
            dcc.Store(id={'type': 'attributes-store', 'subject': 'objects', 'index': object_id}, storage_type='memory',
                      data=''),
            dbc.Button(f"Confirm Object {object_id} Columns",
                       id={'type': 'confirm', 'subject': 'objects',
                           'index': object_id},
                       disabled=True, style=globals.BUTTONS_STYLE),
        ], id={'type': 'object-type', 'index': object_id})

    return object_form


def get_object_form_filled(object_id, object_type_label, table_options, table_value, column_options, column_value,
                           use_columns, object_label, object_filters, object_attributes):
    object_form = \
        html.Div([
            dbc.Row([
                dbc.Col(html.Div(f"Identify Object {object_id}"), width='auto'),
                dbc.Col(
                    dbc.Input(placeholder='Set label for Object Type',
                              id={'type': 'object-label', 'subject': 'objects', 'index': object_id},
                              value=object_type_label, type='text', disabled=False)
                )
            ], align='center'),
            dcc.Dropdown(options=table_options,
                         id={'type': 'tables', 'subject': 'objects',
                             'index': object_id},
                         value=table_value,
                         multi=True,
                         disabled=False),
            dbc.Row([
                dbc.Col(dbc.RadioItems(options=[{'label': 'All columns contain the same object (not object type)',
                                                 'value': False, 'disabled': False},
                                                {'label': 'Select columns:',
                                                 'value': True, 'disabled': False}],
                                       value=use_columns,
                                       id={'type': 'radio-columns', 'subject': 'objects',
                                           'index': object_id}),
                        width=6
                        ),
                dbc.Col(dbc.Input(placeholder='Object Label',
                                  id={'type': 'single-label', 'subject': 'objects', 'index': object_id},
                                  value=object_label, type='text', disabled=use_columns))
            ]),
            dcc.Dropdown(options=column_options,
                         id={'type': 'columns', 'subject': 'objects',
                             'index': object_id},
                         value=column_value,
                         multi=True,
                         disabled=False),
            dbc.Button(
                'Set Filter',
                id={'type': 'open-filter', 'subject': 'objects', 'index': object_id},
                disabled=False, style=globals.BUTTONS_STYLE),
            dcc.Store(id={'type': 'filter-store', 'subject': 'objects', 'index': object_id}, storage_type='memory',
                      data=object_filters),
            dbc.Button(
                'Add Attributes',
                id={'type': 'open-attributes', 'subject': 'objects', 'index': object_id},
                disabled=False, style=globals.BUTTONS_STYLE),
            dcc.Store(id={'type': 'attributes-store', 'subject': 'objects', 'index': object_id}, storage_type='memory',
                      data=object_attributes),
            dbc.Button(f"Remove Object Type {object_id}",
                       id={'type': 'remove-object-type', 'index': object_id},
                       n_clicks=0, disabled=False, style=globals.BUTTONS_STYLE, color='danger')
        ], id={'type': 'object-type', 'index': object_id})
    return object_form


@callback(Output('div-add', 'hidden', allow_duplicate=True),
          Output('div-iteration', 'children', allow_duplicate=True),
          Output({'type': 'show-iteration-options', 'iteration': ALL}, 'hidden', allow_duplicate=True),
          Output('div-store-configuration', 'hidden', allow_duplicate=True),
          Input({'type': 'cancel', 'dummy': 0}, 'n_clicks'),
          prevent_initial_call=True)
def cancel_datasource_identification(n_clicks):
    if n_clicks:
        return False, [], [False for o in ctx.outputs_list[2]], False
    else:
        return no_update


@callback(Output('div-add', 'hidden', allow_duplicate=True),
          Output('div-iteration', 'children'),
          Output({'type': 'show-iteration-options', 'iteration': ALL}, 'hidden', allow_duplicate=True),
          Output('div-store-configuration', 'hidden'),
          Input('add-datasource', 'n_clicks'),
          State('add-direction', 'value'),
          prevent_initial_call=True)
def add_datasource(n_clicks, direction):
    start_timestamp = True if direction == 'Timestamp' else False

    # Assemble candidates
    sug_timestamp_tables = []
    sug_object_tables = []
    if start_timestamp:
        sug_timestamp_tables = get_label_value_base('timestamps')
    else:
        sug_object_tables = get_label_value_base('objects')

    identification_form = html.Div([
        html.Div([
            'Identify a table and a column that contains timestamps!',
            dcc.Dropdown(
                options=sug_timestamp_tables,
                id={'type': 'tables', 'subject': 'timestamp', 'index': 0},
                multi=False,
                disabled=not start_timestamp),
            dcc.Dropdown(
                options=[],
                id={'type': 'columns', 'subject': 'timestamp', 'index': 0},
                multi=False,
                disabled=True),
            timestamp_modal,
            dbc.Button(
                'Custom Timestamp SQL',
                id={'type': 'open-timestamp', 'subject': 'timestamp', 'index': 0},
                disabled=not start_timestamp, style=globals.BUTTONS_STYLE, color='info'),
            dcc.Store(id={'type': 'timestamp-store', 'subject': 'timestamp', 'index': 0}, storage_type='memory',
                      data=''),
            dbc.Button(
                'Confirm Timestamp Column',
                id={'type': 'confirm', 'subject': 'timestamp', 'index': 0},
                disabled=True, style=globals.BUTTONS_STYLE)
        ]),
        html.Hr(),
        html.Div([
            'Identify Event Types',
            dcc.Dropdown(
                options=[],
                id={'type': 'tables', 'subject': 'event-types', 'index': 0},
                value=[],
                multi=True,
                disabled=True),
            dbc.Row([
                dbc.Col(dbc.RadioItems(
                    options=[{'label': 'All columns contain the same event type',
                              'value': False, 'disabled': True},
                             {'label': 'Select columns:',
                              'value': True, 'disabled': True}],
                    value=True,
                    id={'type': 'radio-columns', 'subject': 'event-types',
                        'index': 0}), width=6),
                dbc.Col(dbc.Input(placeholder='Label for the Event Type',
                                  id={'type': 'single-label', 'subject': 'event-types', 'index': 0}, type='text',
                                  disabled=True))
            ]),
            dcc.Dropdown(
                options=[],
                id={'type': 'columns', 'subject': 'event-types', 'index': 0},
                value=[],
                multi=True,
                disabled=True),
            dbc.Button(
                'Set Filter',
                id={'type': 'open-filter', 'subject': 'event-types', 'index': 0},
                disabled=True, style=globals.BUTTONS_STYLE),
            dcc.Store(id={'type': 'filter-store', 'subject': 'event-types', 'index': 0}, storage_type='memory',
                      data=''),
            filter_modal,
            dbc.Button(
                'Add Attributes',
                id={'type': 'open-attributes', 'subject': 'event-types', 'index': 0},
                disabled=True, style=globals.BUTTONS_STYLE),
            dcc.Store(id={'type': 'attributes-store', 'subject': 'event-types', 'index': 0}, storage_type='memory',
                      data=''),
            attributes_modal,
            dbc.Button(
                'Confirm Event Type Columns',
                id={'type': 'confirm', 'subject': 'event-types', 'index': 0},
                disabled=True, style=globals.BUTTONS_STYLE)
        ]),
        html.Hr(),
        html.Div([
            get_object_form(0, sug_object_tables)
        ], id={'type': 'objects-box', 'dummy': 0}),  # dummy is used to ensure no id-not-found-in-layout-errors occur
        dbc.Row([
            dbc.Col([
                dbc.Button('Add Object Type', id={'type': 'add-objects', 'dummy': 0},
                           n_clicks=0, disabled=True, style=globals.BUTTONS_STYLE, color='info'),
                dbc.Button('Add Object Type from another Data Source', id={'type': 'copy-objects', 'dummy': 0},
                           n_clicks=0, disabled=True, style=globals.BUTTONS_STYLE, color='info')
            ], width=5),
            existing_object_modal,
            dbc.Col([
                dbc.Button('Cancel', id={'type': 'cancel', 'dummy': 0}, n_clicks=0, disabled=False,
                           style=globals.BUTTONS_STYLE,
                           color='danger'),
                dbc.Button('Save data source', id={'type': 'save-iteration', 'dummy': 0}, n_clicks=0, disabled=True,
                           style=globals.BUTTONS_STYLE, color='success')
            ], width=4)
        ], justify='between')
    ])

    return True, identification_form, [True for f in ctx.outputs_list[2]], True


@callback(Output({'type': 'objects-box', 'dummy': 0}, 'children'),
          Input({'type': 'add-objects', 'dummy': 0}, 'n_clicks'),
          State({'type': 'object-label', 'subject': 'objects', 'index': ALL}, 'value'),
          prevent_initial_call=True)
def add_object_type(n_clicks, list_object_labels):
    if not n_clicks:
        return no_update

    obj_id = len(ctx.states_list[0])

    object_element = get_object_form(obj_id)

    patched_objects = Patch()
    patched_objects.append(object_element)
    return patched_objects


@callback(Output({'type': 'saved-iteration', 'iteration': MATCH}, 'hidden'),
          Output({'type': 'store', 'iteration': MATCH}, 'data'),
          Input({'type': 'remove', 'iteration': MATCH}, 'n_clicks'),
          State({'type': 'store', 'iteration': MATCH}, 'data'),
          prevent_initial_call=True)
def delete_datasource(n_clicks, dict_iteration):
    if n_clicks:
        dict_iteration['removed'] = True
        return True, dict_iteration
    else:
        return no_update


def reorder_joins(timestamp_table, timestamp_column, event_tables, event_columns, objects_list_tables,
                  objects_list_columns):
    json_tables = []
    if timestamp_table:
        json_tables.append(timestamp_table)
    if event_tables:
        json_tables.extend(event_tables)
    if objects_list_tables[0]:
        json_tables.extend(objects_list_tables[0])
    # json_tables = timestamp_table + event_tables + objects_list_tables[0]
    tables = [json.loads(json_table) for json_table in json_tables]
    timestamp_table_candidate = json.loads(timestamp_column)['table']
    list_candidates = []
    root_table_index = -1
    for i, t in enumerate(tables):
        if 'keypair' in t:
            list_candidates.append((t['table'], t['keypair'], t['path']))
        else:
            list_candidates.append((t['table'], None, t['path']))
        if t['table'] == timestamp_table_candidate:
            root_table_index = i

    # flip table-keypair pairs
    new_table_keypair = []
    previous_table = timestamp_table_candidate
    next_candidate = list_candidates[root_table_index]
    list_candidates.pop(root_table_index)
    table_cleansed = False
    for json_table in event_tables:
        if timestamp_table_candidate == json.loads(json_table)['table']:
            event_tables.remove(json_table)
            table_cleansed = True
    if not table_cleansed:
        for json_table in objects_list_tables[0]:
            if timestamp_table_candidate == json.loads(json_table)['table']:
                objects_list_tables[0].remove(json_table)

    while next_candidate[1] is not None:
        pk_name, pk_table, pk_columns, fk_name, fk_table, fk_columns = globals.dict_keypair[next_candidate[1]]
        if pk_table == previous_table:
            target_table = fk_table
        elif fk_table == previous_table:
            target_table = pk_table
        else:
            raise Exception(f"Could not find key {next_candidate[1]} matching {previous_table}.")

        # path stays the same since it only needs to be unique and info can not be derived
        new_table_keypair.append(
            {'table': target_table, 'keypair': next_candidate[1], 'path': next_candidate[2], 'info': []})
        table_cleansed = False
        for json_table in event_tables:
            if target_table == json.loads(json_table)['table']:
                event_tables.remove(json_table)
                table_cleansed = True
        if not table_cleansed:
            for json_table in objects_list_tables[0]:
                if target_table == json.loads(json_table)['table']:
                    objects_list_tables[0].remove(json_table)

        for i, t in enumerate(list_candidates):
            if t[0] == target_table:
                next_candidate = t  # breaks if last table of the chain is processed

    # connect tables to entities based on selected columns
    required_event_tables_list = []
    for json_column in event_columns:
        req_table = json.loads(json_column)['table']
        if req_table != timestamp_table_candidate:
            required_event_tables_list.append(req_table)
    for i, object_columns in enumerate(objects_list_columns):
        if i > 0:
            for json_column in object_columns:
                req_table = json.loads(json_column)['table']
                if req_table not in objects_list_tables[i] and req_table != timestamp_table_candidate:
                    required_event_tables_list.append(req_table)

    required_event_tables = set(required_event_tables_list)
    event_tables_candidates = []
    while not required_event_tables.issubset(set([i['table'] for i in event_tables_candidates])):
        event_tables_candidates.append(new_table_keypair.pop(0))

    timestamp_table = json.dumps(
        {'table': timestamp_table_candidate, 'path': json.loads(timestamp_column)['path'], 'info': []})
    # tables required to select columns of event types and object types other than the initial one
    # needs to be in the set of event tables
    event_tables.extend([json.dumps(new_table) for new_table in event_tables_candidates])
    # all remaining tables can be bound on object type level
    objects_list_tables[0].extend([json.dumps(new_table) for new_table in new_table_keypair])
    return timestamp_table, event_tables, objects_list_tables


@callback(Output('iteration-overview', 'children'),
          Output('iteration-overview', 'hidden'),
          Output('div-iteration', 'children', allow_duplicate=True),
          Output('div-add', 'hidden', allow_duplicate=True),
          Output({'type': 'show-iteration-options', 'iteration': ALL}, 'hidden', allow_duplicate=True),
          Output('lower-right-side', 'hidden'),
          Output('div-import-configuration', 'hidden'),
          Output('div-store-configuration', 'hidden', allow_duplicate=True),
          Input({'type': 'save-iteration', 'dummy': 0}, 'n_clicks'),
          State({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'value'),
          State({'type': 'columns', 'subject': 'timestamp', 'index': 0}, 'value'),
          State({'type': 'timestamp-store', 'subject': 'timestamp', 'index': 0}, 'data'),
          State({'type': 'tables', 'subject': 'event-types', 'index': 0}, 'value'),
          State({'type': 'columns', 'subject': 'event-types', 'index': 0}, 'value'),
          State({'type': 'radio-columns', 'subject': 'event-types', 'index': 0}, 'value'),
          State({'type': 'single-label', 'subject': 'event-types', 'index': 0}, 'value'),
          State({'type': 'object-label', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'tables', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'columns', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'radio-columns', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'single-label', 'subject': 'objects', 'index': ALL}, 'value'),
          State('add-direction', 'value'),
          State({'type': 'saved-iteration', 'iteration': ALL}, 'children'),
          State({'type': 'filter-store', 'subject': 'event-types', 'index': 0}, 'data'),
          State({'type': 'attributes-store', 'subject': 'event-types', 'index': 0}, 'data'),
          State({'type': 'filter-store', 'subject': 'objects', 'index': ALL}, 'data'),
          State({'type': 'attributes-store', 'subject': 'objects', 'index': ALL}, 'data'),
          prevent_initial_call=True)
def save_iteration(n_clicks, timestamp_table, timestamp_column, timestamp_data, event_tables, event_columns,
                   use_event_columns,
                   replacement_event_label, object_type_label, objects_list_tables, objects_list_columns,
                   use_object_columns, replacement_object_label, direction, stored_iterations, event_filter,
                   event_attributes, list_object_filters, list_object_attributes):
    if not n_clicks:
        return no_update

    iteration_id = len(list(filter(bool, stored_iterations)))

    # clean up inputs
    if timestamp_data:
        timestamp_column = json.dumps({'table': json.loads(timestamp_table)['table'],
                                       'column': 'timestamp',
                                       'table-path': json.loads(timestamp_table)['path']})
    if not use_event_columns:
        event_columns = []
    else:
        replacement_event_label = ''
    for i, use_columns in enumerate(use_object_columns):
        if not use_columns:
            objects_list_columns[i] = []
        else:
            replacement_object_label[i] = ''

    # reorder table joins to equalize iteration outputs
    if direction == 'Object':
        timestamp_table, event_tables, objects_list_tables = reorder_joins(timestamp_table, timestamp_column,
                                                                           event_tables, event_columns,
                                                                           objects_list_tables, objects_list_columns)

    iteration_data = globals.inputs_to_iteration_data(timestamp_table, timestamp_column, timestamp_data, event_tables,
                                                      event_columns,
                                                      use_event_columns, replacement_event_label, {},
                                                      event_filter, event_attributes, object_type_label,
                                                      objects_list_tables,
                                                      objects_list_columns, use_object_columns,
                                                      replacement_object_label,
                                                      list_object_filters, list_object_attributes, False)

    iteration_summary = globals.iteration_data_to_info(iteration_data)

    menu_options = html.Div([
        html.Hr(),
        html.Pre(iteration_summary, id={'type': 'iteration-info', 'iteration': iteration_id}),
        html.Div([
            dbc.Button('Remove',
                       id={'type': 'remove', 'iteration': iteration_id}, n_clicks=0, style=globals.BUTTONS_STYLE,
                       color='danger'),
            dbc.Button('Modify Objects',
                       id={'type': 'modify', 'iteration': iteration_id}, n_clicks=0, style=globals.BUTTONS_STYLE),
            dbc.Button('Label Event Types', id={'type': 'set-labels', 'iteration': iteration_id}, n_clicks=0,
                       style=globals.BUTTONS_STYLE),
        ], id={'type': 'show-iteration-options', 'iteration': iteration_id}, hidden=False),
        dcc.Store(id={'type': 'store', 'iteration': iteration_id}, storage_type='memory', data=iteration_data)
    ], id={'type': 'saved-iteration', 'iteration': iteration_id})

    patched_overview = Patch()
    patched_overview.append(menu_options)

    return patched_overview, False, [], False, [False for f in ctx.outputs_list[4]], False, True, False


@callback(Output('div-iteration', 'children', allow_duplicate=True),
          Output('add-direction', 'value'),
          Output('div-add', 'hidden', allow_duplicate=True),
          Output({'type': 'show-iteration-options', 'iteration': ALL}, 'hidden'),
          Output({'type': 'modify', 'iteration': ALL}, 'n_clicks'),
          Output('div-store-configuration', 'hidden', allow_duplicate=True),
          Input({'type': 'modify', 'iteration': ALL}, 'n_clicks'),
          State({'type': 'store', 'iteration': ALL}, 'data'),
          prevent_initial_call=True)
def modify_datasource(n_clicks, all_iteration_data):
    iteration_id = ctx.triggered_id['iteration']
    if not n_clicks[iteration_id]:
        return no_update

    iteration = all_iteration_data[iteration_id]
    if iteration['removed']:
        return no_update

    (timestamp_table, timestamp_column, timestamp_data,
     event_tables, event_columns, use_event_columns, replacement_event_label, event_labels, event_filter,
     event_attributes,
     object_type_label, objects_list_tables, objects_list_columns, use_object_columns,
     replacement_object_label, object_filters, object_attributes) = globals.iteration_data_to_inputs(iteration)

    # find table options
    objects_list_tables_options = []
    objects_list_columns_options = []
    event_table_options = []
    event_column_options = []
    timestamp_table_options, timestamp_column_options = get_table_column_options(timestamp_table)
    for json_event_table in reversed(event_tables):
        eto, eco = get_table_column_options(json_event_table)
        event_table_options.extend(eto)
        event_column_options.extend(eco)
    for object_tables in objects_list_tables:
        object_table_options = []
        object_column_options = []
        for json_object_table in reversed(object_tables):
            oto, oco = get_table_column_options(json_object_table)
            object_table_options.extend(oto)
            object_column_options.extend(oco)
        objects_list_tables_options.append(object_table_options + event_table_options + timestamp_table_options)
        objects_list_columns_options.append(object_column_options + event_column_options + timestamp_column_options)

    modify_form = html.Div([
        html.Div([
            'Timestamps',
            dcc.Dropdown(
                options=[{'label': f"{json.loads(timestamp_table)['table']}",
                          'value': timestamp_table}],
                id={'type': 'tables', 'subject': 'timestamp', 'index': 0},
                value=timestamp_table,
                multi=False,
                disabled=True),
            dcc.Dropdown(
                options=[{'label': f"{json.loads(timestamp_column)['table']}.{json.loads(timestamp_column)['column']}",
                          'value': timestamp_column}],
                id={'type': 'columns', 'subject': 'timestamp', 'index': 0},
                value=timestamp_column,
                multi=False,
                disabled=True),
        ]),
        html.Hr(),
        html.Div([
            'Event Types',
            dcc.Dropdown(
                options=[{'label': f"{json.loads(t)['table']}",
                          'value': t} for t in event_tables],
                id={'type': 'tables', 'subject': 'event-types', 'index': 0},
                value=event_tables,
                multi=True,
                disabled=True),
            dbc.Row([
                dbc.Col(dbc.RadioItems(
                    options=[{'label': 'All columns contain the same event type',
                              'value': False, 'disabled': True},
                             {'label': 'Select columns:',
                              'value': True, 'disabled': True}],
                    value=use_event_columns,
                    id={'type': 'radio-columns', 'subject': 'event-types',
                        'index': 0})),
                dbc.Col(dbc.Input(placeholder='Label for the Event Type',
                                  id={'type': 'single-label', 'subject': 'event-types', 'index': 0},
                                  value=replacement_event_label, type='text', disabled=True))
            ]),
            dcc.Dropdown(
                options=[{'label': f"{json.loads(c)['table']}.{json.loads(c)['column']}",
                          'value': c} for c in event_columns],
                id={'type': 'columns', 'subject': 'event-types', 'index': 0},
                value=event_columns,
                multi=True,
                disabled=True),
            dbc.Button(
                'Set Filter',
                id={'type': 'open-filter', 'subject': 'event-types', 'index': 0},
                disabled=False, style=globals.BUTTONS_STYLE),
            dcc.Store(id={'type': 'filter-store', 'subject': 'event-types', 'index': 0}, storage_type='memory',
                      data=event_filter),
            filter_modal,
            dbc.Button(
                'Add Attributes',
                id={'type': 'open-attributes', 'subject': 'event-types', 'index': 0},
                disabled=True, style=globals.BUTTONS_STYLE),
            dcc.Store(id={'type': 'attributes-store', 'subject': 'event-types', 'index': 0}, storage_type='memory',
                      data=event_attributes),
            attributes_modal
        ]),
        html.Hr(),
        html.Div([
            get_object_form_filled(obj_id, object_type_label[obj_id], objects_list_tables_options[obj_id],
                                   objects_list_tables[obj_id], objects_list_columns_options[obj_id],
                                   objects_list_columns[obj_id], use_object_columns[obj_id],
                                   replacement_object_label[obj_id], object_filters[obj_id], object_attributes[obj_id])
            for obj_id in
            range(len(objects_list_tables))
        ], id={'type': 'objects-box', 'dummy': 0}),
        dbc.Row([
            dbc.Col([
                dbc.Button('Add Object Type', id={'type': 'add-objects', 'dummy': 0},
                           n_clicks=0, disabled=False, style=globals.BUTTONS_STYLE, color='info'),
                dbc.Button('Add Object Type from another Data Source', id={'type': 'copy-objects', 'dummy': 0},
                           n_clicks=0, disabled=False, style=globals.BUTTONS_STYLE),
            ], width=5),
            existing_object_modal,
            dbc.Col([
                dbc.Button('Cancel', id={'type': 'cancel', 'dummy': 0}, n_clicks=0, disabled=False,
                           style=globals.BUTTONS_STYLE,
                           color='danger'),
                dbc.Button('Finish', id={'type': 'finish-modification', 'iteration': iteration_id},
                           n_clicks=0, disabled=False, style=globals.BUTTONS_STYLE, color='success')
            ], width=3)
        ], justify='between')
    ])
    # set direction to Timestamp so function for the usual form can be reused
    return modify_form, 'Timestamp', True, [True for out in ctx.outputs_list[3]], [0 for out in
                                                                                   ctx.outputs_list[4]], True


@callback(Output({'type': 'object-type', 'index': MATCH}, 'hidden'),
          Input({'type': 'remove-object-type', 'index': MATCH}, 'n_clicks'),
          prevent_initial_call=True)
def remove_object_type(n_clicks):
    if not n_clicks:
        return no_update
    return True


@callback(Output({'type': 'store', 'iteration': ALL}, 'data', allow_duplicate=True),
          Output('div-iteration', 'children', allow_duplicate=True),
          Output('div-add', 'hidden', allow_duplicate=True),
          Output({'type': 'show-iteration-options', 'iteration': ALL}, 'hidden', allow_duplicate=True),
          Output({'type': 'iteration-info', 'iteration': ALL}, 'children'),
          Output('div-store-configuration', 'hidden', allow_duplicate=True),
          # index per overall handled iteration
          Input({'type': 'finish-modification', 'iteration': ALL}, 'n_clicks'),
          State({'type': 'store', 'iteration': ALL}, 'data'),
          State({'type': 'iteration-info', 'iteration': ALL}, 'children'),
          # index per object type in current iteration
          State({'type': 'object-type', 'index': ALL}, 'hidden'),
          State({'type': 'object-label', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'tables', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'columns', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'radio-columns', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'single-label', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'filter-store', 'subject': 'event-types', 'index': ALL}, 'data'),
          State({'type': 'attributes-store', 'subject': 'event-types', 'index': ALL}, 'data'),
          State({'type': 'filter-store', 'subject': 'objects', 'index': ALL}, 'data'),
          State({'type': 'attributes-store', 'subject': 'objects', 'index': ALL}, 'data'),
          prevent_initial_call=True)
def finish_modification(n_clicks, all_iteration_data, all_iteration_info, is_deleted, object_type_label,
                        objects_list_tables, objects_list_columns, use_object_columns, replacement_object_label,
                        dummy_event_filter, dummy_event_attributes, list_object_filters, list_object_attributes):
    if not ctx.triggered_id or not any(
            ctx.triggered_id['iteration'] == i['id']['iteration'] and i['value'] for i in ctx.inputs_list[0]):
        return no_update
    iteration_id = ctx.triggered_id['iteration']
    iteration_data = all_iteration_data[iteration_id]

    # to avoid callback error when canceling the form...
    if dummy_event_filter:
        event_filter = dummy_event_filter[0]
    else:
        event_filter = ''
    if dummy_event_filter:
        event_attributes = dummy_event_attributes[0]
    else:
        event_attributes = ''

    for obj_id, object_table in enumerate(objects_list_tables):
        if is_deleted[obj_id]:
            object_type_label.pop(obj_id)
            objects_list_tables.pop(obj_id)
            objects_list_columns.pop(obj_id)
            use_object_columns.pop(obj_id)
            replacement_object_label.pop(obj_id)
        else:
            if not use_object_columns[obj_id]:
                objects_list_columns[obj_id] = []
            else:
                replacement_object_label[obj_id] = ''

    iteration_data['object_type_label'] = object_type_label
    iteration_data['objects_list_tables'] = objects_list_tables
    iteration_data['objects_list_columns'] = objects_list_columns
    iteration_data['use_object_columns'] = use_object_columns
    iteration_data['replacement_object_label'] = replacement_object_label
    iteration_data['event_filter'] = event_filter
    iteration_data['event_attributes'] = event_attributes
    iteration_data['object_filters'] = list_object_filters
    iteration_data['object_attributes'] = list_object_attributes

    updated_all_iteration_data = []
    updated_all_iteration_info = []
    for tb_output in ctx.outputs_list[0]:
        output_id = tb_output['id']['iteration']
        if output_id == iteration_id:
            updated_all_iteration_data.append(iteration_data)
            updated_all_iteration_info.append(globals.iteration_data_to_info(iteration_data))
        else:
            updated_all_iteration_data.append(all_iteration_data[output_id])
            updated_all_iteration_info.append(all_iteration_info[output_id])

    return updated_all_iteration_data, [], False, [False for out in
                                                   ctx.outputs_list[3]], updated_all_iteration_info, False


@callback(Output({'type': 'modal', 'intend': 'store-configuration'}, 'is_open'),
          Input('store-configuration', 'n_clicks'),
          State({'type': 'store', 'iteration': ALL}, 'data'),
          State({'type': 'o2o-store', 'o2o': ALL}, 'data'),
          State({'type': 'o2o-enforce-store', 'o2o': ALL}, 'data'),
          prevent_initial_call=True)
def export_configuration(n_clicks, all_iteration_data, all_o2o_data, all_o2o_enforce_data):
    if not n_clicks:
        return no_update
    dict_export = {'all_iteration_data': [], 'all_o2o_data': [], 'all_o2o_enforce_data': []}
    for iteration in all_iteration_data:
        if not iteration['removed']:
            dict_export['all_iteration_data'].append(iteration)

    for o2o in all_o2o_data:
        if not o2o['removed']:
            dict_export['all_o2o_data'].append(o2o)

    for enforcement in all_o2o_enforce_data:
        if not enforcement['removed']:
            dict_export['all_o2o_enforce_data'].append(enforcement)

    with open(f"configuration.json", 'w') as f_configuration:
        json.dump(dict_export, f_configuration, indent=1)

    return True


@callback(Output('iteration-overview', 'children', allow_duplicate=True),
          Output('iteration-overview', 'hidden', allow_duplicate=True),
          Output('o2o-store-div', 'children', allow_duplicate=True),
          Output('o2o-enforce-store-div', 'children', allow_duplicate=True),
          Output('div-import-configuration', 'hidden', allow_duplicate=True),
          Output('lower-right-side', 'hidden', allow_duplicate=True),
          Input('upload-configuration', 'contents'),
          Input('upload-configuration', 'filename'),
          prevent_initial_call=True)
def import_configuration(contents, filename):
    if not filename.endswith(".json"):
        return "Please upload a file with the .json extension"

    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)

    # this is a dict!
    content_dict = json.loads(decoded)
    iteration_overview_form = []
    if 'all_iteration_data' in content_dict:
        for iteration_id, iteration_data in enumerate(content_dict['all_iteration_data']):
            iteration_data = globals.validate_iteration_data(iteration_data)
            iteration_summary = globals.iteration_data_to_info(iteration_data)
            menu_options = html.Div([
                html.Hr(),
                html.Pre(iteration_summary, id={'type': 'iteration-info', 'iteration': iteration_id}),
                html.Div([
                    dbc.Button('Remove',
                               id={'type': 'remove', 'iteration': iteration_id}, n_clicks=0,
                               style=globals.BUTTONS_STYLE,
                               color='danger'),
                    dbc.Button('Modify Data Source',
                               id={'type': 'modify', 'iteration': iteration_id}, n_clicks=0,
                               style=globals.BUTTONS_STYLE),
                    dbc.Button('Label Event Types', id={'type': 'set-labels', 'iteration': iteration_id}, n_clicks=0,
                               style=globals.BUTTONS_STYLE),
                ], id={'type': 'show-iteration-options', 'iteration': iteration_id}, hidden=False),
                dcc.Store(id={'type': 'store', 'iteration': iteration_id}, storage_type='memory', data=iteration_data)
            ], id={'type': 'saved-iteration', 'iteration': iteration_id})
            iteration_overview_form.append(menu_options)

    o2o_overview_form = []
    if 'all_o2o_data' in content_dict:
        for o2o_data in content_dict['all_o2o_data']:
            o2o_id = globals.next_o2o_id
            globals.next_o2o_id += 1

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
            o2o_overview_form.append(o2o_div)

    enforcement_o2o_overview_form = []
    if 'all_o2o_enforce_data' in content_dict:
        for enforcement in content_dict['all_o2o_enforce_data']:
            o2o_id = globals.next_o2o_id
            globals.next_o2o_id += 1

            enforced_o2o_summary = globals.enforced_o2o_dict_to_summary(enforcement)

            enforcement_o2o_div = html.Div([
                html.Hr(),
                html.Pre(enforced_o2o_summary, id={'type': 'o2o-info', 'o2o': o2o_id}),
                html.Div([
                    dbc.Button('Remove', id={'type': 'o2o-remove', 'o2o': o2o_id}, n_clicks=0,
                               style=globals.BUTTONS_STYLE, color='danger'),
                ], id={'type': 'o2o-show-options', 'o2o': o2o_id}, hidden=False),
                dcc.Store(id={'type': 'o2o-enforce-store', 'o2o': o2o_id}, storage_type='memory', data=enforcement)
            ], id={'type': 'o2o-saved', 'o2o': o2o_id})
            enforcement_o2o_overview_form.append(enforcement_o2o_div)

    return iteration_overview_form, False, o2o_overview_form, enforcement_o2o_overview_form, True, False


@callback(Output({'type': 'timestamp-modal'}, 'is_open'),
          Output({'type': 'timestamp-body'}, 'children'),
          Output({'type': 'timestamp-footer'}, 'children'),
          Input({'type': 'open-timestamp', 'subject': 'timestamp', 'index': 0}, 'n_clicks'),
          prevent_initial_call=True)
def open_timestamp_modal(n_clicks):
    if not n_clicks:
        return no_update

    sug_timestamp_tables = get_label_value_base('timestamps')

    body = html.Div([
        dcc.Dropdown(options=sug_timestamp_tables, id={'type': 'timestamp-modal-table'},
                     multi=False, disabled=False),
        dbc.Textarea(placeholder='Enter a complete SQL statement.\n'
                                 'Must contain all columns of root table and provide a column "timestamp"!',
                     id={'type': 'timestamp-modal-sql'})
    ])

    footer = dbc.Button('Confirm', id={'type': 'timestamp-modal-confirm', 'subject': 'timestamp', 'index': 0},
                        n_clicks=0, style=globals.BUTTONS_STYLE, color='success', disabled=False)

    return True, body, footer


@callback(Output({'type': 'timestamp-modal'}, 'is_open', allow_duplicate=True),
          Output({'type': 'timestamp-store', 'subject': 'timestamp', 'index': 0}, 'data'),
          Output({'type': 'open-timestamp', 'subject': 'timestamp', 'index': 0}, 'disabled', allow_duplicate=True),
          Output({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'value', allow_duplicate=True),
          Output({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'disabled', allow_duplicate=True),
          Output({'type': 'columns', 'subject': 'timestamp', 'index': 0}, 'disabled', allow_duplicate=True),
          Output({'type': 'confirm', 'subject': 'timestamp', 'index': 0}, 'n_clicks', allow_duplicate=True),
          Input({'type': 'timestamp-modal-confirm', 'subject': 'timestamp', 'index': 0}, 'n_clicks'),
          State({'type': 'timestamp-modal-table'}, 'value'),
          State({'type': 'timestamp-modal-sql'}, 'value'),
          prevent_initial_call=True)
def confirm_timestamp_modal(n_clicks, root_table, timestamp_sql):
    if not n_clicks and not (root_table and timestamp_sql):
        return no_update

    timestamp_data = {'root_table': root_table, 'timestamp_sql': timestamp_sql}

    return False, timestamp_data, True, root_table, True, True, 1
