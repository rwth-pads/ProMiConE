from dash import html, dcc, Output, Input, State, callback, Patch, MATCH, ALL, no_update, ctx
import dash_bootstrap_components as dbc

import globals


def get_filter_form(column_options, filter_id, dict_filter):
    if dict_filter:
        column = dict_filter['column']
        operator = dict_filter['operator']
        is_static = dict_filter['value_type']
        if is_static:
            sta_value = dict_filter['value']
            col_value = None
        else:
            sta_value = ''
            col_value = dict_filter['value']
    else:
        column = []
        operator = 'IS NOT'
        is_static = True
        sta_value = 'NULL'
        col_value = None

    filter_form = html.Div(
        dbc.Row([
            dbc.Col([
                dbc.Label('Column:'),
                dcc.Dropdown(options=column_options, value=column, id={'type': 'filter-column', 'filter_id': filter_id},
                             multi=False)
            ], width=4),
            dbc.Col([
                dbc.Label('Operator:'),
                dbc.Input(id={'type': 'filter-operator', 'filter_id': filter_id}, value=operator, type='text'),
                dbc.FormText("e.g. '>=', 'IS NOT'")
            ], width=2),
            dbc.Col([
                dbc.Label('Value:'),
                html.Div(
                    dbc.Input(id={'type': 'filter-static-value', 'filter_id': filter_id}, value=sta_value, type='text'),
                    id={'type': 'filter-value-div', 'filter_id': filter_id}, hidden=not is_static),
                html.Div(dcc.Dropdown(options=column_options, value=col_value,
                                      id={'type': 'filter-column-value', 'filter_id': filter_id}, multi=False),
                         id={'type': 'filter-column-div', 'filter_id': filter_id}, hidden=is_static),
                dbc.RadioItems(id={'type': 'filter-value-type', 'filter_id': filter_id},
                               options=[{'label': 'Static Value', 'value': True}, {'label': 'Column', 'value': False}],
                               value=is_static),

            ], width=4),
            dbc.Col([
                html.Br(),
                dbc.Button('Remove', id={'type': 'filter-remove', 'filter_id': filter_id}, n_clicks=0,
                           style=globals.BUTTONS_STYLE, color='danger')
            ], width=1)  #
        ])
        , id={'type': 'filter-div', 'filter_id': filter_id})

    return filter_form


@callback(Output({'type': 'filter-value-div', 'filter_id': MATCH}, 'hidden'),
          Output({'type': 'filter-column-div', 'filter_id': MATCH}, 'hidden'),
          Input({'type': 'filter-value-type', 'filter_id': MATCH}, 'value'))
def swap_filter_input(is_static):
    return not is_static, is_static


@callback(Output({'type': 'filter-modal'}, 'is_open'),
          Output({'type': 'filter-modal-body'}, 'children'),
          Output({'type': 'filter-modal-footer'}, 'children'),
          Input({'type': 'open-filter', 'subject': ALL, 'index': ALL}, 'n_clicks'),
          State({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'value'),
          State({'type': 'tables', 'subject': 'event-types', 'index': 0}, 'value'),
          State({'type': 'tables', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'filter-store', 'subject': 'event-types', 'index': 0}, 'data'),
          State({'type': 'filter-store', 'subject': 'objects', 'index': ALL}, 'data'),
          prevent_initial_call=True)
def open_filter_modal(n_clicks, timestamp_table, event_type_tables, list_object_type_tables,  event_filter_data,
                      list_object_filter_data):
    if not ctx.triggered_id or not any(
            ctx.triggered_id['subject'] == inputs['id']['subject'] and ctx.triggered_id['index'] == inputs['id']['index']
            and n_clicks[i] for i, inputs in enumerate(ctx.inputs_list[0])):
        return no_update

    subject = ctx.triggered_id['subject']
    index = ctx.triggered_id['index']
    filter_values = event_filter_data

    object_type_tables = []
    if subject == 'objects':
        for j, ot in enumerate(ctx.states_list[2]):
            if index == ot['id']['index']:
                object_type_tables = list_object_type_tables[j]
                filter_values = list_object_filter_data[j] if list_object_filter_data else []

    filters = []
    filter_id = 0
    filter_column_options = []
    for json_table in reversed([timestamp_table] + event_type_tables + object_type_tables):
        tables_options, columns_options = globals.get_table_column_options(json_table)
        filter_column_options.extend(columns_options)

    for dict_filter in filter_values:
        filters.append(get_filter_form(filter_column_options, filter_id, dict_filter))
        filter_id += 1

    filters.append(get_filter_form(filter_column_options, filter_id, None))

    filter_buttons = html.Div([
        dbc.Button('Add Filter', id={'type': 'filter-add'},
                   n_clicks=0, style=globals.BUTTONS_STYLE, color='primary'),
        dbc.Button('Finish', id={'type': 'filter-finish', 'subject': subject, 'index': index},
                   n_clicks=0, style=globals.BUTTONS_STYLE, color='success')
    ])

    return True, filters, filter_buttons


@callback(Output({'type': 'filter-div', 'filter_id': MATCH}, 'children'),
          Input({'type': 'filter-remove', 'filter_id': MATCH}, 'n_clicks'),
          prevent_initial_call=True)
def remove_filter_entry(n_clicks):
    if not n_clicks:
        return no_update
    return html.Div([])


@callback(Output({'type': 'filter-modal-body'}, 'children', allow_duplicate=True),
          Input({'type': 'filter-add'}, 'n_clicks'),
          State({'type': 'filter-column', 'filter_id': ALL}, 'options'),
          prevent_initial_call=True)
def add_filter(n_clicks, list_filter_options):
    if not n_clicks:
        return no_update

    if len(ctx.states_list[0]) > 0:
        filter_id = ctx.states_list[0][len(ctx.states_list[0]) - 1]['id']['filter_id']
    else:
        filter_id = 0

    patched_body = Patch()
    patched_body.append(get_filter_form(list_filter_options[0], filter_id, None))

    return patched_body


@callback(Output({'type': 'filter-store', 'subject': MATCH, 'index': MATCH}, 'data'),
          Input({'type': 'filter-finish', 'subject': MATCH, 'index': MATCH}, 'n_clicks'),
          State({'type': 'filter-column', 'filter_id': ALL}, 'value'),
          State({'type': 'filter-operator', 'filter_id': ALL}, 'value'),
          State({'type': 'filter-value-type', 'filter_id': ALL}, 'value'),
          State({'type': 'filter-static-value', 'filter_id': ALL}, 'value'),
          State({'type': 'filter-column-value', 'filter_id': ALL}, 'value'),
          prevent_initial_call=True)
def finish_filter(n_clicks, list_column, list_operator, list_value_type, list_sta_value, list_col_value):
    if not n_clicks:
        return no_update

    filter_data = []

    for i, col_state in enumerate(ctx.states_list[0]):
        if list_column[i]:
            dict_filter = {'column': list_column[i],
                           'operator': list_operator[i],
                           'value_type': list_value_type[i]}
            if list_value_type[i]:
                dict_filter['value'] = list_sta_value[i]
            else:
                dict_filter['value'] = list_col_value[i]
            filter_data.append(dict_filter)

    return filter_data#, [], False


@callback(Output({'type': 'filter-modal'}, 'is_open', allow_duplicate=True),
          Input({'type': 'filter-store', 'subject': ALL, 'index': ALL}, 'data'),
          prevent_initial_call=True)
def close_filter_modal(data):
    if not ctx.triggered_id:
        return no_update
    return False


@callback(Output({'type': 'filter-modal'}, 'is_open', allow_duplicate=True),
          Output({'type': 'filter-modal-body'}, 'children', allow_duplicate=True),
          Output({'type': 'filter-modal-footer'}, 'children', allow_duplicate=True),
          Input({'type': 'open-o2o-filter', 'object': ALL}, 'n_clicks'),
          State({'type': 'table-o2o'}, 'value'),
          State({'type': 'filter-store', 'object': 1}, 'data'),
          State({'type': 'filter-store', 'object': 2}, 'data'),
          prevent_initial_call=True)
def open_o2o_filter_modal(n_clicks, o2o_tables, o1_filter_data, o2_filter_data):
    if not ctx.triggered_id or not any(
            ctx.triggered_id['object'] == inputs['id']['object']
            and n_clicks[i] for i, inputs in enumerate(ctx.inputs_list[0])):
        return no_update

    object_id = ctx.triggered_id['object']
    filter_values = o1_filter_data if object_id == 1 else o2_filter_data

    filters = []
    filter_id = 0
    filter_column_options = []
    for json_table in reversed(o2o_tables):
        tables_options, columns_options = globals.get_table_column_options(json_table)
        filter_column_options.extend(columns_options)

    for dict_filter in filter_values:
        filters.append(get_filter_form(filter_column_options, filter_id, dict_filter))
        filter_id += 1

    filters.append(get_filter_form(filter_column_options, filter_id, None))

    filter_buttons = html.Div([
        dbc.Button('Add Filter', id={'type': 'filter-add'},
                   n_clicks=0, style=globals.BUTTONS_STYLE, color='primary'),
        dbc.Button('Finish', id={'type': 'filter-finish', 'object': object_id},
                   n_clicks=0, style=globals.BUTTONS_STYLE, color='success')
    ])

    return True, filters, filter_buttons


@callback(Output({'type': 'filter-store', 'object': MATCH}, 'data'),
          Input({'type': 'filter-finish', 'object': MATCH}, 'n_clicks'),
          State({'type': 'filter-column', 'filter_id': ALL}, 'value'),
          State({'type': 'filter-operator', 'filter_id': ALL}, 'value'),
          State({'type': 'filter-value-type', 'filter_id': ALL}, 'value'),
          State({'type': 'filter-static-value', 'filter_id': ALL}, 'value'),
          State({'type': 'filter-column-value', 'filter_id': ALL}, 'value'),
          prevent_initial_call=True)
def finish_o2o_filter(n_clicks, list_column, list_operator, list_value_type, list_sta_value, list_col_value):
    if not n_clicks:
        return no_update

    filter_data = []

    for i, col_state in enumerate(ctx.states_list[0]):
        if list_column[i]:
            dict_filter = {'column': list_column[i],
                           'operator': list_operator[i],
                           'value_type': list_value_type[i]}
            if list_value_type[i]:
                dict_filter['value'] = list_sta_value[i]
            else:
                dict_filter['value'] = list_col_value[i]
            filter_data.append(dict_filter)

    return filter_data


@callback(Output({'type': 'filter-modal'}, 'is_open', allow_duplicate=True),
          Input({'type': 'filter-store', 'object': ALL}, 'data'),
          prevent_initial_call=True)
def close_o2o_filter_modal(data):
    if not ctx.triggered_id:
        return no_update
    return False
