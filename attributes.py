from dash import html, dcc, Output, Input, State, callback, Patch, MATCH, ALL, no_update, ctx
import dash_bootstrap_components as dbc

import globals


@callback(Output({'type': 'attribute-body'}, 'children'),
          Output({'type': 'attribute-footer'}, 'children'),
          Output({'type': 'attribute-modal'}, 'is_open'),
          Input({'type': 'open-attributes', 'subject': ALL, 'index': ALL}, 'n_clicks'),
          State({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'value'),
          State({'type': 'tables', 'subject': 'event-types', 'index': 0}, 'value'),
          State({'type': 'tables', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'attributes-store', 'subject': 'event-types', 'index': 0}, 'data'),
          State({'type': 'attributes-store', 'subject': 'objects', 'index': ALL}, 'data'),
          prevent_initial_call=True)
def open_attribute_modal(n_clicks, timestamp_table, event_type_tables, list_object_type_tables, event_attributes,
                         list_object_attributes):
    if not ctx.triggered_id or not any(
            ctx.triggered_id['subject'] == inputs['id']['subject'] and ctx.triggered_id['index'] == inputs['id']['index']
            and n_clicks[i] for i, inputs in enumerate(ctx.inputs_list[0])):
        return no_update

    subject = ctx.triggered_id['subject']
    index = ctx.triggered_id['index']
    attributes_value = event_attributes

    object_type_tables = []
    if subject == 'objects':
        for j, ot in enumerate(ctx.states_list[2]):
            if index == ot['id']['index']:
                object_type_tables = list_object_type_tables[j]
                attributes_value = list_object_attributes[j] if list_object_attributes else []

    attribute_column_options = []
    for json_table in reversed([timestamp_table] + event_type_tables + object_type_tables):
        tables_options, columns_options = globals.get_table_column_options(json_table)
        attribute_column_options.extend(columns_options)

    attribute_selection = []
    for i, label in enumerate(attributes_value):
        attribute_selection.append(
            dbc.Row([
                dbc.Input(id={'type': 'attribute-label', 'attribute-index': i}, type='text', value=label),
                dcc.Dropdown(options=attribute_column_options, value=attributes_value[label],
                             id={'type': 'attribute-options', 'attribute-index': i})
            ]))

    attribute_selection.append(
        dbc.Row([
            dbc.Input(id={'type': 'attribute-label', 'attribute-index': len(attributes_value) + 1}, type='text',
                      value=str(index)),
            dcc.Dropdown(options=attribute_column_options, value=attributes_value,
                         id={'type': 'attribute-options', 'attribute-index': index}, multi=True)
        ]))

    body = attribute_selection

    footer = html.Div([
        dbc.Button('Add another Attribute', id={'type': 'add-attribute'},
                   n_clicks=0, style=globals.BUTTONS_STYLE, color='primary'),
        dbc.Button('Finish', id={'type': 'attribute-finish', 'subject': subject, 'index': index},
                   n_clicks=0, style=globals.BUTTONS_STYLE, color='success')
    ])

    return body, footer, True


@callback(Output({'type': 'attribute-body'}, 'children', allow_duplicate=True),
          Input({'type': 'add-attribute'}, 'n_clicks'),
          State({'type': 'attribute-options', 'attribute-index': ALL}, 'options'),
          prevent_initial_call=True)
def add_attribute(n_clicks, list_attribute_options):
    if not n_clicks:
        return no_update

    index = ctx.states_list[0][0]['id']['index']
    attribute_column_options = list_attribute_options[0]  # all have the same column options

    attribute_selection = dbc.Row([
        dbc.Input(id={'type': 'attribute-label', 'attribute-index': index}, type='text', value=str(index)),
        dcc.Dropdown(options=attribute_column_options,
                     id={'type': 'attribute-options', 'attribute-index': index}, multi=True)
    ])

    patched_body = Patch()
    patched_body.append(attribute_selection)

    return patched_body


@callback(Output({'type': 'attributes-store', 'subject': MATCH, 'index': MATCH}, 'data'),
          Input({'type': 'attribute-finish', 'subject': MATCH, 'index': MATCH}, 'n_clicks'),
          State({'type': 'attribute-options', 'attribute-index': ALL}, 'value'),
          State({'type': 'attribute-label', 'attribute-index': ALL}, 'value'),
          prevent_initial_call=True)
def finish_selecting_attributes(n_clicks, attribute_columns, attribute_labels):
    if not n_clicks:
        return no_update

    attribute_dict = {}

    for i, label in enumerate(attribute_labels):
        attribute_dict[label] = attribute_columns[i]

    return attribute_dict


@callback(Output({'type': 'attribute-modal'}, 'is_open', allow_duplicate=True),
          Input({'type': 'attributes-store', 'subject': ALL, 'index': ALL}, 'data'),
          prevent_initial_call=True)
def close_attribute_modal(data):
    if not ctx.triggered_id:
        return no_update
    return False

