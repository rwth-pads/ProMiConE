from dash import html, dcc, Output, Input, State, callback, ALL, MATCH, no_update, ctx
import dash_bootstrap_components as dbc
import json
import globals
from extraction import get_table_label_dict, build_event_type_sql


@callback(Output('labeling-div', 'children'),
          Output('labeling-div', 'hidden'),
          Output('div-add', 'hidden', allow_duplicate=True),
          Output({'type': 'show-iteration-options', 'iteration': ALL}, 'hidden', allow_duplicate=True),
          Output({'type': 'set-labels', 'iteration': ALL}, 'n_clicks'),
          Input({'type': 'set-labels', 'iteration': ALL}, 'n_clicks'),
          State({'type': 'store', 'iteration': ALL}, 'data'),
          State('update-max-rows', 'value'),
          prevent_initial_call=True)
def initiate_labeling(n_clicks_all, iteration_data_all, max_fetched_rows):
    iteration_id = ctx.triggered_id['iteration']

    if not n_clicks_all[iteration_id] or iteration_data_all[iteration_id]['removed']:
        return no_update

    max_fetched_rows = 0
    iteration_data = iteration_data_all[iteration_id]

    (timestamp_table, timestamp_column, timestamp_data, event_tables, event_columns, use_event_columns, replacement_event_label,
     event_labels, event_filter, event_attributes, object_type_label, objects_list_tables, objects_list_columns,
     use_object_columns, replacement_object_label, object_filters, object_attributes) = globals.iteration_data_to_inputs(iteration_data)

    # get table labels to avoid ambiguities
    list_tables = [json.loads(t) for t in [timestamp_table] + event_tables + objects_list_tables[0]]
    inner_table_keys = [tuple(json.loads(t)['path']) for t in [timestamp_table] + event_tables]
    tb_labels, inner_columns = get_table_label_dict(list_tables, inner_table_keys)

    sql_inner, len_event_columns = build_event_type_sql(timestamp_table, timestamp_column, timestamp_data, event_tables,
                                                        event_columns, use_event_columns, replacement_event_label,
                                                        event_labels, event_filter, event_attributes, object_type_label[0],
                                                        objects_list_tables[0], objects_list_columns[0],
                                                        use_object_columns[0], replacement_object_label[0],
                                                        object_filters[0], object_attributes[0], tb_labels,
                                                        inner_columns, max_fetched_rows, None, None)
    # object values are not used

    # select statement
    sql_select = f"SELECT DISTINCT "

    sql_event_columns = ', '.join(['inner."event_type' + str(i) + '"' for i in range(len_event_columns)])
    sql_from = f"\nFROM ({sql_inner}) inner"

    sql = f"{sql_select}{sql_event_columns}{sql_from}"

    # database credentials
    conn = globals.get_connection()

    print(f"Executing: {sql}")
    c = conn.cursor()
    c.execute(sql)

    event_types = []
    fetched_rows = c.fetchmany(200)
    while fetched_rows:
        for row in fetched_rows:
            event_type = ' '.join(map(str, row[0:len_event_columns]))
            event_types.append(event_type)

        fetched_rows = c.fetchmany(200)

    globals.close_connection(conn)
    print('SQL parsed!')

    labels = list(set(event_types))  # unique event types

    # construct labeling form
    labeling_form = []
    provided_event_types = []
    if globals.provided_ocpn:
        provided_event_types = list(globals.provided_ocpn['activities'])  # disregards generic start and end activities

    for i, tb_labeled in enumerate(labels):
        labeling_form.append(
            dbc.Row([
                dbc.Col(html.Div(f"Set label for {tb_labeled}:")),
                dbc.Col([
                    dcc.Dropdown(options=provided_event_types,
                                 id={'type': 'typing-dropdown', 'label': str(tb_labeled), 'object': ''}, multi=False),
                    dcc.Store(id={'type': 'typing-dropdown-helper', 'label': str(tb_labeled), 'object': ''},
                              storage_type='memory', data='')
                ])
            ])
        )
    labeling_form.append(dbc.Col(dbc.Button('Confirm Labels', id={'type': 'confirm-labels', 'iteration': iteration_id},
                                            color='success'), align='end'))
    return labeling_form, False, True, [True for f in ctx.outputs_list[3]], [0 for f in ctx.outputs_list[4]]


@callback(Output({'type': 'store', 'iteration': ALL}, 'data', allow_duplicate=True),
          Output('labeling-div', 'children', allow_duplicate=True),
          Output('div-add', 'hidden', allow_duplicate=True),
          Output({'type': 'show-iteration-options', 'iteration': ALL}, 'hidden', allow_duplicate=True),
          Output('div-store-configuration', 'hidden', allow_duplicate=True),
          Input({'type': 'confirm-labels', 'iteration': ALL}, 'n_clicks'),
          State({'type': 'store', 'iteration': ALL}, 'data'),
          State({'type': 'typing-dropdown', 'label': ALL, 'object': ''}, 'value'),
          prevent_initial_call=True)
def finish_labeling(n_clicks, all_iteration_data, label_values):
    iteration_id = ctx.triggered_id['iteration']
    if not n_clicks[0]:  # only 1 confirm button is there at a time iteration is only used to link to iteration-store
        return no_update
    iteration_data = all_iteration_data[iteration_id]

    dict_label = {}
    for i, label_data in enumerate(ctx.states_list[1]):  # label_values
        if label_values[i]:
            dict_label[label_data['id']['label']] = label_values[i]

    iteration_data['event_labels'] = dict_label

    output = []
    for tb_output in ctx.outputs_list[0]:
        output_id = tb_output['id']['iteration']
        if output_id == iteration_id:
            output.append(iteration_data)
        else:
            output.append(all_iteration_data[output_id])

    return output, [], False, [False for f in ctx.outputs_list[3]], False
