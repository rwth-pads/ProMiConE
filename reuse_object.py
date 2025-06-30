from dash import html, dcc, Output, Input, State, callback, Patch, MATCH, ALL, no_update, ctx
import dash_bootstrap_components as dbc
import json

import globals
from navigation import get_table_column_options
from datasource_managment import get_object_form_filled


def get_candidate_tables(timestamp_table, event_tables):
    candidates = [json.loads(timestamp_table)]
    if event_tables:
        # order event_tables
        loaded_event_tables = [json.loads(t) for t in event_tables]
        while loaded_event_tables:  # add all event tables as candidates
            for t in loaded_event_tables:
                pk_name, pk_table, pk_columns, fk_name, fk_table, fk_columns = globals.dict_keypair[
                    t['keypair']]
                table_candidates_names = [t['table'] for t in candidates]
                if pk_table in table_candidates_names or fk_table in table_candidates_names:
                    candidates.append(t)  # join exists so we can add this as valid candidate
                    loaded_event_tables.remove(t)
    return candidates


@callback(Output({'type': 'existing-object-modal'}, 'options'),
          Output({'type': 'object-modal'}, 'is_open'),
          Input({'type': 'copy-objects', 'dummy': 0}, 'n_clicks'),
          State({'type': 'store', 'iteration': ALL}, 'data'),
          prevent_initial_call=True)
def initialize_object_modal(n_clicks, all_iteration_data):
    if not n_clicks:
        return no_update

    object_options = []

    for j, iteration in enumerate(all_iteration_data):
        if not iteration['removed']:
            (timestamp_table, timestamp_column, timestamp_data,
             event_tables, event_columns, use_event_columns, replacement_event_label, event_labels, event_filter,
             event_attributes, object_type_label, objects_list_tables, objects_list_columns, use_object_columns,
             replacement_object_label, object_filters, object_attributes) = globals.iteration_data_to_inputs(iteration)

            # filter and attributes are ignored
            for i, object_label in enumerate(object_type_label):
                dict_object = {'label': object_label,
                               'tables': objects_list_tables[i],
                               'columns': objects_list_columns[i],
                               'use_object_columns': use_object_columns[i],
                               'replacement': replacement_object_label[i]}
                candidate_tables = get_candidate_tables(timestamp_table, event_tables)
                available_tables = set([json.loads(tab)['table'] for tab in dict_object['tables']])
                required_tables = set([json.loads(col)['table'] for col in dict_object['columns']])
                while not required_tables.issubset(available_tables):
                    t = candidate_tables.pop()
                    dict_object['tables'].append(t)
                    available_tables.add(json.loads(t)['table'])
                object_options.append((dict_object, j))

    options = [{'label': f"{o['label']} - Data Source {j}", 'value': json.dumps(o)} for (o, j) in object_options]

    return options, True


def breadth_first_table_search(target_table_set, list_current_tables):  # 'path' -> 'search-path'
    que = [{'table': t['table'], 'search-path': [], 'path': t['path'], 'info': t['info']} for t in list_current_tables]
    max_search_depth = 3
    iteration = 0
    while iteration < max_search_depth:
        exploring = que.pop(0)
        table_index = globals.df_tables.index[globals.df_tables['Table'] == exploring['table']].values[0]
        for i in range(len(globals.relation_matrix[table_index])):
            keypair = globals.relation_matrix[table_index][i]
            if keypair < 0:  # multiple joins exist
                t = globals.df_tables.iloc[i]['Table']
                # take the first one arbitrarily since we can not tell which one would be preferred
                key = globals.dict_multi_relation_keys[str(keypair)][0]
                join_label = globals.dict_keypair[str(key)][3] if globals.dict_keypair[str(key)][3] else ', '.join(
                    globals.dict_keypair[str(key)][5])
                exploring['search-path'].append({'table': t, 'keypair': str(key), 'path': exploring['path'] + [key],
                                                 'info': exploring['info'] + [join_label]})
                if t in target_table_set:
                    return exploring['search-path'], t
                else:
                    que.append({'table': t, 'search-path': exploring['search-path'], 'path': exploring['path'] + [key],
                                'info': exploring['info'] + [join_label]})
            elif keypair:  # key relationship exists
                t = globals.df_tables.iloc[i]['Table']
                exploring['search-path'].append(
                    {'table': t, 'keypair': str(keypair), 'path': exploring['path'] + [keypair],
                     'info': exploring['info']})
                if t in target_table_set:
                    return exploring['search-path'], t
                else:
                    que.append(
                        {'table': t, 'search-path': exploring['search-path'], 'path': exploring['path'] + [keypair],
                         'info': exploring['info']})
    return None, None


@callback(Output({'type': 'info-object-modal'}, 'children'),
          Input({'type': 'existing-object-modal'}, 'value'),
          State({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'value'),
          State({'type': 'tables', 'subject': 'event-types', 'index': 0}, 'value'),
          prevent_initial_call=True)
def select_option_object_modal(json_existing_object_type, timestamp_table, event_type_tables):
    if not json_existing_object_type:
        return no_update

    existing_object_type = json.loads(json_existing_object_type)
    # find path
    datasource_tables = [json.loads(timestamp_table)] + [json.loads(et) for et in event_type_tables]
    # begin with most recently added table
    object_tables = set([json.loads(t)['table'] for t in existing_object_type['tables']])

    join_path, contact_table = breadth_first_table_search(object_tables, datasource_tables)

    if existing_object_type['use_object_columns']:
        col_string_list = []
        for json_col in existing_object_type['columns']:
            col = json.loads(json_col)
            col_string_list.append(f"{col['table']}.{col['column']}")
        col_string = f"Object Columns: {', '.join(col_string_list)}"
    else:
        col_string = f"All lines contain the same object labeled '{existing_object_type['replacement']}'."

    if join_path:
        info_box = html.Div([
            html.Pre(f"Object Type: {existing_object_type['label']}\n"
                     f"{col_string}\n"
                     f"Object Tables: {', '.join([json.loads(json_t)['table'] for json_t in existing_object_type['tables']])}\n"
                     f"Suggested tables to connect to the data source:\n{', '.join([t['table'] for t in join_path])}"),
            dcc.Store(data={'join-path': join_path, 'contact': contact_table}, id={'type': 'value-object-modal'}),
            html.Div([  # dummy in this case
                dcc.Dropdown(id={'type': 'dropdown-object-modal'}),
                dbc.Alert('', color='danger', id={'type': 'warning-object-modal'})
            ], hidden=True)
        ])
    else:
        info_box = html.Div([
            html.Pre(f"Object Type: {existing_object_type['label']}\n"
                     f"{col_string}\n"
                     f"Object Tables: {', '.join([json.loads(json_t)['table'] for json_t in existing_object_type['tables']])}\n"
                     f"Could not connect object type to already selected tables!"),
            'Connect to the object type tables:',
            dcc.Dropdown(id={'type': 'dropdown-object-modal'}),
            dbc.Alert('', color='danger', id={'type': 'warning-object-modal'}),
            dcc.Store(id={'type': 'value-object-modal'})  # dummy in this case
        ])

    return info_box


@callback(Output({'type': 'confirm-object-modal'}, 'disabled'),
          Input({'type': 'value-object-modal'}, 'data'))
def enable_modal_confirm(data):
    if not data:
        return no_update
    return False


@callback(Output({'type': 'dropdown-object-modal'}, 'options'),
          Input({'type': 'dropdown-object-modal'}, 'value'),
          State({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'value'),
          State({'type': 'tables', 'subject': 'event-types', 'index': 0}, 'value'),
          State({'type': 'value-object-modal'}, 'data')
          )
def dropbox_options_modal(object_tables, timestamp_table, event_tables, object_data):
    if object_data:  # path was found - no inputs needed
        return no_update

    options = []
    for table in reversed([timestamp_table] + event_tables + object_tables):
        tables_options, columns_options = get_table_column_options(table)
        options.append(tables_options)

    return options


@callback(Output({'type': 'warning-object-modal'}, 'color'),
          Output({'type': 'confirm-object-modal'}, 'disabled', allow_duplicate=True),
          Input({'type': 'dropdown-object-modal'}, 'value'),
          State({'type': 'existing-object-modal'}, 'value'),
          State({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'value'),
          State({'type': 'tables', 'subject': 'event-types', 'index': 0}, 'value'),
          State({'type': 'value-object-modal'}, 'data'),
          prevent_initial_call=True)
def validate_object_connection(json_explored_object_tables, json_object_type, timestamp_table, event_tables,
                               object_data):
    if object_data:  # path was found - no inputs needed
        return no_update

    object_type = json.loads(json_object_type)
    extra_object_tables = [json.loads(ot) for ot in json_explored_object_tables]

    target_object_tables = set(object_type['tables'])
    explored_tables = set([json.loads(timestamp_table)['table']] +
                          [json.loads(t)['table'] for t in event_tables] +
                          extra_object_tables)

    if not target_object_tables.intersection(explored_tables):
        return no_update
    else:
        return 'success', False


def inductive_swap_keypair(current_table, unknown_tree_tables, stable_tree_tables):
    search = ''
    if not unknown_tree_tables:  # no further tables required to be swapped
        return []

    if current_table in [json.loads(t) for t in unknown_tree_tables]:
        return unknown_tree_tables.drop(current_table)  # drop such that tables is not added twice

    if 'keypair' in current_table.keys():  # table joins a different table -> need to reorder joins
        pk_name, pk_table, pk_columns, fk_name, fk_table, fk_columns = globals.dict_keypair[current_table['keypair']]
        if pk_table == current_table['table']:
            search = fk_table
        elif fk_table == current_table['table']:
            search = pk_table

        # only shortcut
        if search in stable_tree_tables:  # table was ordered correctly already
            return unknown_tree_tables  # since contact point is ordered correctly - all are ordered correctly

        for json_table in unknown_tree_tables:
            table = json.loads(json_table)
            if search == table['table']:  # table did join another table, and we have to reorder that tables join
                new_table_list = [json.dumps(
                    {'table': search, 'keypair': current_table['keypair'], 'path': table['path'], 'info': []})]

                inductive_table_list = inductive_swap_keypair(search, unknown_tree_tables.drop(json_table),
                                                              stable_tree_tables.add(current_table['table']))

                return new_table_list.extend(inductive_table_list)
    # table is independent, and we can just ignore the keypair or
    # table had join to table that is not part of current data source thus can be ignored
    return unknown_tree_tables


@callback(Output({'type': 'objects-box', 'dummy': 0}, 'children', allow_duplicate=True),
          Output({'type': 'object-modal'}, 'is_open', allow_duplicate=True),
          Input({'type': 'confirm-object-modal'}, 'n_clicks'),
          State({'type': 'object-label', 'subject': 'objects', 'index': ALL}, 'value'),
          State({'type': 'existing-object-modal'}, 'value'),
          State({'type': 'value-object-modal'}, 'data'),
          State({'type': 'dropdown-object-modal'}, 'value'),
          State({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'value'),
          State({'type': 'tables', 'subject': 'event-types', 'index': 0}, 'value'),
          prevent_initial_call=True)
def confirm_object_modal(n_clicks_confirm, all_object_labels,
                         json_existing_object, searched_join_path, user_join_path, timestamp_table, event_tables):
    if not n_clicks_confirm:
        return no_update

    obj_id = len(ctx.states_list[0])

    existing_object = json.loads(json_existing_object)

    loaded_existing_object_tables = [json.loads(t)['table'] for t in existing_object['tables']]
    # load join path
    contact_table = ''
    if searched_join_path:
        join_path = searched_join_path['join-path']
        contact_table = searched_join_path['contact']
    else:  # user_join_path
        join_path = [json.loads(json_table) for json_table in user_join_path]
        # search contact table
        for table in join_path:
            if table['table'] in loaded_existing_object_tables:
                contact_table = table['table']

    # get contact table dict
    contact_point = ''
    for json_table in existing_object['tables']:
        table = json.loads(json_table)
        if table['table'] == contact_table:
            contact_point = table
            existing_object['tables'].remove(json_table)

    stable_tree_tables = set([json.loads(timestamp_table)['table']] + [json.loads(et)['table'] for et in event_tables]
                             + [jp['table'] for jp in join_path])

    # join path is ordered correctly since it is explored from the database source tables
    # but object_tables might be ordered in the wrong direction
    updated_object_tables = inductive_swap_keypair(contact_point, existing_object['tables'], stable_tree_tables)

    needed_object_tables = join_path + [json.loads(t) for t in updated_object_tables]
    needed_object_columns = [json.loads(c) for c in existing_object['columns']]

    object_tables = []
    object_columns = []
    object_columns_options = []
    object_tables_options = []

    correct_tables = [timestamp_table] + event_tables
    while (needed_object_tables or needed_object_columns) and correct_tables:
        json_table = correct_tables.pop(0)
        # for json_table in all_tables:
        tables_options, columns_options = get_table_column_options(json_table)

        # replace object table/column values to be consistent with path value
        if needed_object_tables:
            loaded_tables_options = [json.loads(t['value']) for t in tables_options]
            for ot in needed_object_tables:
                for lt in loaded_tables_options:
                    if ot['table'] == lt['table'] and ot['keypair'] == lt['keypair']:
                        object_tables.append(json.dumps(lt))
                        correct_tables.append(json.dumps(lt))
                        needed_object_tables.remove(ot)
                        break

        if needed_object_columns:
            loaded_columns_options = [json.loads(c['value']) for c in columns_options]
            for oc in needed_object_columns:
                for lc in loaded_columns_options:
                    if oc['table'] == lc['table'] and oc['column'] == lc['column']:
                        object_columns.append(json.dumps(lc))
                        needed_object_columns.remove(oc)
                        break

        object_tables_options.extend(tables_options)
        object_columns_options.extend(columns_options)

    # construct form and fill values
    object_element = get_object_form_filled(obj_id, existing_object['label'],
                                            object_tables_options, object_tables,
                                            object_columns_options, object_columns,
                                            existing_object['use_object_columns'], existing_object['replacement'],
                                            '', '')

    # patch object form
    patched_objects = Patch()
    patched_objects.append(object_element)

    return patched_objects, False
