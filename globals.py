import pandas as pd
import numpy as np
import cx_Oracle
import sqlite3
import json
import datetime

timestamp_format = '%Y-%m-%d %H:%M:%S'  # python format string of extracted timestamps
oracle_timestamp_format = 'YYYY-MM-DD HH24:MI:SS'  # format string used by oracle

dict_credentials = {}  # ip, port, service, user, pw
df_tables = pd.DataFrame()  # dataframe of tables ['Table', 'NumRows', 'DateCol', 'SuggestedObject', 'Description']
dict_table_columns = {}  # table column dictionary (column, datatype, col_desc)
dict_keypair = {}  # dictionary of key pairs:
#                    dict_keypair[str(fk_id)] = (pk_name, pk_table, pk_columns, fk_name, fk_table, fk_columns)
relation_matrix = np.zeros(shape=(2, 2))  # relationship matrix: 0: no relation
#                                                            fk-id: referring to dict_keypair
dict_multi_relation_keys = {}  # stores a list of keypair in cases where multiple options are available for a join
provided_ocpn = {}
extracted_ocpn = {}
ocpn_width = 1160
ocpn_height = 380
next_o2o_id = 0

enabled_event_radio_options = (
    {'label': 'All columns contain the same event type', 'value': False, 'disabled': False},
    {'label': 'Select columns:', 'value': True, 'disabled': False})
disabled_event_radio_options = (
    {'label': 'All columns contain the same event type', 'value': False, 'disabled': True},
    {'label': 'Select columns:', 'value': True, 'disabled': True})

enabled_object_radio_options = ({'label': 'All columns contain the same object (not object type)',
                                 'value': False, 'disabled': False},
                                {'label': 'Select columns:', 'value': True, 'disabled': False})
disabled_object_radio_options = ({'label': 'All columns contain the same object (not object type)',
                                  'value': False, 'disabled': True},
                                 {'label': 'Select columns:', 'value': True, 'disabled': True})

# CSS Container
BUTTONS_STYLE = {
    "margin": "0.25rem 0.25rem",
}

UPLOAD_STYLE = {
    # "width": "100%",
    "height": "60px",
    "lineHeight": "60px",
    "borderWidth": "1px",
    "borderStyle": "dashed",
    "borderRadius": "5px",
    "textAlign": "center",
    "margin": "10px",
}


def get_connection():
    if dict_credentials['db-type'] == 'OracleEBS':
        dsn_tns = cx_Oracle.makedsn(dict_credentials['ip'], dict_credentials['port'],
                                    service_name=dict_credentials['service'])
        conn = cx_Oracle.connect(user=dict_credentials['user'], password=dict_credentials['pw'], dsn=dsn_tns)
    elif dict_credentials['db-type'] == 'SQLite':
        conn = sqlite3.connect(dict_credentials['database'])
    else:
        conn = None
    return conn


def close_connection(conn):
    if dict_credentials['db-type'] == 'OracleEBS':
        conn.close()
    elif dict_credentials['db-type'] == 'SQLite':
        conn.close()
    else:
        conn.close()


def get_timestamp_filter(column, operator, value: datetime.datetime):
    if dict_credentials['db-type'] == 'OracleEBS':
        return f"{column} {operator} to_timestamp('{value.strftime(timestamp_format)}', '{oracle_timestamp_format}')"
    elif dict_credentials['db-type'] == 'SQLite':
        return f"{column} {operator} '{value.strftime(timestamp_format)}'"
    else:
        return f"{column} {operator} {value.strftime(timestamp_format)}"


def validate_iteration_data(iteration_data):  # used for backwards compatibility
    if 'timestamp_data' not in iteration_data:
        iteration_data['timestamp_data'] = ''
    if 'event_filter' not in iteration_data:
        iteration_data['event_filter'] = []
    if 'event_attributes' not in iteration_data:
        iteration_data['event_attributes'] = []
    if 'object_filters' not in iteration_data:
        iteration_data['object_filters'] = [[] for o in iteration_data['object_type_label']]
    if 'object_attributes' not in iteration_data:
        iteration_data['object_attributes'] = [[] for o in iteration_data['object_type_label']]

    return iteration_data


def inputs_to_iteration_data(timestamp_table, timestamp_column, timestamp_data, event_tables, event_columns, use_event_columns,
                             replacement_event_label, event_labels, event_filter, event_attributes,
                             object_type_label, objects_list_tables, objects_list_columns, use_object_columns,
                             replacement_object_label, object_filters, object_attributes, removed):
    iteration_data = {'timestamp_table': timestamp_table,
                      'timestamp_column': timestamp_column,
                      'timestamp_data': timestamp_data,
                      'event_tables': event_tables,
                      'event_columns': event_columns,
                      'use_event_columns': use_event_columns,
                      'replacement_event_label': replacement_event_label,
                      'event_labels': event_labels,
                      'event_filter': event_filter,
                      'event_attributes': event_attributes,
                      'object_type_label': object_type_label,
                      'objects_list_tables': objects_list_tables,
                      'objects_list_columns': objects_list_columns,
                      'use_object_columns': use_object_columns,
                      'replacement_object_label': replacement_object_label,
                      'object_filters': object_filters,
                      'object_attributes': object_attributes,
                      'removed': removed}

    return iteration_data


def iteration_data_to_inputs(iteration_data):
    timestamp_table = iteration_data['timestamp_table']
    timestamp_column = iteration_data['timestamp_column']
    timestamp_data = iteration_data['timestamp_data']
    event_tables = iteration_data['event_tables']
    event_columns = iteration_data['event_columns']
    use_event_columns = iteration_data['use_event_columns']
    event_labels = iteration_data['event_labels']
    event_filter = iteration_data['event_filter']
    event_attributes = iteration_data['event_attributes']
    replacement_event_label = iteration_data['replacement_event_label']
    object_type_label = iteration_data['object_type_label']
    objects_list_tables = iteration_data['objects_list_tables']
    objects_list_columns = iteration_data['objects_list_columns']
    use_object_columns = iteration_data['use_object_columns']
    replacement_object_label = iteration_data['replacement_object_label']
    object_attributes = iteration_data['object_attributes']
    object_filters = iteration_data['object_filters']

    return (timestamp_table, timestamp_column, timestamp_data, event_tables, event_columns, use_event_columns, replacement_event_label,
            event_labels, event_filter, event_attributes, object_type_label, objects_list_tables, objects_list_columns,
            use_object_columns, replacement_object_label, object_filters, object_attributes)


def iteration_data_to_info(iteration_data):
    (timestamp_table, timestamp_column, timestamp_data, event_tables, event_columns, use_event_columns,
     replacement_event_label, event_labels, event_filter, event_attributes, object_type_label, objects_list_tables, objects_list_columns,
     use_object_columns, replacement_object_label, object_filters, object_attributes) = iteration_data_to_inputs(iteration_data)

    iteration_summary = ''
    iteration_summary += f"Timestamp table: {json.loads(timestamp_table)['table']}\nTimestamp column: "
    # for t_col in t_columns:
    iteration_summary += f"{json.loads(timestamp_column)['table']}.{json.loads(timestamp_column)['column']} "

    iteration_summary += f"\nEvent-Type tables: "
    iteration_summary += ', '.join([json.loads(e_table)['table'] for e_table in event_tables])
    iteration_summary += f"\nEvent-Type columns: "
    if use_event_columns:
        iteration_summary += ', '.join(
            [f"{json.loads(e_column)['table']}.{json.loads(e_column)['column']}" for e_column in event_columns])
    else:
        iteration_summary += f"\n '{replacement_event_label}'"

    for obj_id, object_tables in enumerate(objects_list_tables):
        if not object_type_label[obj_id]:
            object_type_label[obj_id] = str(obj_id)
        iteration_summary += f"\nObject-Type {object_type_label[obj_id]} tables: "
        iteration_summary += ', '.join([json.loads(o_table)['table'] for o_table in object_tables])
        iteration_summary += f"\nObject-Type {object_type_label[obj_id]} columns: "
        if use_object_columns[obj_id]:
            iteration_summary += ', '.join(
                [f"{json.loads(o_column)['table']}.{json.loads(o_column)['column']}" for o_column in
                 objects_list_columns[obj_id]])
        else:
            iteration_summary += f"\n '{replacement_object_label[obj_id]}'"

    return iteration_summary


def o2o_dict_to_summary(o2o_dict):
    info_string = (
        f"{o2o_dict['object_type_1']} Columns: {', '.join(json.loads(c)['table'] + '.' + json.loads(c)['column'] for c in o2o_dict['o2o_columns_1'])}\n"
        f"{o2o_dict['object_type_2']} Columns: {', '.join(json.loads(c)['table'] + '.' + json.loads(c)['column'] for c in o2o_dict['o2o_columns_2'])}\n"
        f"with Tables: {', '.join(json.loads(t)['table'] for t in o2o_dict['o2o_tables'])}"
        )
    return info_string


def enforced_o2o_dict_to_summary(dict_enforced_o2o):
    info_string = f"{dict_enforced_o2o['dominant_object']} dominates with the {dict_enforced_o2o['enforced_o2o'][0]} - {dict_enforced_o2o['enforced_o2o'][1]} relation"
    return info_string


def get_table_column_options(json_table):
    tables_options = []
    columns_options = []

    if json_table:
        table = json.loads(json_table)
        table_index = df_tables.index[df_tables['Table'] == table['table']].values[0]
        tables_options = []
        for i in range(len(relation_matrix[table_index])):
            keypair = relation_matrix[table_index][i]
            if keypair < 0:  # multiple joins exist
                for key in dict_multi_relation_keys[str(keypair)]:
                    t, nr = df_tables.iloc[i][['Table', 'NumRows']]
                    join_label = dict_keypair[str(key)][3] if dict_keypair[str(key)][3] else ', '.join(
                        dict_keypair[str(key)][5])
                    info_label = table['info']+[join_label]
                    nr_rows_label = f" - {nr} Rows"
                    val = json.dumps({'table': t, 'keypair': str(key), 'path': table['path']+[str(key)],
                                      'info': info_label})
                    tables_options.append(
                        {'label': f"{t}{' with ' + ' '.join(info_label) if any(info_label) else ''}{nr_rows_label if nr else ''}",
                         'value': val})
            elif keypair:  # key relationship exists
                t, nr = df_tables.iloc[i][['Table', 'NumRows']]
                val = json.dumps({'table': t, 'keypair': str(keypair), 'path': table['path']+[str(keypair)],
                                  'info': table['info']})
                nr_rows_label = f" - {nr} Rows"
                tables_options.append({'label': f"{t}{' with ' + ' '.join(table['info']) if any(table['info']) else ''}{nr_rows_label if nr else ''}", 'value': val})

        for column, datatype, col_desc in dict_table_columns[table['table']]:
            val = json.dumps({'table': table['table'], 'column': column, 'table-path': table['path']})
            columns_options.append({'label': f"{table['table']}.{column} - Datatype: {datatype}", 'value': val})

    return tables_options, columns_options


def get_label_value_base(mode):
    table_options = []
    if mode == 'timestamps':
        sug_timestamp_tables = df_tables.sort_values(
            by=['DateCol', 'NumRows', 'Table'],
            ascending=[False, False, True])[['Table', 'NumRows', 'DateCol']].reset_index().values.tolist()

        table_options = [
            {'label': f"{t}{' - ' + str(nr) + ' Rows' if nr else ''}{' - Timestamp Column' if d else ''}",
             'value': json.dumps({'table': t, 'path': [str(i)], 'info': []})} for i, t, nr, d in sug_timestamp_tables]
    elif mode == 'objects':
        sug_object_tables = df_tables.sort_values(
            by=['SuggestedObject', 'NumRows', 'Table'],
            ascending=[False, False, True])[['Table', 'NumRows', 'SuggestedObject']].reset_index().values.tolist()
        table_options = [
            {'label': f"{t}{'- ' + str(nr) + ' Rows' if nr else ''}{' - Suggested' if o else ''}",
             'value': json.dumps({'table': t, 'path': [str(i)], 'info': []})} for i, t, nr, o in sug_object_tables]
    # else:
    return table_options
