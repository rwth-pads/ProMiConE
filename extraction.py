from dash import Output, Input, State, callback, ALL, dcc
import datetime
import pandas as pd
import json
import pm4py
import pm4py.objects.ocel.obj as pm4py_ocel_obj
import globals
import ocpn_visualization


def get_table_label_dict(list_tables, inner_table_keys):
    alph = list(map(chr, range(97, 123)))  # use only letters to be compatible with oracle
    table_label_dict = {}
    for i, table in enumerate(list_tables):
        # get label
        d = i // len(alph)
        m = i % len(alph)
        label = ''
        for j in [0] * (1 + d):
            label += alph[m]
        table_key = tuple(table['path'])
        is_inner = True if table_key in inner_table_keys else False
        table_label_dict[table_key] = {'label': label, 'join': '', 'is_inner': is_inner}

    inner_columns = []
    # remember labels for joins
    for table in list_tables:
        if 'keypair' in table:
            found = False
            for parent in list_tables:
                if table['path'][:-1] == parent['path']:
                    keypair = table['keypair']
                    pk_name, pk_table, pk_columns, fk_name, fk_table, fk_columns = globals.dict_keypair[keypair]
                    if (parent['table'] == pk_table and table['table'] == fk_table) or (
                            parent['table'] == fk_table and table['table'] == pk_table):

                        label_table = table_label_dict[tuple(table['path'])]['label']
                        label_parent = table_label_dict[tuple(parent['path'])]['label']

                        store_pk_col = False
                        store_fk_col = False
                        replacement_label = ''
                        if tuple(table['path']) not in inner_table_keys and tuple(parent['path']) in inner_table_keys:
                            replacement_label = label_parent
                            label_parent = 'inner'
                            if table['table'] == pk_table:
                                store_fk_col = True
                            else:
                                store_pk_col = True

                        pk_label = label_table if table['table'] == pk_table else label_parent
                        fk_label = label_table if table['table'] == fk_table else label_parent

                        join = f" JOIN {table['table']} {label_table} ON "
                        for i, (pk_col, fk_col) in enumerate(zip(pk_columns, fk_columns)):
                            # since columns are ordered in the schema extraction pairs match
                            pk_col_label = pk_col
                            fk_col_label = fk_col

                            if store_fk_col:
                                col_label = '"' + replacement_label + '_' + fk_col_label + '"'
                                inner_columns.append(f"{replacement_label}.{fk_col_label} AS {col_label}")
                                fk_col_label = col_label
                            elif store_pk_col:
                                col_label = '"' + replacement_label + '_' + pk_col_label + '"'
                                inner_columns.append(f"{replacement_label}.{pk_col_label} AS {col_label}")
                                pk_col_label = col_label

                            if i == 0:
                                join = f"{join}{pk_label}.{pk_col_label} = {fk_label}.{fk_col_label}"
                            else:
                                join = f"{join} AND {pk_label}.{pk_col_label} = {fk_label}.{fk_col_label}"
                        table_label_dict[tuple(table['path'])]['join'] = join
                        found = True
                        break

            if not found:  # child must be the parent - from object case
                for t in list_tables:
                    if t['path'][:-1] == table['path']:
                        keypair = t['keypair']
                        pk_name, pk_table, pk_columns, fk_name, fk_table, fk_columns = globals.dict_keypair[keypair]
                        if (t['table'] == pk_table and table['table'] == fk_table) or (
                                t['table'] == fk_table and table['table'] == pk_table):

                            pk_name, pk_table, pk_columns, fk_name, fk_table, fk_columns = globals.dict_keypair[keypair]
                            label_table = table_label_dict[tuple(table['path'])]['label']
                            label_child = table_label_dict[tuple(parent['path'])]['label']

                            store_pk_col = False
                            store_fk_col = False
                            replacement_label = ''
                            if tuple(table['path']) not in inner_table_keys and tuple(
                                    parent['path']) in inner_table_keys:
                                replacement_label = label_child
                                label_child = 'inner'
                                if table['table'] == pk_table:
                                    store_fk_col = True
                                else:
                                    store_pk_col = True

                            pk_label = label_table if table['table'] == pk_table else label_child
                            fk_label = label_table if table['table'] == fk_table else label_child

                            join = f" JOIN {table['table']} {label_table} ON "
                            for i, (pk_col, fk_col) in enumerate(zip(pk_columns, fk_columns)):
                                # since columns are ordered in the schema extraction pairs match
                                pk_col_label = pk_col
                                fk_col_label = fk_col

                                if store_fk_col:
                                    col_label = '"' + replacement_label + '_' + fk_col_label + '"'
                                    inner_columns.append(f"{replacement_label}.{fk_col_label} AS {col_label}")
                                    fk_col_label = col_label
                                elif store_pk_col:
                                    col_label = '"' + replacement_label + '_' + pk_col_label + '"'
                                    inner_columns.append(f"{replacement_label}.{pk_col_label} AS {col_label}")
                                    pk_col_label = col_label

                                if i == 0:
                                    join = f"{join}{pk_label}.{pk_col_label} = {fk_label}.{fk_col_label}"
                                else:
                                    join = f"{join} AND {pk_label}.{pk_col_label} = {fk_label}.{fk_col_label}"
                            table_label_dict[tuple(table['path'])]['join'] = join
                            break
        # else:  # is root
        #     table_label_dict[tuple(child['path'])]['join'] = ''

    return table_label_dict, inner_columns


def build_event_type_sql(timestamp_table, timestamp_column, timestamp_data, event_tables, event_columns, use_event_columns,
                         replacement_event_label, event_labels, event_filter, event_attribute_columns,
                         object_type_label, object_tables, object_columns, use_object_columns, replacement_object_label,
                         object_filters, object_attribute_columns, tb_labels, inner_columns, max_fetched_rows,
                         start_date, end_date):
    row_num_select = ''
    row_num_where = ''
    if max_fetched_rows and globals.dict_credentials['db-type'] == 'OracleEBS':
        row_num_where = f" AND ROWNUM <= {max_fetched_rows}"
    elif max_fetched_rows and globals.dict_credentials['db-type'] == 'SQLite':
        row_num_select = f" TOP {max_fetched_rows}"

    if timestamp_data:
        timestamp_column_label = f"{tb_labels[tuple(json.loads(timestamp_table)['path'])]['label']}.\"timestamp\""
        sql_table = f"({timestamp_data['timestamp_sql']})"
    else:
        timestamp_column_label = f"{tb_labels[tuple(json.loads(timestamp_column)['table-path'])]['label']}.{json.loads(timestamp_column)['column']}"
        sql_table = json.loads(timestamp_table)['table']
    sql_inner_select = f"rownum AS \"event_id\", {timestamp_column_label} AS \"timestamp\""

    if use_event_columns:
        for i, col in enumerate(event_columns):
            sql_inner_select = f"{sql_inner_select}, {tb_labels[tuple(json.loads(col)['table-path'])]['label']}.{json.loads(col)['column']} AS \"event_type{i}\""
        len_event_columns = len(event_columns)
    else:
        if replacement_event_label:
            sql_inner_select = f"{sql_inner_select}, '{replacement_event_label}' AS \"event_type0\""
        else:
            sql_inner_select = f"{sql_inner_select}, 'Event Type stored in {json.loads(timestamp_table)['table']}' AS \"event_type0\""
        len_event_columns = 1

    for i, ea_col in enumerate(event_attribute_columns):
        sql_inner_select = f"{sql_inner_select}, {tb_labels[tuple(json.loads(ea_col)['table-path'])]['label']}.{json.loads(ea_col)['column']} AS \"event_attribute{i}\""

    if use_object_columns:
        for i, col in enumerate(object_columns):
            if tb_labels[tuple(json.loads(col)['table-path'])]['is_inner']:
                table_label = tb_labels[tuple(json.loads(col)['table-path'])]['label']
                sql_inner_select = f"{sql_inner_select}, {table_label}.{json.loads(col)['column']} AS \"object_{object_type_label}_{i}\""

    # inner columns for object filter
    for i, dict_filter in enumerate(object_filters):
        # main column
        if tb_labels[tuple(json.loads(dict_filter['column'])['table-path'])]['is_inner']:
            main_table_label = tb_labels[tuple(json.loads(dict_filter['column'])['table-path'])]['label']
            sql_inner_select = f"{sql_inner_select}, {main_table_label}.{json.loads(dict_filter['column'])['column']} AS \"object_{object_type_label}_filter{i}\""
        # value column
        is_static = dict_filter['value_type']
        if not is_static and tb_labels[tuple(json.loads(dict_filter['value'])['table-path'])]['is_inner']:
            value_table_label = tb_labels[tuple(json.loads(dict_filter['value'])['table-path'])]['label']
            sql_inner_select = f"{sql_inner_select}, {value_table_label}.{json.loads(dict_filter['value'])['column']} AS \"object_{object_type_label}_value_filter{i}\""

    for i, oa_col in enumerate(object_attribute_columns):
        if tb_labels[tuple(json.loads(oa_col)['table-path'])]['is_inner']:
            table_label = tb_labels[tuple(json.loads(oa_col)['table-path'])]['label']
            sql_inner_select = f"{sql_inner_select}, {table_label}.{json.loads(oa_col)['column']} AS \"object_{object_type_label}_attribute{i}\""

    if inner_columns:
        sql_inner_select = f"{sql_inner_select}, {', '.join(inner_columns)}"

    # join statements
    sql_inner_join = ''
    for json_table in event_tables:
        tab = json.loads(json_table)
        sql_inner_join = f"{sql_inner_join}LEFT OUTER{tb_labels[tuple(tab['path'])]['join']}\n"

    sql_inner_where = f"{timestamp_column_label} IS NOT NULL"

    sql_timestamp_where = ''
    if start_date:
        sql_timestamp_where += f" AND {globals.get_timestamp_filter(timestamp_column_label, '>=', start_date)}"
    if end_date:
        sql_timestamp_where += f" AND {globals.get_timestamp_filter(timestamp_column_label, '<=', end_date)}"

    sql_event_filter = ''
    for dict_filter in event_filter:
        column = f"{tb_labels[tuple(json.loads(dict_filter['column'])['table-path'])]['label']}.{json.loads(dict_filter['column'])['column']}"
        operator = dict_filter['operator']
        is_static = dict_filter['value_type']
        if is_static:
            value = dict_filter['value']
        else:
            value = f"{tb_labels[tuple(json.loads(dict_filter['value'])['table-path'])]['label']}.{json.loads(dict_filter['value'])['column']}"
        sql_event_filter = f"{sql_event_filter} AND {column} {operator} {value}"

    # inner select statement for timestamp + event type counting
    sql_inner = (f"SELECT{row_num_select} {sql_inner_select}"
                 f"\nFROM {sql_table} {tb_labels[tuple(json.loads(timestamp_table)['path'])]['label']}\n"
                 f"{sql_inner_join}"
                 f"WHERE {sql_inner_where}{row_num_where}{sql_timestamp_where}{sql_event_filter}")

    return sql_inner, len_event_columns


def build_sql_extract_chunk(data_source_id, timestamp_table, timestamp_column, timestamp_data, event_tables,
                            event_columns, use_event_columns, replacement_event_label, event_labels, event_filter,
                            event_attributes, object_type_label,
                            object_tables, object_columns, use_object_columns, replacement_object_label, object_filters,
                            object_attributes, max_fetched_rows, start_date, end_date):
    # get table labels to avoid ambiguities
    list_tables = [json.loads(t) for t in [timestamp_table] + event_tables + object_tables]
    inner_table_keys = [tuple(json.loads(t)['path']) for t in [timestamp_table] + event_tables]
    tb_labels, inner_columns = get_table_label_dict(list_tables, inner_table_keys)

    # attributes
    len_event_attributes = []
    event_attribute_columns = []
    event_attribute_labels = []
    for ea_label in event_attributes:
        len_event_attributes.append(len(event_attributes[ea_label]))
        event_attribute_columns.extend(event_attributes[ea_label])
        event_attribute_labels.append(ea_label)
    sum_len_event_attributes = sum(len_event_attributes)

    len_object_attributes = []
    object_attribute_columns = []
    object_attribute_labels = []
    for oa_label in object_attributes:
        len_object_attributes.append(object_attributes[oa_label])
        object_attribute_columns.append(object_attributes[oa_label])
        object_attribute_labels.append(oa_label)

    # inner
    sql_inner, len_event_columns = build_event_type_sql(timestamp_table, timestamp_column, timestamp_data, event_tables, event_columns,
                                                        use_event_columns, replacement_event_label, event_labels,
                                                        event_filter, event_attribute_columns,
                                                        object_type_label, object_tables, object_columns,
                                                        use_object_columns, replacement_object_label, object_filters,
                                                        object_attribute_columns, tb_labels, inner_columns,
                                                        max_fetched_rows, start_date, end_date)

    # sql outer
    sql_event_columns = ', '.join(['inner."event_type' + str(i) + '"' for i in range(len_event_columns)])

    sql_ea_columns = ''
    if event_attributes:
        sql_ea_columns = ', ' + ', '.join(
            ['inner."event_attribute' + str(i) + '"' for i in range(sum_len_event_attributes)])
    sql_select_outer = f"SELECT inner.\"event_id\", inner.\"timestamp\", {sql_event_columns}{sql_ea_columns}"

    if use_object_columns:
        for i, col in enumerate(object_columns):
            if tb_labels[tuple(json.loads(col)['table-path'])]['is_inner']:
                sql_select_outer = f"{sql_select_outer}, inner.\"object_{object_type_label}_{i}\" AS \"object_{object_type_label}_{i}\""
            else:
                sql_select_outer = f"{sql_select_outer}, {tb_labels[tuple(json.loads(col)['table-path'])]['label']}.{json.loads(col)['column']} AS \"object_{object_type_label}_{i}\""
        len_object_columns = len(object_columns)
    else:
        if replacement_object_label:
            sql_select_outer = f"{sql_select_outer}, '{replacement_object_label}' AS \"object_{object_type_label}_0\""
        else:
            sql_select_outer = f"{sql_select_outer}, 'Object stored in {json.loads(timestamp_table)['table']}' AS \"object_{object_type_label}_0\""
        len_object_columns = 1

    for i, oa_column in enumerate(object_attribute_columns):
        if tb_labels[tuple(json.loads(oa_column)['table-path'])]['is_inner']:
            sql_select_outer = f"{sql_select_outer}, inner.\"object_{object_type_label}_attribute{i}\" AS \"object_{object_type_label}_attribute{i}\""
        else:
            sql_select_outer = f"{sql_select_outer}, {tb_labels[tuple(json.loads(oa_column)['table-path'])]['label']}.{json.loads(oa_column)['column']} AS \"object_{object_type_label}_attribute{i}\""

    # from statement
    sql_from = f"\nFROM ({sql_inner}) inner\n"

    sql_outer_join = ''
    if object_tables:
        for json_table in object_tables:
            tab = json.loads(json_table)
            sql_outer_join = f"{sql_outer_join}LEFT OUTER{tb_labels[tuple(tab['path'])]['join']}\n"

    # filter rows without objects
    where_object_clauses = []
    for i, col in enumerate(object_columns):
        if tb_labels[tuple(json.loads(col)['table-path'])]['is_inner']:
            col_label = f"inner.\"object_{object_type_label}_{i}\""
        else:
            col_label = tb_labels[tuple(json.loads(col)['table-path'])]['label'] + '.' + json.loads(col)['column']
        where_object_clauses.append(col_label + ' IS NOT NULL')

    sql_object_filter = ''
    for i, dict_filter in enumerate(object_filters):
        if tb_labels[tuple(json.loads(dict_filter['column'])['table-path'])]['is_inner']:
            column = f"inner.\"object_{object_type_label}_filter{i}\""
        else:
            column = f"{tb_labels[tuple(json.loads(dict_filter['column'])['table-path'])]['label']}.{json.loads(dict_filter['column'])['column']}"
        operator = dict_filter['operator']
        is_static = dict_filter['value_type']
        if is_static:
            value = dict_filter['value']
        else:
            if tb_labels[tuple(json.loads(dict_filter['value'])['table-path'])]['is_inner']:
                value = f"inner.\"object_{object_type_label}_value_filter{i}\""
            else:
                value = f"{tb_labels[tuple(json.loads(dict_filter['value'])['table-path'])]['label']}.{json.loads(dict_filter['value'])['column']}"
        sql_object_filter = f"{sql_object_filter} AND {column} {operator} {value}"

    sql_where = f"WHERE ({' OR '.join(where_object_clauses)}){sql_object_filter}"

    sql = sql_select_outer + sql_from + sql_outer_join + sql_where

    # database credentials
    conn = globals.get_connection()

    print('Beginning OCEL extraction.')
    event_ids = []
    timestamps = []
    event_types = []
    dict_event_attributes = {e: [] for e in event_attribute_labels}
    objects = []
    object_type = object_type_label
    dict_object_attributes = {o: [] for o in object_attribute_labels}

    print(f"Executing: {sql}")
    c = conn.cursor()
    c.execute(sql)

    # fetch_more = True
    num_fetched_rows = 0
    fetched_rows = c.fetchmany(200)

    while fetched_rows:
        for row in fetched_rows:
            event_id = str(data_source_id) + ' ' + str(row[0])
            timestamp = row[1]
            event_type = ' '.join(map(str, row[2:len_event_columns + 2]))

            event_attribute = []
            ea_offset = 0
            for len_event_att in len_event_attributes:
                event_attribute.append(' '.join(
                    map(str, row[len_event_columns + 2 + ea_offset:len_event_columns + 2 + ea_offset + len_event_att])))
                ea_offset += len_event_att

            object_cols = row[
                          len_event_columns + 2 + sum_len_event_attributes:len_object_columns + len_event_columns + 2 + sum_len_event_attributes]

            object_attribute = []
            oa_offset = 0
            if any(object_cols):
                object_id = f"{object_type} {' '.join(map(str, object_cols))}"
                for len_object_att in len_object_attributes:
                    object_attribute.append(' '.join(
                        map(str, row[
                                 len_object_columns + 2 + oa_offset:len_object_columns + 2 + oa_offset + len_object_att])))
                    oa_offset += len_object_att
            else:
                object_id = None
                for len_event_att in len_event_attributes:
                    object_attribute.append(None)

            if event_labels:
                if event_type in event_labels:
                    event_type = event_labels[event_type]

            event_ids.append(event_id)
            timestamps.append(timestamp)
            event_types.append(event_type)
            for i, ea_label in enumerate(event_attribute_labels):
                dict_event_attributes[ea_label].append(event_attribute[i])

            objects.append(object_id)
            for i, oa_label in enumerate(object_attribute_labels):
                dict_object_attributes[oa_label].append(object_attribute[i])
            # Since we add an element to each list for every row.
            # Entries of the list with the same index are the relations

        num_fetched_rows += len(fetched_rows)
        fetched_rows = c.fetchmany(200)

    object_types = [object_type] * len(objects)

    globals.close_connection(conn)
    print('SQL parsed!')

    if timestamps and not isinstance(timestamps[0], datetime.date):
        try:
            timestamps = [datetime.datetime.strptime(t, globals.timestamp_format) for t in timestamps]
        except Exception:
            raise Exception(f"Timestamp column {json.loads(timestamp_column)['table']}."
                            f"{json.loads(timestamp_column)['column']} is not of type datetime!")

    return event_ids, timestamps, event_types, dict_event_attributes, objects, object_types, dict_object_attributes


def get_all_relations_objects(all_iteration_data, max_fetched_rows, start_date, end_date, df_o2o_raw,
                              all_o2o_enforce_data):
    event_id_list = []
    timestamp_list = []
    event_types_list = []
    all_event_attributes = {}
    ea_labels_list = []
    objects_list = []
    object_types_list = []
    all_object_attributes = {}
    oa_labels_list = []

    for data_source_id, iteration in enumerate(all_iteration_data):
        if not iteration['removed']:
            (timestamp_table, timestamp_column, timestamp_data,
             event_tables, event_columns, use_event_columns, replacement_event_label, event_labels, event_filter,
             event_attributes, object_type_label, objects_list_tables, objects_list_columns, use_object_columns,
             replacement_object_label, object_filters, object_attributes) = globals.iteration_data_to_inputs(iteration)

            for e_attr_label in event_attributes:
                if e_attr_label:
                    all_event_attributes[e_attr_label] = []
                    ea_labels_list.append(e_attr_label)

            for o in object_attributes:
                if o:
                    for o_attr_label in object_attributes:
                        if o_attr_label:
                            all_object_attributes[o_attr_label] = []
                            oa_labels_list.append(o_attr_label)

            num_objects = len(objects_list_tables)
            for object_type_id in range(num_objects):
                if not object_type_label[object_type_id]:
                    object_type_label[object_type_id] = str(object_type_id)

                if not max_fetched_rows:  # complete extraction build_sql_extract_complete
                    (event_ids, timestamps, event_types, dict_event_attributes,
                     objects, object_types, dict_object_attributes) = build_sql_extract_chunk(
                        data_source_id, timestamp_table, timestamp_column, timestamp_data, event_tables, event_columns,
                        use_event_columns, replacement_event_label, event_labels, event_filter, event_attributes,
                        object_type_label[object_type_id], objects_list_tables[object_type_id],
                        objects_list_columns[object_type_id], use_object_columns[object_type_id],
                        replacement_object_label[object_type_id], object_filters[object_type_id],
                        object_attributes[object_type_id],
                        0, start_date, end_date)
                else:  # chunk for OCPN update
                    (event_ids, timestamps, event_types, dict_event_attributes,
                     objects, object_types, dict_object_attributes) = build_sql_extract_chunk(
                        data_source_id, timestamp_table, timestamp_column, timestamp_data, event_tables, event_columns,
                        use_event_columns, replacement_event_label, event_labels, event_filter, event_attributes,
                        object_type_label[object_type_id], objects_list_tables[object_type_id],
                        objects_list_columns[object_type_id], use_object_columns[object_type_id],
                        replacement_object_label[object_type_id], object_filters[object_type_id],
                        object_attributes[object_type_id],
                        max_fetched_rows, start_date, end_date)

                event_id_list.extend(event_ids)
                timestamp_list.extend(timestamps)
                event_types_list.extend(event_types)

                for ea_label in all_event_attributes:
                    if ea_label in dict_event_attributes:
                        all_event_attributes[ea_label].extend(dict_event_attributes[ea_label])
                    else:
                        all_event_attributes[ea_label].extend([None for i in object_types])  # fill empty

                objects_list.extend(objects)
                object_types_list.extend(object_types)

                for oa_label in all_object_attributes:
                    if oa_label in dict_object_attributes:
                        all_object_attributes[oa_label].extend(dict_object_attributes[oa_label])
                    else:
                        all_object_attributes[oa_label].extend([None for i in object_types])  # fill empty

    print('Creating OCEL dataframes.')
    # since list elements have fixed positions the relations extracted from the sql statement are extracted
    df_relations_raw = pd.DataFrame(all_event_attributes | all_object_attributes | {
        'ocel:eid': event_id_list, 'ocel:activity': event_types_list, 'ocel:timestamp': timestamp_list,
        'ocel:oid': objects_list, 'ocel:type': object_types_list,
        'ocel:qualifier': [f"At {t} {e} occurred with object {o}." for t, e, o in
                           zip(timestamp_list, event_types_list, objects_list)]
    }).drop_duplicates(inplace=False, ignore_index=True)

    # prefilter on o2o's that have at least one object in the relations
    if not df_o2o_raw.empty and all_o2o_enforce_data:
        df_o2o_has_object = df_o2o_raw[df_o2o_raw[['ocel:oid', 'ocel:oid_2']].isin(df_relations_raw['ocel:oid'].drop_duplicates().to_list()).any(axis=1)]

        for enforcement in all_o2o_enforce_data:
            if not enforcement['removed']:
                dom_obj = enforcement['dominant_object']
                enf_obj = [x for x in enforcement['enforced_o2o'] if x != dom_obj].pop()
                # get dominant objects in the relations
                set_dom_obj = set(df_relations_raw[df_relations_raw['ocel:type'] == dom_obj]['ocel:oid'].unique())
                # get enforced allowed objects
                set_enf_obj = set(df_o2o_has_object[(df_o2o_has_object['ocel:type'] == enf_obj)
                                                    & (df_o2o_has_object['ocel:type_2'] == dom_obj)
                                                    & (df_o2o_has_object['ocel:oid_2'].isin(set_dom_obj))
                                  ]['ocel:oid'].unique())
                set_enf_obj = set_enf_obj.union(set(df_o2o_has_object[(df_o2o_has_object['ocel:type_2'] == enf_obj)
                                                                       & (df_o2o_has_object['ocel:type'] == dom_obj)
                                                                       & df_o2o_has_object['ocel:oid'].isin(set_dom_obj)
                                                    ]['ocel:oid_2'].unique()))

                eid_remove = set(df_relations_raw[(df_relations_raw['ocel:type'] == enf_obj) & (
                    ~df_relations_raw['ocel:oid'].isin(set_enf_obj))]['ocel:eid'].unique())

                df_relations_raw = df_relations_raw[~df_relations_raw['ocel:eid'].isin(eid_remove)]

    df_relations_cleaned = df_relations_raw

    # drop e2o relations without objects
    df_relations = df_relations_cleaned[['ocel:eid', 'ocel:activity', 'ocel:timestamp', 'ocel:oid', 'ocel:type',
                                         'ocel:qualifier']].dropna(subset=['ocel:oid']).reset_index(drop=True)

    # keep 'empty' event types
    df_events = (df_relations_cleaned[['ocel:eid', 'ocel:timestamp', 'ocel:activity'] + ea_labels_list]
                 .drop_duplicates(inplace=False, ignore_index=True)
                 .reset_index(drop=True))
    # .groupby(['ocel:eid', 'ocel:timestamp', 'ocel:activity']) aggregate attribute values?
    # .agg(lambda x: ', '.join(filter(None, x)))

    df_objects = (df_relations_cleaned[['ocel:oid', 'ocel:type'] + oa_labels_list]
                  .drop_duplicates(inplace=False, ignore_index=True)
                  .reset_index(drop=True))

    if not df_o2o_raw.empty:
        # filter o2o on existing objects
        df_o2o_raw = df_o2o_raw[
            df_o2o_raw[['ocel:oid', 'ocel:oid_2']].isin(df_objects['ocel:oid'].to_list()).any(axis=1)]

        oa_attribute_dummy_data = {oa_label: [None for o in range(df_o2o_raw.shape[0])] for oa_label in oa_labels_list}
        df_o2o_raw.assign(**oa_attribute_dummy_data)

        df_o2o_raw_2 = df_o2o_raw[['ocel:oid_2', 'ocel:type_2'] + oa_labels_list].rename(
            columns={'ocel:oid_2': 'ocel:oid',
                     'ocel:type_2': 'ocel:type'})
        # create missing objects later occurrences are dropped thus if an attribute value exists it is kept
        df_objects = pd.concat([df_objects, df_o2o_raw[['ocel:oid', 'ocel:type'] + oa_labels_list],
                                df_o2o_raw_2]).drop_duplicates(
            keep='first').reset_index(drop=True)

        df_o2o = df_o2o_raw[['ocel:oid', 'ocel:oid_2', 'ocel:qualifier']].reset_index(drop=True)
    else:
        df_o2o = None

    return df_relations, df_events, df_objects, df_o2o


def extract_o2o(o2o_data):
    object_type_1 = o2o_data['object_type_1']
    object_type_2 = o2o_data['object_type_2']
    o2o_tables = o2o_data['o2o_tables']
    o2o_columns_1 = o2o_data['o2o_columns_1']
    o2o_columns_2 = o2o_data['o2o_columns_2']
    qualifier = o2o_data['qualifier']
    o1_filter = o2o_data['o1_filter']
    o2_filter = o2o_data['o2_filter']

    list_tables = [json.loads(ot) for ot in o2o_tables]
    tb_labels, inner_columns = get_table_label_dict(list_tables, [])
    column_strings = []
    for i, json_column in enumerate(o2o_columns_1):
        col = json.loads(json_column)
        column_strings.append(
            f"{tb_labels[tuple(col['table-path'])]['label']}.{col['column']} AS \"object0_{i}\"")
    len_columns_o1 = len(column_strings)
    for i, json_column in enumerate(o2o_columns_2):
        col = json.loads(json_column)
        column_strings.append(
            f"{tb_labels[tuple(col['table-path'])]['label']}.{col['column']} AS \"object1_{i}\"")
    len_columns_o2 = len(column_strings) - len_columns_o1

    root_table_string = ''
    joins = []
    for json_table in o2o_tables:
        tab = json.loads(json_table)
        if 'keypair' not in tab.keys():
            root_table_string = tab['table'] + ' ' + tb_labels[tuple(tab['path'])]['label']
        else:
            joins.append('INNER' + tb_labels[tuple(tab['path'])]['join'])

    if not root_table_string:
        raise Exception('No root table detected!')

    wheres = []
    list_filter = []
    if o1_filter:
        list_filter.extend(o1_filter)
    if o2_filter:
        list_filter.extend(o2_filter)

    for dict_filter in list_filter:
        column = f"{tb_labels[tuple(json.loads(dict_filter['column'])['table-path'])]['label']}.{json.loads(dict_filter['column'])['column']}"
        operator = dict_filter['operator']
        is_static = dict_filter['value_type']
        if is_static:
            value = dict_filter['value']
        else:
            value = f"{tb_labels[tuple(json.loads(dict_filter['value'])['table-path'])]['label']}.{json.loads(dict_filter['value'])['column']}"
        wheres.append(f"{column} {operator} {value}")

    new_line = '\n'
    sql = (f"SELECT {', '.join(column_strings)}\n"
           f"FROM {root_table_string}\n"
           f"{new_line.join(joins)}"
           f"{new_line + 'WHERE ' if wheres else ''}{' AND '.join(wheres)}")

    print(f"o2o sql statement:\n{sql}")

    conn = globals.get_connection()

    print('Beginning OCEL extraction.')
    o1_list = []
    o2_list = []
    o2o_list = []
    c = conn.cursor()
    c.execute(sql)

    fetched_rows = c.fetchmany(200)
    while fetched_rows:
        for row in fetched_rows:
            o1 = f"{object_type_1} {' '.join(map(str, row[0:len_columns_o1]))}"
            o2 = f"{object_type_2} {' '.join(map(str, row[len_columns_o1:len_columns_o1 + len_columns_o2]))}"

            # set qualifier here
            o2o = qualifier.replace(r'{1}', object_type_1).replace(r'{2}', object_type_2)

            o1_list.append(o1)
            o2_list.append(o2)
            o2o_list.append(o2o)
            # Since we add an element to each list for every row.
            # Entries of the list with the same index are the relations
        fetched_rows = c.fetchmany(200)

    globals.close_connection(conn)
    print('SQL parsed!')

    return o1_list, o2_list, o2o_list


def get_all_o2o(all_o2o_data):
    object1_list = []
    object2_list = []
    qualifier_list = []
    object1_type_list = []
    object2_type_list = []
    for o2o_data in all_o2o_data:
        if not o2o_data['removed']:
            object_type_1 = o2o_data['object_type_1']
            object_type_2 = o2o_data['object_type_2']
            o1_list, o2_list, o2o_list = extract_o2o(o2o_data)
            # INNER JOIN is used thus no None Objects are extracted
            object1_list.extend(o1_list)
            object2_list.extend(o2_list)
            qualifier_list.extend(o2o_list)
            object1_type_list.extend([object_type_1 for o1 in o1_list])
            object2_type_list.extend([object_type_2 for o2 in o2_list])

    df_o2o_raw = pd.DataFrame(
        {'ocel:oid': object1_list, 'ocel:oid_2': object2_list,
         'ocel:qualifier': qualifier_list,
         'ocel:type': object1_type_list, 'ocel:type_2': object2_type_list})

    df_o2o = df_o2o_raw.drop_duplicates().reset_index(drop=True)

    return df_o2o


@callback(Output('div-extracted-ocpn', 'children'),
          Input('update-ocpn', 'n_clicks'),
          State('update-max-rows', 'value'),
          State({'type': 'store', 'iteration': ALL}, 'data'),
          State('use-timeframe-filter', 'value'),
          State('timeframe-filter', 'start_date'),
          State('timeframe-filter', 'end_date'),
          prevent_initial_call=True)
def update_ocpn(n_clicks, max_fetched_rows, all_iteration_data, use_timeframe_filter, start_date, end_date):
    if use_timeframe_filter:
        if start_date:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')  # format used in Dash
        else:
            start_date = None
        if end_date:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            end_of_day = datetime.timedelta(hours=23, minutes=59, seconds=59)
            end_date = end_date + end_of_day
        else:
            end_date = None
    else:
        start_date = None
        end_date = None

    df_relations, df_events, df_objects, x = get_all_relations_objects(all_iteration_data, max_fetched_rows, start_date,
                                                                       end_date, pd.DataFrame(), {})

    ocel = pm4py_ocel_obj.OCEL(events=df_events, objects=df_objects, relations=df_relations)
    print(ocel)

    mined_ocpn = pm4py.ocel.discover_oc_petri_net(ocel)
    print(f"OCPN discovered!")

    globals.extracted_ocpn = mined_ocpn

    return dcc.Graph(id={'type': 'extracted-ocpn', 'dummy': 0},
                     figure=ocpn_visualization.generate_ocpn_image(mined_ocpn))


@callback(Output({'type': 'modal', 'intend': 'export'}, 'is_open'),
          Input('export', 'n_clicks'),
          State({'type': 'store', 'iteration': ALL}, 'data'),
          State({'type': 'o2o-store', 'o2o': ALL}, 'data'),
          State({'type': 'o2o-enforce-store', 'o2o': ALL}, 'data'),
          State('use-timeframe-filter', 'value'),
          State('timeframe-filter', 'start_date'),
          State('timeframe-filter', 'end_date'),
          prevent_initial_call=True)
def export_ocel(n_clicks, all_iteration_data, all_o2o_data, all_o2o_enforce_data, use_timeframe_filter, start_date,
                end_date):
    if use_timeframe_filter:
        if start_date:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')  # format used in Dash
        else:
            start_date = None
        if end_date:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            end_of_day = datetime.timedelta(hours=23, minutes=59, seconds=59)
            end_date = end_date + end_of_day
        else:
            end_date = None
    else:
        start_date = None
        end_date = None

    df_o2o_raw = get_all_o2o(all_o2o_data)

    df_relations, df_events, df_objects, df_o2o = get_all_relations_objects(all_iteration_data, 0,
                                                                            start_date, end_date, df_o2o_raw,
                                                                            all_o2o_enforce_data)

    ocel = pm4py_ocel_obj.OCEL(events=df_events, objects=df_objects, relations=df_relations, o2o=df_o2o)
    print(ocel)

    fp = r'./ocel.xml'
    pm4py.write.write_ocel2_xml(ocel, fp)
    # fp = r'./ocel.csv'
    # pm4py.write.write_ocel2_json(ocel, fp)
    # fp = r'./ocel.sqlite'
    # pm4py.write.write_ocel2_sqlite(ocel, fp)

    return True
