from dash import Output, Input, State, callback, no_update
import pandas as pd
import numpy as np
import cx_Oracle
import json

import globals
import sql_schema_oracle
import sql_schema_sqlite

# set this only once
cx_Oracle.init_oracle_client(lib_dir=sql_schema_oracle.oracle_client)


def import_oracle_schema():
    conn = globals.get_connection()

    db_schema_sql = sql_schema_oracle.db_schema_sql
    db_pks_sql = sql_schema_oracle.db_pks_sql
    db_fks_sql = sql_schema_oracle.db_fks_sql

    print("Running Schema-Queries")

    dict_tables = {}

    # table structure
    c = conn.cursor()
    c.execute(db_schema_sql)

    print("Beginning table import")

    fetched_rows = c.fetchmany(200)
    while fetched_rows:
        for table, column, datatype, col_desc, num_rows, tab_desc in fetched_rows:
            if column:  # filter out tables without columns
                if table not in globals.dict_table_columns:
                    globals.dict_table_columns[table] = []
                    dict_tables[table] = [num_rows, 0, tab_desc]
                globals.dict_table_columns[table].append((column, datatype, col_desc))

                if datatype == 'D':  # KeyError: 'PO.CHV_AUTHORIZATIONS'
                    dict_tables[table][1] += 1  # count number of date columns D
            # else:
            #     print(f"Empty table {table} was not added.")
        fetched_rows = c.fetchmany(200)

    globals.df_tables = pd.DataFrame(
        [(t, dict_tables[t][0], dict_tables[t][1] > 0, False, dict_tables[t][2]) for t in dict_tables],
        columns=['Table', 'NumRows', 'DateCol', 'SuggestedObject', 'Description'])

    print("Tables imported!")
    # primary keys
    c.execute(db_pks_sql)
    pk_id_table = {}
    fetched_rows = c.fetchmany(200)
    while fetched_rows:
        for table, column, pk_id, pk_name in fetched_rows:
            if pk_id:  # not empty
                if pk_id not in pk_id_table.keys():
                    pk_id_table[pk_id] = (table, pk_name, [])
                pk_id_table[pk_id][2].append(column)
        fetched_rows = c.fetchmany(200)
    print("Primary keys imported!")

    # foreign keys
    c.execute(db_fks_sql)
    fk_id_table = {}
    fetched_rows = c.fetchmany(200)
    while fetched_rows:
        for table, column, fk_id, fk_name, fk_target_id in fetched_rows:
            if fk_id:  # not empty
                if fk_id not in fk_id_table.keys():
                    fk_id_table[fk_id] = (table, fk_name, [], fk_target_id)
                fk_id_table[fk_id][2].append(column)
        fetched_rows = c.fetchmany(200)
    print("Foreign keys imported!")

    globals.close_connection(conn)
    # Combine relations
    missed_pks = ''
    i = -1
    globals.relation_matrix = np.zeros(shape=(len(globals.df_tables), len(globals.df_tables)), dtype=np.int32)
    for fk_id in fk_id_table:
        fk_table, fk_name, fk_columns, target_pk = fk_id_table[fk_id]
        if target_pk in pk_id_table:
            pk_table, pk_name, pk_columns = pk_id_table[target_pk]
            fk_index = globals.df_tables.index[globals.df_tables['Table'] == fk_table].values[0]
            pk_index = globals.df_tables.index[globals.df_tables['Table'] == pk_table].values[0]

            if globals.relation_matrix[pk_index][fk_index] > 0:
                globals.dict_multi_relation_keys[str(i)] = [int(globals.relation_matrix[pk_index][fk_index])]
                globals.relation_matrix[pk_index][fk_index] = i
                globals.relation_matrix[fk_index][pk_index] = i
                globals.dict_multi_relation_keys[str(i)].append(fk_id)
                i -= 1
            elif globals.relation_matrix[pk_index][fk_index] < 0:
                globals.dict_multi_relation_keys[str(globals.relation_matrix[pk_index][fk_index])].append(fk_id)
            else:
                globals.relation_matrix[pk_index][fk_index] = fk_id
                globals.relation_matrix[fk_index][pk_index] = fk_id
            globals.dict_keypair[str(fk_id)] = (pk_name, pk_table, pk_columns, fk_name, fk_table, fk_columns)
        else:
            missed_pks += f"{target_pk}, "
    print(f"Unmatched keys: ({missed_pks})")
    print('db_dict successfully build')


def import_sqlite_schema():
    dict_tables = {}
    dict_columns = {}

    conn = globals.get_connection()
    c = conn.cursor()

    c.execute(sql_schema_sqlite.sql_tables)
    fetched_rows = c.fetchmany(200)
    while fetched_rows:
        for row in fetched_rows:
            # (num_rows, num_date_cols, tab_desc)
            dict_tables[row[0]] = [None, 0, '']
        fetched_rows = c.fetchmany(200)
    print("Tables imported!")

    for table in dict_tables:
        dict_columns[table] = []
        sql_columns = sql_schema_sqlite.get_column_query(table)

        c.execute(sql_columns)
        fetched_rows = c.fetchmany(200)
        while fetched_rows:
            for col, datatype, description in fetched_rows:
                dict_columns[table].append((col, datatype, description))
                if datatype == 'TIMESTAMP':
                    dict_tables[table][1] += 1
            fetched_rows = c.fetchmany(200)
    print("Columns imported!")
    globals.dict_table_columns = dict_columns

    globals.df_tables = pd.DataFrame(
        [(t, dict_tables[t][0], dict_tables[t][1] > 0, False, dict_tables[t][2]) for t in dict_tables],
        columns=['Table', 'NumRows', 'DateCol', 'SuggestedObject', 'Description'])

    c.execute(sql_schema_sqlite.sql_pk)
    pk_id_table = {}
    fetched_rows = c.fetchmany(200)
    while fetched_rows:
        for table, column, pk_id, pk_name in fetched_rows:
            if pk_id:  # not empty
                if pk_id not in pk_id_table.keys():
                    pk_id_table[pk_id] = (table, pk_name, [])
                pk_id_table[pk_id][2].append(column)
        fetched_rows = c.fetchmany(200)
    print("Primary keys imported!")

    # foreign keys
    c.execute(sql_schema_sqlite.sql_fk)
    fk_id_table = {}
    fetched_rows = c.fetchmany(200)
    while fetched_rows:
        for table, column, fk_id, fk_name, fk_target_id in fetched_rows:
            if fk_id:  # not empty
                if fk_id not in fk_id_table.keys():
                    fk_id_table[fk_id] = (table, fk_name, [], fk_target_id)
                fk_id_table[fk_id][2].append(column)
        fetched_rows = c.fetchmany(200)
    print("Foreign keys imported!")

    globals.close_connection(conn)
    # Combine relations
    missed_pks = ''
    i = -1
    globals.relation_matrix = np.zeros(shape=(len(globals.df_tables), len(globals.df_tables)), dtype=np.int32)
    for fk_id in fk_id_table:
        fk_table, fk_name, fk_columns, target_pk = fk_id_table[fk_id]
        if target_pk in pk_id_table:
            pk_table, pk_name, pk_columns = pk_id_table[target_pk]
            fk_index = globals.df_tables.index[globals.df_tables['Table'] == fk_table].values[0]
            pk_index = globals.df_tables.index[globals.df_tables['Table'] == pk_table].values[0]

            if globals.relation_matrix[pk_index][fk_index] > 0:
                globals.dict_multi_relation_keys[str(i)] = [int(globals.relation_matrix[pk_index][fk_index])]
                globals.relation_matrix[pk_index][fk_index] = i
                globals.relation_matrix[fk_index][pk_index] = i
                globals.dict_multi_relation_keys[str(i)].append(fk_id)
                i -= 1
            elif globals.relation_matrix[pk_index][fk_index] < 0:
                globals.dict_multi_relation_keys[str(globals.relation_matrix[pk_index][fk_index])].append(fk_id)
            else:
                globals.relation_matrix[pk_index][fk_index] = fk_id
                globals.relation_matrix[fk_index][pk_index] = fk_id
            globals.dict_keypair[str(fk_id)] = (pk_name, pk_table, pk_columns, fk_name, fk_table, fk_columns)
        else:
            missed_pks += f"{target_pk}, "
    print(f"Unmatched keys: ({missed_pks})")
    print('db_dict successfully build')


def update_df_tables(provided_ocpn):
    if not globals.df_tables.empty and provided_ocpn:
        provided_object_types = list(provided_ocpn['object_types'])

        for i, row in globals.df_tables.iterrows():
            table = row['Table']
            # if table in provided_object_types:
            if any(str(ot).lower() in str(table).lower() for ot in provided_object_types):
                globals.df_tables.at[i, 'SuggestedObject'] = True
            else:
                for column in [column_tuples[0] for column_tuples in globals.dict_table_columns[table]]:
                    if any(str(ot).lower() in str(column).lower() for ot in provided_object_types):
                        globals.df_tables.at[i, 'SuggestedObject'] = True


@callback(Output('div-sqlite-login', 'hidden'),
          Output('div-oracle-login', 'hidden'),
          Input('db-type', 'value'),
          )  # prevent_initial_call=True
def login_form(mode):
    if mode == 'OracleEBS':
        return True, False
    elif mode == 'SQLite':
        return False, True
    else:
        return no_update


@callback(Output('div-db-credentials', 'hidden'),
          Output('div-schema-extraction', 'hidden'),
          Input('login', 'n_clicks'),
          State('db-type', 'value'),
          State('db-ip', 'value'),
          State('db-port', 'value'),
          State('service-name', 'value'),
          State('db-user', 'value'),
          State('db-pw', 'value'),
          State('path-sqlite', 'value'),
          prevent_initial_call=True)
def login_to_database(n_clicks, db_type, ip, port, service, user, pw, path):
    globals.dict_credentials['db-type'] = db_type
    if db_type == 'OracleEBS':
        globals.dict_credentials['ip'] = ip
        globals.dict_credentials['port'] = port
        globals.dict_credentials['service'] = service
        globals.dict_credentials['user'] = user
        globals.dict_credentials['pw'] = pw
    elif db_type == 'SQLite':
        globals.dict_credentials['database'] = path

    try:
        conn = globals.get_connection()
        globals.close_connection(conn)

        with open(f"login.json", 'w') as f_login:
            json.dump(globals.dict_credentials, f_login)

    except cx_Oracle.NotSupportedError as e:
        print('Database type not supported!')
        # return no_update

        print('No connection established!')
        return True, False
    except cx_Oracle.Error as e:
        print(e)
        # return no_update

        print('No connection established!')
        return True, False
    else:
        return True, False


@callback(Output('div-add', 'hidden'),
          Output('div-schema-extraction', 'hidden', allow_duplicate=True),
          Output('tabs', 'hidden'),
          Input('schema-from-db', 'n_clicks'),
          State('db-type', 'value'),
          prevent_initial_call=True)
def import_schema(n_click, db_type):
    if not globals.df_tables.empty:
        raise Exception('Database Schema already initialized!')
    else:
        if db_type == 'OracleEBS':
            import_oracle_schema()
        elif db_type == 'SQLite':
            import_sqlite_schema()

        # allways store schema
        db_schema = {'dict_table_columns': globals.dict_table_columns,
                     'df_tables': globals.df_tables.to_dict(),
                     'dict_keypair': globals.dict_keypair,
                     'relation_matrix': globals.relation_matrix.tolist(),
                     'dict_multi_relation_keys': globals.dict_multi_relation_keys}

        # store database credentials for reuse
        # CAUTION password is stored in clear text!
        # with open(f"db_schema_{db_type}.json", 'w') as f_db_schema:
        #     json.dump(db_schema, f_db_schema, indent=1)

        update_df_tables(globals.provided_ocpn)

        return False, True, False


@callback(Output('div-add', 'hidden', allow_duplicate=True),
          Output('div-schema-extraction', 'hidden', allow_duplicate=True),
          Output('tabs', 'hidden', allow_duplicate=True),
          Input('schema-from-file', 'n_clicks'),
          State('db-type', 'value'),
          prevent_initial_call=True)
def load_schema(num_click, db_type):
    if not globals.df_tables.empty:
        raise Exception('Database Schema already initialized!')
    else:
        expected_filename = 'db_schema.json'
        if db_type == 'OracleEBS':
            expected_filename = 'db_schema_OracleEBS.json'
        elif db_type == 'SQLite':
            expected_filename = 'db_schema_SQLite.json'

    with open(expected_filename) as f_db_schema:
        db_schema = json.load(f_db_schema)
    globals.dict_table_columns = db_schema['dict_table_columns']
    globals.df_tables = pd.DataFrame.from_dict(db_schema['df_tables']).reset_index(drop=True)
    globals.dict_keypair = db_schema['dict_keypair']
    globals.relation_matrix = np.array(db_schema['relation_matrix'], dtype=np.int32)
    globals.dict_multi_relation_keys = db_schema['dict_multi_relation_keys']

    update_df_tables(globals.provided_ocpn)

    return False, True, False
