import datetime

from dash import Output, Input, State, callback, MATCH, ALL, no_update, ctx
import json

import globals
from globals import get_table_column_options


@callback(Output({'type': 'columns', 'subject': ALL, 'index': ALL}, 'options'),
          Output({'type': 'tables', 'subject': ALL, 'index': ALL}, 'options',
                 allow_duplicate=True),
          Input({'type': 'tables', 'subject': ALL, 'index': ALL}, 'value'),
          State({'type': 'tables', 'subject': 'timestamp', 'index': ALL}, 'options'),
          State({'type': 'tables', 'subject': 'objects', 'index': 0}, 'value'),
          State('add-direction', 'value'),
          prevent_initial_call=True)
def table_explored(all_selected_tables, old_timestamp_options, old_object_value, direction):
    index_list = []
    for table_selections in ctx.inputs_list[0]:  # count object types
        if table_selections['id']['subject'] == 'objects':
            index_list.append(table_selections['id']['index'])

    timestamp_tables, event_tables = [], []
    object_type_tables = [[] for i in range(max(index_list) + 1)]
    for i, table_selections in enumerate(ctx.inputs_list[0]):
        if all_selected_tables[i]:
            if table_selections['id']['subject'] == 'timestamp':
                timestamp_tables.append(all_selected_tables[i])  # timestamp is not multiselect
            elif table_selections['id']['subject'] == 'event-types':
                event_tables = all_selected_tables[i]
            elif table_selections['id']['subject'] == 'objects':
                object_type_tables[table_selections['id']['index']] = all_selected_tables[i]
            else:
                raise Exception('Unexpected table source.')

    timestamp_table_options, timestamp_column_options = [], []
    for t_table in reversed(timestamp_tables):
        t_t, t_c = get_table_column_options(t_table)
        timestamp_table_options = timestamp_table_options + t_t
        timestamp_column_options = timestamp_column_options + t_c
    events_table_options, events_column_options = [], []
    for e_table in reversed(event_tables):
        e_t, e_c = get_table_column_options(e_table)
        events_table_options = events_table_options + e_t
        events_column_options = events_column_options + e_c
    objects_table_options = [[] for i in range(max(index_list) + 1)]
    objects_column_options = [[] for i in range(max(index_list) + 1)]
    for i, object_tables in enumerate(object_type_tables):
        for o_table in reversed(object_tables):
            o_t, o_c = get_table_column_options(o_table)
            objects_table_options[i] = objects_table_options[i] + o_t
            objects_column_options[i] = objects_column_options[i] + o_c

    column_options_output = []
    if direction == 'Timestamp':
        for column_output_type in ctx.outputs_list[0]:
            if column_output_type['id']['subject'] == 'timestamp':
                column_options = timestamp_column_options
                column_options_output.append(column_options)
            elif column_output_type['id']['subject'] == 'event-types':
                column_options = events_column_options + timestamp_column_options
                column_options_output.append(column_options)
            elif column_output_type['id']['subject'] == 'objects':
                column_options = objects_column_options[column_output_type['id'][
                    'index']] + events_column_options + timestamp_column_options
                column_options_output.append(column_options)
    elif direction == 'Object':
        for column_output_type in ctx.outputs_list[0]:
            if column_output_type['id']['subject'] == 'timestamp':
                column_options = timestamp_column_options + events_column_options + objects_column_options[0]
                column_options_output.append(column_options)
            elif column_output_type['id']['subject'] == 'event-types':
                column_options = events_column_options + objects_column_options[0]
                column_options_output.append(column_options)
            elif column_output_type['id']['index'] == 0 and column_output_type['id']['subject'] == 'objects':
                column_options = objects_column_options[column_output_type['id'][
                    'index']] + events_column_options + timestamp_column_options
                column_options_output.append(column_options)
            elif column_output_type['id']['subject'] == 'objects':
                column_options = objects_column_options[column_output_type['id'][
                    'index']] + events_column_options + timestamp_column_options + objects_column_options[0]
                column_options_output.append(column_options)
    else:
        raise Exception('Unexpected direction.')

    table_options_output = []
    if direction == 'Timestamp':
        for table_output_type in ctx.outputs_list[1]:
            if table_output_type['id']['subject'] == 'timestamp':
                table_options = old_timestamp_options[0]  # timestamp table is single select
                table_options_output.append(table_options)
            elif table_output_type['id']['subject'] == 'event-types':
                table_options = events_table_options + timestamp_table_options
                table_options_output.append(table_options)
            elif table_output_type['id']['subject'] == 'objects':
                table_options = objects_table_options[
                                    table_output_type['id']['index']] + events_table_options + timestamp_table_options
                table_options_output.append(table_options)
    elif direction == 'Object':
        for table_output_type in ctx.outputs_list[1]:
            if table_output_type['id']['index'] == 0 and table_output_type['id']['subject'] == 'objects':
                for json_object_table_value in old_object_value:  # insert old value
                    object_table_value = json.loads(json_object_table_value)
                    if 'keypair' not in object_table_value:
                        or_t, or_nr = globals.df_tables[globals.df_tables['Table'] == object_table_value['table']][
                            ['Table', 'NumRows']].values.tolist()[0]
                        list_object_value_label = [{'label': f"{or_t}{' with ' + ' '.join(object_table_value['info']) if any(object_table_value['info']) else ''}{' - '+or_nr+' Rows' if or_nr else ''}",
                                                    'value': json_object_table_value}]
                        table_options = list_object_value_label + objects_table_options[
                            0] + events_table_options + timestamp_table_options
                        table_options_output.append(table_options)
            elif table_output_type['id']['subject'] == 'timestamp':
                table_options = timestamp_table_options + events_table_options + objects_table_options[0]
                table_options_output.append(table_options)
            elif table_output_type['id']['subject'] == 'event-types':
                table_options = events_table_options + objects_table_options[0]
                table_options_output.append(table_options)
            elif table_output_type['id']['subject'] == 'objects':
                table_options = (objects_table_options[table_output_type['id']['index']] + events_table_options
                                 + timestamp_table_options + objects_table_options[0])
                table_options_output.append(table_options)
    else:
        raise Exception('Unexpected direction.')

    return column_options_output, table_options_output


@callback(Output({'type': 'save-iteration', 'dummy': 0}, 'disabled'),
          Input({'type': 'confirm', 'subject': ALL, 'index': ALL}, 'n_clicks'),
          State({'type': 'confirm', 'subject': ALL, 'index': ALL}, 'disabled'),
          prevent_initial_call=True)
def enable_confirm_iteration(all_n_clicks, all_disabled):
    if all_n_clicks:
        for n_clicks in all_n_clicks:
            for disabled in all_disabled:
                if not n_clicks and not disabled:
                    return True
        return False  # all clicked and disabled
    else:
        return True


@callback(Output({'type': 'confirm', 'subject': 'timestamp', 'index': 0}, 'disabled'),
          Input({'type': 'columns', 'subject': 'timestamp', 'index': 0}, 'value'),
          prevent_initial_call=True)
def enable_timestamp_confirm(columns):
    if columns:
        return False
    else:
        return True


@callback(Output({'type': 'confirm', 'subject': 'event-types', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Input({'type': 'columns', 'subject': 'event-types', 'index': 0}, 'value'),
          Input({'type': 'radio-columns', 'subject': 'event-types', 'index': 0}, 'value'),
          prevent_initial_call=True)
def enable_event_type_confirm(columns, use_columns):
    if columns or not use_columns:
        return False
    else:
        return True


@callback(Output({'type': 'confirm', 'subject': 'objects', 'index': MATCH}, 'disabled',
                 allow_duplicate=True),
          Input({'type': 'columns', 'subject': 'objects', 'index': MATCH}, 'value'),
          Input({'type': 'radio-columns', 'subject': 'objects', 'index': MATCH}, 'value'),
          prevent_initial_call=True)
def enable_object_type_confirm(columns, use_columns):
    if columns or not use_columns:
        return False
    else:
        return True


@callback(Output({'type': 'columns', 'subject': 'timestamp', 'index': 0}, 'disabled'),
          Input({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'value'),
          State('add-direction', 'value'),
          State({'type': 'timestamp-store', 'subject': 'timestamp', 'index': 0}, 'data'),
          prevent_initial_call=True)
def activate_timestamp_columns(timestamp_tables, direction, timestamp_data):
    if (timestamp_tables or direction == 'Object') and not timestamp_data:
        return False
    else:
        return True


@callback(Output({'type': 'tables', 'subject': 'event-types', 'index': 0}, 'disabled'),
          Output({'type': 'columns', 'subject': 'event-types', 'index': 0}, 'disabled'),
          Output({'type': 'radio-columns', 'subject': 'event-types', 'index': 0}, 'options'),
          Output({'type': 'open-filter', 'subject': 'event-types', 'index': 0}, 'disabled'),
          Output({'type': 'open-attributes', 'subject': 'event-types', 'index': 0}, 'disabled'),
          Output({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'columns', 'subject': 'timestamp', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'confirm', 'subject': 'timestamp', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Input({'type': 'confirm', 'subject': 'timestamp', 'index': 0}, 'n_clicks'),
          State({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'value'),
          State('add-direction', 'value'),
          prevent_initial_call=True)
def confirm_timestamp_selection(num_click, timestamp_table, direction):
    if direction == 'Timestamp':
        return False, False, globals.enabled_event_radio_options, False, False, True, True, True
    else:
        return True, True, globals.disabled_event_radio_options, True, True, True, True, True


@callback(Output({'type': 'columns', 'subject': MATCH, 'index': MATCH}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'single-label', 'subject': MATCH, 'index': MATCH}, 'disabled'),
          Input({'type': 'radio-columns', 'subject': MATCH, 'index': MATCH}, 'value'),
          prevent_initial_call=True)
def radiobutton_empty_columns(select_columns):
    return not select_columns, select_columns


@callback(Output({'type': 'object-label', 'subject': 'objects', 'index': 0}, 'disabled'),
          Output({'type': 'tables', 'subject': 'objects', 'index': 0}, 'disabled'),
          Output({'type': 'columns', 'subject': 'objects', 'index': 0}, 'disabled'),
          Output({'type': 'radio-columns', 'subject': 'objects', 'index': 0}, 'options'),
          Output({'type': 'open-filter', 'subject': 'objects', 'index': 0}, 'disabled'),
          Output({'type': 'open-attributes', 'subject': 'objects', 'index': 0}, 'disabled'),
          Output({'type': 'tables', 'subject': 'event-types', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'columns', 'subject': 'event-types', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'radio-columns', 'subject': 'event-types', 'index': 0}, 'options',
                 allow_duplicate=True),
          Output({'type': 'single-label', 'subject': 'event-types', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'confirm', 'subject': 'event-types', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'tables', 'subject': 'timestamp', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'columns', 'subject': 'timestamp', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Input({'type': 'confirm', 'subject': 'event-types', 'index': 0}, 'n_clicks'),
          State({'type': 'columns', 'subject': 'event-types', 'index': 0}, 'value'),
          State({'type': 'radio-columns', 'subject': 'event-types', 'index': 0}, 'value'),
          State('add-direction', 'value'),
          prevent_initial_call=True)
def confirm_event_selection(n_clicks, event_columns, use_event_columns, direction):
    if use_event_columns:
        if not event_columns:
            return no_update
    if direction == 'Timestamp':
        return (False, False, False, globals.enabled_object_radio_options, False, False,
                True, True, globals.disabled_event_radio_options, True, True,
                True, True)  # timestamps
    else:
        return (True, True, True, globals.disabled_object_radio_options, True, True,
                True, True, globals.disabled_event_radio_options, True, True,
                False, False)  # timestamps


@callback(Output({'type': 'tables', 'subject': 'objects', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'columns', 'subject': 'objects', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'radio-columns', 'subject': 'objects', 'index': 0}, 'options',
                 allow_duplicate=True),
          Output({'type': 'single-label', 'subject': 'objects', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'confirm', 'subject': 'objects', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'tables', 'subject': 'event-types', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'columns', 'subject': 'event-types', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'radio-columns', 'subject': 'event-types', 'index': 0}, 'options',
                 allow_duplicate=True),
          Output({'type': 'open-filter', 'subject': 'event-types', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'open-attributes', 'subject': 'event-types', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Input({'type': 'confirm', 'subject': 'objects', 'index': 0}, 'n_clicks'),
          State({'type': 'columns', 'subject': 'objects', 'index': 0}, 'value'),
          State({'type': 'radio-columns', 'subject': 'objects', 'index': 0}, 'value'),
          State('add-direction', 'value'),
          prevent_initial_call=True)
def confirm_object_selection_from_object(n_clicks, object_columns, use_object_columns, direction):
    if use_object_columns:
        if not object_columns:
            return no_update
    if direction == 'Object':
        return (True, True, globals.disabled_object_radio_options, True, True,
                False, False, globals.enabled_event_radio_options, True, True)
    else:
        return (True, True, globals.disabled_object_radio_options, True, True,
                True, True, globals.disabled_event_radio_options, True, True)


@callback(Output({'type': 'tables', 'subject': 'objects', 'index': MATCH}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'columns', 'subject': 'objects', 'index': MATCH}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'radio-columns', 'subject': 'objects', 'index': MATCH}, 'options',
                 allow_duplicate=True),
          Output({'type': 'single-label', 'subject': 'objects', 'index': MATCH}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'confirm', 'subject': 'objects', 'index': MATCH}, 'disabled',
                 allow_duplicate=True),
          Input({'type': 'confirm', 'subject': 'objects', 'index': MATCH}, 'n_clicks'),
          State({'type': 'columns', 'subject': 'objects', 'index': MATCH}, 'value'),
          State({'type': 'radio-columns', 'subject': 'objects', 'index': MATCH}, 'value'),
          State('add-direction', 'value'),
          prevent_initial_call=True)
def confirm_object_selection(n_clicks, object_columns, use_object_columns, direction):
    if ctx.triggered_id['index'] == 0:
        return no_update
    if use_object_columns:
        if not object_columns:
            return no_update
    return True, True, globals.disabled_object_radio_options, True, True


@callback(Output({'type': 'add-objects', 'dummy': 0}, 'disabled'),
          Input({'type': 'confirm', 'subject': 'timestamp', 'index': 0}, 'n_clicks'),
          Input({'type': 'confirm', 'subject': 'event-types', 'index': 0}, 'n_clicks'),
          prevent_initial_call=True)
def enable_add_object_type(n_clicks_timestamp, n_clicks_event_type):
    if n_clicks_timestamp and n_clicks_event_type:
        return False
    else:
        return no_update


@callback(Output({'type': 'columns', 'subject': 'objects', 'index': 0}, 'disabled',
                 allow_duplicate=True),
          Output({'type': 'radio-columns', 'subject': 'objects', 'index': 0}, 'options',
                 allow_duplicate=True),
          Input({'type': 'tables', 'subject': 'objects', 'index': 0}, 'value'),
          State('add-direction', 'value'),
          prevent_initial_call=True)
def enable_object_columns(object_tables, direction):
    if direction == 'Timestamp':
        return no_update
    else:
        enabled_object_options = ({'label': 'All columns contain the same object (not object type)',
                                   'value': False, 'disabled': False},
                                  {'label': 'Select columns:', 'value': True, 'disabled': False})

        disabled_object_options = ({'label': 'All columns contain the same object (not object type)',
                                    'value': False, 'disabled': True},
                                   {'label': 'Select columns:', 'value': True, 'disabled': True})
        if object_tables:
            return False, enabled_object_options
        else:
            return True, disabled_object_options


@callback(Output({'type': 'typing-dropdown', 'label': MATCH, 'object': MATCH}, 'options'),
          Output({'type': 'typing-dropdown-helper', 'label': MATCH, 'object': MATCH}, 'data'),
          Input({'type': 'typing-dropdown', 'label': MATCH, 'object': MATCH}, 'search_value'),
          State({'type': 'typing-dropdown', 'label': MATCH, 'object': MATCH}, 'options'),
          State({'type': 'typing-dropdown-helper', 'label': MATCH, 'object': MATCH}, 'data'),
          prevent_initial_call=True)
def typing_dropbox_handler(search_value, options, previous_search_value):
    n = len(search_value)
    if previous_search_value:
        n_previous = len(previous_search_value)
    else:
        n_previous = 0

    if previous_search_value is None and n > 0:
        options += [search_value]
    elif n > n_previous > 0:
        options = options[:-1] + [search_value]
    elif n < n_previous and n_previous > 0:
        if n > 0:
            options = options[:-1] + [search_value]
        else:
            if n < n_previous - 1:
                search_value = None
            else:
                options = options[:-1]

    return options, search_value


@callback(Output('data-source-tabs-div', 'hidden', allow_duplicate=True),
          Output('o2o-tabs-div', 'hidden', allow_duplicate=True),
          Input('tabs-mode', 'active_tab'),
          prevent_initial_call=True)
def switch_tab(active_tab):
    if active_tab == 'tab-data-source':
        return False, True
    elif active_tab == 'tab-o2o':
        return True, False


@callback(Output({'type': 'modal', 'intend': MATCH}, 'is_open', allow_duplicate=True),
          Input({'type': 'close-modal', 'intend': MATCH}, 'n_clicks'),
          prevent_initial_call=True)
def close_modal(n_clicks):
    if not n_clicks:
        return no_update
    return False


@callback(Output('timeframe-filter', 'disabled'),
          Input('use-timeframe-filter', 'value'),
          prevent_initial_call=True)
def activate_timeframe_filter(checkbox):
    if not checkbox:
        return no_update
    return False


@callback(Output('timeframe-filter', 'initial_visible_month'),
          Input('timeframe-filter', 'start_date'),
          prevent_initial_call=True)
def set_starting_month(start_date):
    if start_date:
        return start_date
    else:
        return datetime.datetime.now()


@callback(Output({'type': 'copy-objects', 'dummy': 0}, 'disabled'),
          Input({'type': 'store', 'iteration': ALL}, 'data'))
def enable_add_existing_object(data):
    if not data:
        return no_update
    return False
