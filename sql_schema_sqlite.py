sql_tables = """SELECT tbl_name 
FROM main.sqlite_schema s 
WHERE s.type is 'table';"""


def get_column_query(table):
    return f"SELECT name, type, '' FROM pragma_table_info('{table}');"


# Example for key relationships added by hand from known schema:
# tbl.name col.name pk.id pk.name
sql_pk = f"""VALUES
('event', 'ocel_id', 1, ''),
('event_map_type', 'ocel_type', 2, ''),
('event_object', 'ocel_event_id', 3, ''),
('event_object', 'ocel_object_id', 3, ''),
('event_object', 'ocel_qualifier', 3, ''),
('event_confirmorder', 'ocel_id', 4, ''),
('event_createpackage', 'ocel_id', 5, ''),
('event_faileddelivery', 'ocel_id', 6, ''),
('event_itemoutofstock', 'ocel_id', 7, ''),
('event_packagedelivered', 'ocel_id', 8, ''),
('event_payorder', 'ocel_id', 9, ''),
('event_paymentreminder', 'ocel_id', 10, ''),
('event_pickitem', 'ocel_id', 11, ''),
('event_placeorder', 'ocel_id', 12, ''),
('event_reorderitem', 'ocel_id', 13, ''),
('event_sendpackage', 'ocel_id', 14, ''),
('object', 'ocel_id', 15, ''),
('object_map_type', 'ocel_type', 16, ''),
('object_object', 'ocel_source_id', 17, ''),
('object_object', 'ocel_target_id', 17, ''),
('object_object', 'ocel_qualifier', 17, ''),
('object_customers', 'ocel_id', 18, ''),
('object_items', 'ocel_id', 19, ''),
('object_orders', 'ocel_id', 20, ''),
('object_packages', 'ocel_id', 21, ''),
('object_products', 'ocel_id', 22, '');"""

# tbl.name col.name fk.id fk.name pk.id
sql_fk = f"""VALUES
('event_object', 'ocel_event_id', 1, '', 1),
('event_object', 'ocel_object_id', 2, '', 15),
('event', 'ocel_type', 3, '', 2),
('event_confirmorder', 'ocel_id', 4, '', 1),
('event_createpackage', 'ocel_id', 5, '', 1),
('event_faileddelivery', 'ocel_id', 6, '', 1),
('event_itemoutofstock', 'ocel_id', 7, '', 1),
('event_packagedelivered', 'ocel_id', 8, '', 1),
('event_payorder', 'ocel_id', 9, '', 1),
('event_paymentreminder', 'ocel_id', 10, '', 1),
('event_pickitem', 'ocel_id', 11, '', 1),
('event_placeorder', 'ocel_id', 12, '', 1),
('event_reorderitem', 'ocel_id', 13, '', 1),
('event_sendpackage', 'ocel_id', 14, '', 1),
('object', 'ocel_type', 15, '', 16),
('object_object', 'ocel_source_id', 16, '', 15),
('object_object', 'ocel_target_id', 17, '', 15),
('object_customers', 'ocel_id', 18, '', 15),
('object_items', 'ocel_id', 19, '', 15),
('object_orders', 'ocel_id', 20, '', 15),
('object_packages', 'ocel_id', 21, '', 15),
('object_products', 'ocel_id', 22, '', 15);"""
