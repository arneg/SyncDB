class ASC(object type) {
    object encode_sql(object table) {
        array(string) names = type->escaped_sql_names(table->table_name());
        return SyncDB.MySQL.Query(names * " ASC," + " ASC");
    }
}

class DESC(object type) {
    object encode_sql(object table) {
        array(string) names = type->escaped_sql_names(table->table_name());
        return SyncDB.MySQL.Query(names * " DESC," + " DESC");
    }
}

class OrderBy(object ... a) {
    object encode_sql(object table) {
        return SyncDB.MySQL.Query(" ORDER BY ", a->encode_sql(table), ", ");
    }
}

class Limit(int offset, int row_count) {
    object encode_sql(object table) {
        return SyncDB.MySQL.Query(sprintf(" LIMIT %d, %d ", offset, row_count));
    }
}
