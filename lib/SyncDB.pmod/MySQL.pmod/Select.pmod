class ASC(object type) {
    object encode_sql(object table) {
        array(string) names = type->escaped_sql_names(table->table_name());
        return SyncDB.MySQL.Query(names * " ASC," + " ASC");
    }

    int __hash() {
        return hash_value(type) ^ hash_value(this_program);
    }

    int(0..1) `==(mixed b) {
        return objectp(b) && object_program(b) == this_program && b->type == type;
    }
}

class DESC(object type) {
    object encode_sql(object table) {
        array(string) names = type->escaped_sql_names(table->table_name());
        return SyncDB.MySQL.Query(names * " DESC," + " DESC");
    }

    int __hash() {
        return hash_value(type) ^ hash_value(this_program);
    }

    int(0..1) `==(mixed b) {
        return objectp(b) && object_program(b) == this_program && b->type == type;
    }
}

class OrderBy(object ... a) {
    object encode_sql(object table) {
        return SyncDB.MySQL.Query(" ORDER BY ", a->encode_sql(table), ", ");
    }

    int __hash() {
        int ret = 0;
        foreach (a;; object o) {
            ret ^= hash_value(o);
        }
        return ret;
    }

    int(0..1) `==(mixed b) {
        return objectp(b) && object_program(b) == this_program && equal(b->a, a);
    }
}

class Limit(int offset, int row_count) {
    object encode_sql(object table) {
        return SyncDB.MySQL.Query(sprintf(" LIMIT %d, %d ", offset, row_count));
    }

    int(0..1) `==(mixed b) {
        return objectp(b) && object_program(b) == this_program && offset == b->offset && row_count == b->row_count;
    }
}
