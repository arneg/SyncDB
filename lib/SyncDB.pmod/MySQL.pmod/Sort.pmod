class Base(object type) {
    mixed `+(object ... types) {
	return Combine(this, @types);
    }
}

class ASC {
    inherit Base;
    string encode_sql(object table, function quote) {
	return sprintf("%s ASC", type->sql_name(table->table_name()));
    }
}

class DESC {
    inherit Base;
    string encode_sql(object table, function quote) {
	return sprintf("%s DESC", type->sql_name(table->table_name()));
    }
}

class Combine(object ... types) {
    string encode_sql(object table, function quote) {
	return types->encode_sql(table, quote) * ", ";
    }
}
