class ASC(object type) {
    string encode_sql(object table, function quote) {
	return sprintf("%s ASC", type->sql_name(table->table));
    }
}

class DSC(object type) {
    string encode_sql(object table, function quote) {
	return sprintf("%s DSC", type->sql_name(table->table));
    }
}

class Combine(object ... types) {
    string encode_sql(object table, function quote) {
	return types->encode_sql(table, quote) * ", ";
    }
}
