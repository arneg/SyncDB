inherit .Base;

constant is_array = 1;

SyncDB.Types.Base type;
array(string) fields;

void create(SyncDB.Types.Base type, array(string) fields, SyncDB.Flags.Base ... flags) {
    this_program::type = type;
    this_program::fields = fields;
    ::create(@args);
}

array(string) encode_sql(array r) {
    return map(r, type->encode_sql);
}

array decode_sql(array(string) s) {
    return map(s, type->decode_sql);
}

mapping make_named_rows(array(string) vals) {
    return mkmapping(fields, vals);
}
