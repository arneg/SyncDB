inherit .Base;

constant is_charset = 1;

string charset_name;

void create(string charset_name) {
    this_program::charset_name = charset_name;
}

array(SyncDB.MySQL.Query) flag_definitions(object type) {
    return ({ SyncDB.MySQL.Query("CHARSET "+charset_name) });
}
