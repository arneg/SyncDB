inherit Error.Generic;

string sqlstate;
SyncDB.MySQL.Table table;

void create(string sqlstate, SyncDB.MySQL.Table table, string msg, array backtrace) {
    this_program::sqlstate = sqlstate;
    this_program::table = table;
    ::create(sprintf("SQL_STATE('%s'): %s", sqlstate, msg), backtrace); 
}

// possible helpers:
