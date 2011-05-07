// The standard columns that every table needs (the default one that it is binded to)
//
// _syncdb  table which stores:
//  table		name
//  revision		LONG LONG
//
// _syncdb_version		LONG LONG
//   always refers to the table "revision" when this was last changed
//
//
// tables in locale storage:
//  LocalStorage["_syncdb"] = {
//  	table : {
//  		revision : r,
//  		hash : h,
//  	}
//  	[..]
//  };
//
// create a transaction for set, to handle all joins
// 
// do a join on get
#if constant(Weirdos.Really)
SyncDB.MySQL(schema, ([
    "name" : Join("contacts.fullname", "id"),
    // this would add a WHERE (%d == company_id) clause
    //
    // what about subquery company_id in ("SELECT company_id FROM user_companies WHERE uid = %d ", uid)
    "company_id" : Filter(Select("user_companies.company_id", Equal("uid", UserVar("uid"))))
]));

void create(mapping schema, mapping map2) {
    foreach (schema; string name; object type) {
	if (map2[name]) {
	      
	}
    }
}
#endif
