object Version = Serialization.Factory.generate_struct(SyncDB.Version(), "_version");

object Flag, Type, Schema, Filter;

// this ignore boundaries
class Range {
   inherit Serialization.Types.Tuple;

   void create(object type) {
       ::create("_range", SyncDB.Interval, type, type);
   }
}

class pSchema {
    inherit Serialization.Types.OneTypedList;

    void create() {
	::create(Type);
	type = "_schema";
    }

    SyncDB.Schema decode(Serialization.Atom a) {
	return SyncDB.Schema(@::decode(a));
    }

    int(0..1) can_encode(mixed o) {
	return objectp(o) && object_program(o) == SyncDB.Schema;
    }

    Serialization.Atom encode(mixed o) {
	return ::encode(o->fields);
    }
}

void create(mapping|void overwrites) {
    object black_magic;
    Flag = Serialization.Factory.generate_structs(([
	"_automatic" : SyncDB.Flags.Automatic(),
	"_foreign" : SyncDB.Flags.Foreign(),
	//"_hash" : SyncDB.Flags.Hash(),
	"_hidden" : SyncDB.Flags.Hidden(),
	"_trivial" : SyncDB.Flags.Trivial(),
	"_index" : SyncDB.Flags.Index(),
	"_join" : SyncDB.Flags.Join(),
	"_key" : SyncDB.Flags.Key(),
	"_link" : SyncDB.Flags.Link(),
	"_mandatory" : SyncDB.Flags.Mandatory(),
	"_readonly" : SyncDB.Flags.ReadOnly(),
	"_reference" : SyncDB.Flags.Reference(),
	"_unique" : SyncDB.Flags.Unique(),
	"_writeonly" : SyncDB.Flags.WriteOnly()
    ]), 0, ([
	"string" : Serialization.Types.Symbol(),
    ]));

    black_magic = Serialization.Types.OneTypedList(Flag);
    Filter = Serialization.Factory.generate_structs(([
	"_and" : SyncDB.Mysql.Filter.And(({})),
	"_or" : SyncDB.Mysql.Filter.Or(({})),
	"_equal" : SyncDB.Mysql.Filter.Equal(({})),
	"_true" : SyncDB.Mysql.Filter.True(({})),
	"_false" : SyncDB.Mysql.Filter.False(({})),
    ]), lambda(object o, string s) {
	if (s == "filters") return black_magic;
	return 0;
    }, ([
	"string" : Serialization.Types.Symbol(),
    ]));
    black_magic->etype = Filter;

    black_magic = Serialization.Types.OneTypedList(Flag);
    Type = Serialization.Factory.generate_structs(([
	"_integer" : SyncDB.Types.Integer(""),
	"_string" : SyncDB.Types.String(""),
	"_date" : SyncDB.Types.Date("", Calendar.Second),
	"_vector" : SyncDB.Types.Vector("", ({})),
	"_range" : SyncDB.Types.Range("", Flag, Flag),
    ]), lambda(object o, string s) {
	if (s == "flags") return Serialization.Types.OneTypedList(Flag);
	if (s == "fields") return black_magic;
	return 0;
    });
    black_magic->etype = Type;

    Schema = pSchema();
}
