object Version = Serialization.Factory.generate_struct(SyncDB.Version(), "_version");

object Flag, Type, Schema;

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
    Flag = Serialization.Factory.generate_structs(([
	"_automatic" : SyncDB.Flags.Automatic(),
	"_foreign" : SyncDB.Flags.Foreign(),
	//"_hash" : SyncDB.Flags.Hash(),
	"_hidden" : SyncDB.Flags.Hidden(),
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

    object black_magic = Serialization.Types.OneTypedList(Flag);
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
