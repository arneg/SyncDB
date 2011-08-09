object Version = Serialization.Factory.generate_struct(SyncDB.Version(), "_version");

object Flag, Type, Schema, Filter;

// this ignore boundaries
class Range {
   inherit Serialization.Types.Tuple;

   void create(object type) {
       ::create("_range", ADT.Interval, type, type);
   }
}

class OverlapsFilter {
    inherit Serialization.Types.RangeSet;

    int(0..1) can_encode(mixed o) {
	return objectp(o) && Program.inherits(object_program(o), SyncDB.Filter.Overlaps);
    }

    mixed decode(Serialization.Atom atom) {
	return SyncDB.Filter.Overlaps(::decode(atom));
    }

    Serialization.Atom encode(mixed o) {
	return ::encode(o->rangefilter);
    }
}

class BloomFilter {
    inherit MMP.Utils.Bloom.tFilter;

    int(0..1) can_encode(mixed o) {
	return (objectp(o) && Program.inherits(object_program(o), SyncDB.Filter.Bloom));
    }

    mixed decode(Serialization.Atom atom) {
	return SyncDB.Filter.Bloom(::decode(atom));
    }

    Serialization.Atom encode(mixed o) {
	return ::encode(o->bloom);
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
	"_and" : SyncDB.MySQL.Filter.And(),
	"_or" : SyncDB.MySQL.Filter.Or(),
	"_equal" : SyncDB.MySQL.Filter.Equal("foo", Serialization.Atom("_foo", "bar")),
	"_true" : SyncDB.MySQL.Filter.True("foo"),
	"_false" : SyncDB.MySQL.Filter.False("foo"),
	"_overlaps" : SyncDB.MySQL.Filter.Overlaps("foo", Serialization.Atom("_foo", "bar")),
    ]), lambda(object o, string s) {
	if (s == "filters") return black_magic;
	if (s == "value") return Serialization.Types.Atom();
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
	if (s == "a") return -1;
	if (s == "b") return -1;
	return 0;
    });
    black_magic->etype = Type;

    Schema = pSchema();
}
