var HMAC = Base.extend({
    constructor : function(hashp) {
	this.hashp = hashp;
    },
    get : function(key) {
	return new this.HMAC(new this.hashp(), key);
    },
    HMAC : Base.extend({
	constructor : function(hash, key) {
	    var blen;
	    this.hash = hash;
	    blen = hash.block_bytes;
	    hash.init();
	    if (key.length > blen) {
		hash.update(key);
		key = hash.string_digest(key);
		hash.init();
	    }
	    var a;

	    if ((key.length & (blen-1)) != 0) {
		a = new Array(blen - (key.length & (blen-1)));
		key += String.fromCharCode.apply(window, a);
		//UTIL.log("sizeof key: %o, blen: %o, key: %o", key.length, blen, key);
	    }
	    this.o_key_pad = this.XOR(key, 0x5c);
	    this.i_key_pad = this.XOR(key, 0x36);
	    //UTIL.log(">> %o, %o", this.o_key_pad.length, this.i_key_pad.length);
	},
	// move this somewhere else
	XOR : function(s, i) {
	    var a = new Array(s.length);
	    for (var j = 0; j < a.length; j++)
		a[j] = s.charCodeAt(j)^i;
	    return String.fromCharCode.apply(window, a);
	},
	      /*
	xor : function(one, two) {
	    var three;

	    if (one.length < two.length) {
		var three = one;
		two = one;
		one = three;
	    }

	    three = new Array(one.length - two.length);

	    UTIL.log("o, t: %o, %o", one.length, two.length);
	    for (var i = 0; i < (one.length - two.length); i++) {
		UTIL.log("DID IT %o %o", i, one.length - two.length);
		two += String.fromCharCode(two.charCodeAt(i));
		three[i] = two.charCodeAt(i);
	    }
	    two += String.fromCharCode.apply(window, three);

	    for (var i = 0; i < one.length; i++) {
		three[i] = one.charCodeAt(i)^two.charCodeAt(i%(two.length));
	    }

	    return String.fromCharCode.apply(window, three);

	    return three;
	},
	*/
	hmac : function(s) {
	    var inner;
	    this.hash.update(this.i_key_pad + s)
	    inner = this.hash.string_digest();
	    this.hash.init();
	    this.hash.update(this.o_key_pad + inner);
	    inner = this.hash.hex_digest();
	    this.hash.init();
	    return inner;
        }
    })
});
