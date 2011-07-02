/* Since js does all integer arithmetic with 64 bit floats, we have to
 * make sure our calculations never overflow 2^52 otherwise we will end
 * up having inconsistent results. I assume here that the input is 32 bit
 * integer and we only truncate at the end. TODO: we should check if these
 * assumptions holds true.
 */
/** @namespace */
UTIL.Int = {
    /**
     * 32 bit integer mixing function by thomas wang
     * http://www.concentric.net/~ttwang/tech/inthash.htm
     */
    hash32shift : function(i) {
	i = ~i + (i << 15); // i = (i << 15) - i - 1;
	i = i ^ (i >>> 12);
	i = i + (i << 2);
	i = i ^ (i >>> 4);
	i = i * 2057; // i = (i + (i << 3)) + (i << 11);
	i = i ^ (i >>> 16);
	return i & 0xffffffff;
    },
    /**
     * 32 bit integer mixing function used in the java hashmap
     * implementation.
     */
    hashmap : function(i) {
	i ^= (i >>> 20) ^ (i >>> 12);
	i ^= (i >>> 7) ^ (i >>> 4);
	return i & 0xffffffff;
    },
    /** 32 bit integer mixing function by bob jenkins.  */
    jenkins : function(i) {
	i = (i+0x7ed55d16) + (i<<12);
	i = (i^0xc761c23c) ^ (i>>>19);
	i = (i+0x165667b1) + (i<<5);
	i = (i+0xd3a2646c) ^ (i<<9);
	i = (i+0xfd7046c5) + (i<<3);
	i = (i^0xb55a4f09) ^ (i>>>16);
	return i & 0xffffffff;
    }
};
UTIL.Int.Hash = UTIL.Hash.extend(
    /**
     * @lends UTIL.Int.Hash 
     */
    {
    /** 
     * Non cryptographic integer hashing. This hash is intented to be used with bloom filters for integers. 
     * @constructs UTIL.Int.Hash 
     * @augments UTIL.Hash
     */
    constructor : function() {
	this.init();
    },
    init : function() {
	this.state = [ 0, 0, 0 ];
    },
    block_bytes : 12,
    update : function(i) {
	this.state = [ UTIL.Int.hashmap(i), UTIL.Int.hash32shift(i), UTIL.Int.jenkins(i) ];
    },
    digest : function() {
	return this.state.concat();
    }
});
