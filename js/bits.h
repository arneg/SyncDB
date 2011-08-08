#if 1
#define BITVECTOR(n) (new Array(Math.ceil((n)>>>5)))
#define BV_IS_SET(v, n) !!((v)[(n)>>>5] & (1 << ((n)&31)))
#define BV_SET(v, n) do { ((v)[(n)>>>5]) |= 1 << ((n)&31); } while(0)
#define BV_UNSET(v, n) do { ((v)[(n)>>>5]) &= ~(1 << ((n)&31)); } while(0)
#define BV_SET_BIT(v, n, bit) do {\
    if (bit) { BV_SET(v, n); }	  \
    else { BV_UNSET(v, n); }	  \
} while (0)
#define BV_GET_INT(v, n, len, x) do {\
    var t = (v)[(n)>>>5];	  \
    t >>>= (n) &31;		  \
    if ((len) > 32 - ((n)&31)) {	  \
	t |= (v)[((n)>>>5) + 1] << (32 - ((n) & 31));\
    }				  \
    (x) = t & (1 << (len)) - 1;	  \
} while(0)
#else
#define BITVECTOR(n) (new UTIL.BitVector(n))
#define BV_IS_SET(v, n) ((v).get(n))
#define BV_SET(v, n) ((v).set((n), 1))
#define BV_UNSET(v, n) ((v).set((n), 0))
#define BV_SET_BIT(v, n, bit) ((v).set((n), (bit)))
#define BV_GET_INT(v, n, len, x) do { (x) = (v).get_int((n), (len)); } while (0)
#endif

#define ROUND_UP32(t)	do { t |= t >> 1;t |= t >> 2;t |= t >> 4;t |= t >> 8;t |= t >> 16; } while(0)
