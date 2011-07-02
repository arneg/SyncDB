all: util.bloom.js util.sha256.js
%.js: %.js.in bits.h
	cpp -P $< > $@
doc:
	sh ${JSDOCDIR}/jsrun.sh -t="${JSDOCTEMPLATEDIR}" -d=`pwd`/../docs -r 0 -v -a -x=h,js,in SyncDB.js bits.h util.sha256.js.in util.hmac.js util.int.js util.bloom.js.in
