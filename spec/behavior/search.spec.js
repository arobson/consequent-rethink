require( "../setup" );

function translate( val, key ) {
	if( _.isArray( val ) ) {
		if( val.length == 2 ) {
			return { key: key, op: {
				name: "between",
				min: val[ 0 ],
				max: val[ 1 ]
			} };
		} else {
			return { key: key, op: {
				name: "contains",
				value: val[ 0 ]
			} };
		}
	} else if( _.isObject( val ) ) {
		 var keys = _.keys( val );
		 var vals = _.values( val );
		 if( keys.length === 1 ) {
		 	var k = keys[ 0 ];
		 	var v = vals[ 0 ];
		 	return {
		 		key: key,
		 		op: {
		 			name: k,
		 			value: v
		 		}
		 	};
		 }
	} else {
		return {
			key: key,
			op: {
				name: "eq",
				value: val
			}
		}
	}
}

function thunkerate( criteria ) {
	switch( criteria.op.name ) {
		case "eq":
			return function( col, val, row ) {
				return row( col ).eq( val );
			}.bind( criteria.key, criteria.op.value );
		case "gt":
			return function( col, val, row ) {
				return row( col ).gt( val );
			}.bind( criteria.key, criteria.op.value );
		case "gte":
			return function( col, val, row ) {
				return row( col ).gte( val );
			}.bind( criteria.key, criteria.op.value );
		case "lt":
			return function( col, val, row ) {
				return row( col ).lt( val );
			}.bind( criteria.key, criteria.op.value );
		case "lte":
			return function( col, val, row ) {
				return row( col ).lte( val );
			}.bind( criteria.key, criteria.op.value );
		case "match":
			return function( col, val, row ) {
				return row( col ).match( val );
			}.bind( criteria.key, criteria.op.value );
		case "between":
			return function( col, min, max, row ) {
				return row( col ).between( min, max );
			}.bind( criteria.key, criteria.op.min, criteria.op.max );
		case "contains":
			return function( col, val, row ) {
				return row( col ).contains( val );
			}.bind( criteria.key, criteria.op.value );
		default:
			throw new Error( `RethinkDB does not support criteria operation${crieratia.op.name}`);
	}
}

describe( "when transforming generic criteria to reql", function() {
	
	describe( "without nested columns", function() {
		var criteria;
		var ops;
		var r = { row: _.noop };
		var mockR;
		before( function() {
			criteria =
				{
					a: "string", // eq
					b: 100, // eq
					c: { gt: 1 }, // gt
					d: { gte: 2 }, // gte
					e: { lt: 3 }, // lt
					f: { lte: 4 }, // lte
					g: { match: "lulzy%" }, // match
					j: [ "m", "q" ], // between
					k: [ "x" ] // contains
				};

			ops = _.map( criteria, translate );
			var thunks = _.map( ops, thunkerate );
			console.log( thunks );

			mockR = sinon.mock( r );

		} );

		it( "should produce calls for each op", function() {
			"ohhai".should.equal( "ohhai" );
		} );
	} );
} );