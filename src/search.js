var _ = require( "lodash" );
var when = require( "when" );
var rethink = require( "rethinkdb" );

function find( client, db, table, criteria ) {
	return pending
		.then( ( c ) => 
			rethink
				.db( db )
				.table( table )
				.filter( buildCriteria( criteria ) )
				.run( c )
		)
		.then( ( x ) => x.toArray() );
}

function buildCriteria( criteria ) {
	var ops = _.map( criteria, translate );
	var thunks = _.map( ops, thunkerate );
	return ( row ) => {
		return rethink.and.apply( rethink, thunks.map( ( t ) => t( row ) ) );
	};
}

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

module.exports = function( client, db, type ) {
	var table = `${type}_snapshot`;
	return {
		find: find.bind( null, client, db, table ),
	};
};