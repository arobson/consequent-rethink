var _ = require( "lodash" );
var when = require( "when" );
var sequence = require( "when/sequence" );
var rethink = require( "rethinkdb" );

function createTable( connection, db, table, tablePromise, type ) {
	function onTableListing( results ) {
		var client = results[ 0 ];
		var tables = results[ 1 ];
		var exists = _.indexOf( tables, table ) >= 0;
		if( !exists ){
			return createTable = rethink
				.db( db )
				.tableCreate( table, { primaryKey: "snapshotId" } )
				.run( client )
				.then( 
					() => createIndex( client, db, table, "id" )
				);
		} else {
			return when( client );
		}
	}

	return when.join( connection, tablePromise )
		.then( onTableListing );
}

function createIndex( client, db, table, index ) {
	return sequence( [
		() => rethink
			.db( db )
			.table( table )
			.indexCreate( index )
			.run( client ),
		() => rethink
			.db( db )
			.table( table )
			.indexWait( index )
			.run( client )
		] )
	.then( () => client );
}

function fetch( pending, db, table, actorId ) {
	return pending
		.then( 
			( c ) => 
				rethink
					.db( db )
					.table( table )
					.getAll( actorId, { index: "id" } )
					.orderBy( rethink.desc( "snapshotId" ) )
					.limit( 1 )
					.run( c )
		)
		.then( 
			( x ) => x.toArray()
		)
		.then( 
			( x ) => x ? x[ 0 ] : null
		);
}

function findAncestor( client, db, table, type, actorId ) {
	return when.reject( new Error( "Not supported" ) );
}

function getVersion( vector ) {
	var clocks = vector.split( ";" );
	return clocks.reduce( function( version, clock ) {
		var parts = clock.split( ":" );
		return version + parseInt( parts[ 1 ] );
	}, 0 );
}

function store( pending, sliver, db, table, actorId, vectorClock, actor ) {
	var version = getVersion( vectorClock || "" );
	actor.snapshotId = sliver.getId();
	actor.id = actorId;
	actor.version = version;
	actor.ancestor = actor.ancestor || "";
	actor.vector = vectorClock.toString();
	actor = _.omitBy( actor, ( v ) => !v );
	return pending
		.then( 
			( c ) => rethink
				.db( db )
				.table( table )
				.insert( actor )
				.run( c )
		)
}


module.exports = function( sliver, connection, db, tables, type ) {
	var table = `${type}_snapshot`;
	var pending = createTable( connection, db, table, tables, type );
	return {
		fetch: fetch.bind( null, pending, db, table ),
		findAncestor: findAncestor,
		store: store.bind( null, pending, sliver, db, table )
	};
};