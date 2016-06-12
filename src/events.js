var _ = require( "lodash" );
var when = require( "when" );
var sequence = require( "when/sequence" );
var zlib = require( "zlib" );
var rethink = require( "rethinkdb" );

function createTable( connection, db, table, tablePromise, type ) {
	function onTableListing( results ) {
		var client = results[ 0 ];
		var tables = results[ 1 ];
		var exists = _.indexOf( tables, table ) >= 0;
		if( !exists ){
			return rethink
				.db( db )
				.tableCreate( table )
				.run( client )
				.then( () => createIndex( client, db, table ) );
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
			.indexCreate( "actorId" )
			.run( client ),
		() => rethink
			.db( db )
			.table( table )
			.indexCreate( "instance", [
				rethink.row( "actorId" ),
				rethink.row( "vector" )
			] )
			.run( client ),
		() => rethink
				.db( db )
				.table( table )
				.indexWait()
				.run( client )
		] )
	.then( () => client );
}

function getEventsFor( pending, db, table, actorId, lastEventId ) {
	return pending.then( ( c ) =>
		rethink
			.db( db )
			.table( table )
			.between( lastEventId || rethink.minval, rethink.maxval, { index: "id", leftBound: "open" } )
			.filter( { correlationId: actorId } )
			.orderBy( rethink.desc( "id" ) )
			.run( c ) )
	.then( ( x ) => x.toArray() );
}

function getEventPackFor( pending, db, table, actorId, vectorClock ) {
	return pending.then( ( c ) =>
		rethink
			.db( db )
			.table( table )
			.getAll( [ actorId, vectorClock ], { index: "instance" } )
			.limit( 1 )
			.run( c )
		)
		.then( ( x ) => {
			var list = x.toArray();
			return list.length ? list[ 0 ] : null;
		} );
}

function getVersion( vector ) {
	var clocks = vector.split( ";" );
	return clocks.reduce( function( version, clock ) {
		var parts = clock.split( ":" );
		return version + parseInt( parts[ 1 ] );
	}, 0 );
}

function storeEvents( sliver, pending, db, table, actorId, events ) {
	var storageFormat = _.map( events, ( e ) => {
		e.id = e.id || sliver.getId();
		e.vector = e.vector;
		e.initiatedById = e.initiatedById;
		return _.omitBy( e, ( v ) => !v );
	} );
	return pending.then( 
		( c ) => rethink
			.db( db )
			.table( table )
			.insert( storageFormat )
			.run( c ) 
	);
}

function storeEventPack( sliver, pending, packTable, actorId, vectorClock, events ) {
	var pack = {
		events,
		actorId: actorid,
		id: sliver.getId,
		vector: vectorClock,
		version: getVersion( vectorClock )
	};
	return pending.then( ( c ) =>
		rethink
			.db( db )
			.table( table )
			.insert( pack )
			.run( c ) );
}


module.exports = function( sliver, client, db, tables, type ) {
	var eventTable = `${type}_events`;
	var packTable = `${type}_packs`;

	var pending = when.all( [
		createTable( client, db, eventTable, tables, "event" ),
		createTable( client, db, packTable, tables, "pack" )
	] ).then( () => client );

	return {
		getEventsFor: getEventsFor.bind( null, pending, db, eventTable ),
		getEventPackFor: getEventPackFor.bind( null, pending, db, packTable ),
		storeEvents: storeEvents.bind( null, sliver, pending, db, eventTable ),
		storeEventPack: storeEventPack.bind( null, sliver, pending, packTable )
	};
};