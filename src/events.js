var _ = require( "lodash" );
var when = require( "when" );
var sequence = require( "when/sequence" );
var zlib = require( "zlib" );
var rethink = require( "rethinkdb" );
var findModule = require( "./find");

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
				.then( () => createPrimaryIndex( client, db, table ) );
		} else {
			return when( client );
		}
	}

	return when.join( connection, tablePromise )
		.then( onTableListing );
}

function createIndices( client, db, table, indices ) {
	return rethink
		.db( db )
		.table( table )
		.indexList()
		.run( client )
		.then( ( list ) => {
			return when.all( _.map( indices, ( indexName ) => {
				if( list.indexOf( indexName ) < 0 ) {
					return rethink
						.db( db )
						.table( table )
						.indexCreate( indexName )
						.run( client );
				} else {
					return when();
				}
			} ) );
		} );
}

function createPrimaryIndex( client, db, table, index ) {
	return sequence( [
		() => rethink
			.db( db )
			.table( table )
			.indexCreate( "_modelId" )
			.run( client ),
		() => rethink
			.db( db )
			.table( table )
			.indexCreate( "instance", [
				rethink.row( "_modelId" ),
				rethink.row( "_vector" )
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

function findEvents( pending, db, table, criteria, lastEventId ) {
	return pending.then( ( c ) => 
		rethink
			.db( db )
			.table( table )
			.between( lastEventId || rethink.minval, rethink.maxval, { index: "id", leftBound: "open" } )
			.filter( findModule.buildCriteria( criteria ) )
			.run( c )
	)
	.then( ( x ) => x.toArray() );
}

function getEventsByIndex( pending, db, table, indexName, indexValue, lastEventId ) {
	return pending.then( ( c ) =>
		rethink
			.db( db )
			.table( table )
			.between( lastEventId || rethink.minval, rethink.maxval, { index: "id", leftBound: "open" } )
			.fetch( r.row( indexName ).eq( indexValue ) )
			.orderBy( rethink.desc( "id" ) )
			.run( c ) )
	.then( ( x ) => x.toArray() );
}

function getEventsFor( pending, db, table, modelId, lastEventId ) {
	return pending.then( ( c ) =>
		rethink
			.db( db )
			.table( table )
			.between( lastEventId || rethink.minval, rethink.maxval, { index: "id", leftBound: "open" } )
			.filter( { _modelId: modelId } )
			.orderBy( rethink.desc( "id" ) )
			.run( c ) )
	.then( ( x ) => x.toArray() );
}

function getEventPackFor( pending, db, table, modelId, vectorClock ) {
	return pending.then( ( c ) =>
		rethink
			.db( db )
			.table( table )
			.getAll( [ modelId, vectorClock ], { index: "instance" } )
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

function storeEvents( sliver, pending, db, table, modelId, events ) {
	var indices = [];
	var storageFormat = _.map( events, ( e ) => {
		if( e.indexBy ) {
			_.each( e.indexBy, ( v, k ) => {
				if( !_.isInteger( k ) && !e[ k ] ) {
					e[ k ] = v;
					indices.push( v );
				} else {
					indices.push( v );
				}
			} );
		}
		return _.omitBy( e, ( v ) => !v );
	} );
		indices = _.uniq( indices );
	return pending.then( ( c ) => {
		createIndices( c, db, table, indices )
		return rethink
			.db( db )
			.table( table )
			.insert( storageFormat )
			.run( c ) 
	} );
}

function storeEventPack( sliver, pending, packTable, modelId, vectorClock, events ) {
	var pack = {
		events,
		modelId: modelid,
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