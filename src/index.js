var rethinkdb = require( "rethinkdb" );
var modelStore = require( "./models" );
var eventStore = require( "./events" );
var sliver = require( "sliver" )();

function listTables( config, db, connectionPromise ) {
	return connectionPromise
		.then( 
			( client ) => 
			{
				return rethinkdb
					.db( db )
					.tableList()
					.run( client );
			},
			( err ) => {
				console.error( `Connection to ${config.host}:${config.port} failed with ${err.stack}` );
				throw err;
			}
		);
}

function initialize( config ) {
	var db = config.database || "test";
	var connection = rethinkdb.connect( {
		database: db,
		user: config.user || "admin",
		password: config.password || "",
		host: config.host || "localhost",
		timeout: config.timeout || 20,
		port: config.port || 28015,
		ssl: config.ssl
	} );

	var tablePromise = listTables( config, db, connection );
	return {
		model: { create: modelStore.bind( null, sliver, connection, db, tablePromise ) },
		event: { create: eventStore.bind( null, sliver, connection, db, tablePromise ) }
	}
}

module.exports = initialize;
