var findModule = require( "./find");
 
module.exports = function( client, db, type ) {
	var table = `${type}_snapshot`;
	return {
		find: findModule.find.bind( null, client, db, table ),
	};
};