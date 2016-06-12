global.when = require( "when" );
var chai = require( "chai" );
chai.use( require( "chai-as-promised" ) );
global.should = chai.should();
global.expect = chai.expect;
global._ = require( "lodash" );
global.sinon = require( "sinon" );
chai.use( require( "sinon-chai" ) );
require( "sinon-as-promised" );