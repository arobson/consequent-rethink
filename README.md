# Consequent-Rethink
Provides model and event storage adapter for RethinkDB. Includes support for the `find` and most of the find operators.

> Note: this is an initial pass/proof of concept that doesn't support siblings - it will not detect network partitions in Rethink clusters that could occur during snapshots.

## General Approach
Snapshots, events and event packs are all stored in model/entity specific tables; a group of 3 tables will be created for each model in your system.

> Note: At any point in time, you should be able to eliminate any or all of the records in the snapshot and/or event pack tables and they will be rebuilt. Since eventpacks only exist as a record of the events that created a snapshot, you should remove the corresponding evenpack when removing a snapshot.

## Usage

```javascript
var consequentFn = require( "consequent" );

var rethink = require( "../src/index" )( {
	host: "localhost",
	database: "test"
} );

consequentFn( {
	modelStore: rethink.model,
	eventStore: rethink.event,
	models: "./models"
} ).then( ( consequent ) => {
	// consequent is ready and intitialized
} );
```

## To Do
 * test coverage