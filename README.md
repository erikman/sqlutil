[![Build Status](https://travis-ci.org/erikman/sqlutil.svg?branch=master)](https://travis-ci.org/erikman/sqlutil)

# Wrapper for node-sqlite3

This is a wrapper module for node-sqlite3 which makes it possible to create sql queries using
mongodb like syntax, and also promisifies the API.

The support is incomplete and is currently only used for some personal projects. Pull requests are
welcome.

## Installation

```bash
npm install sqlutil
```

## Usage

see test suite for more:

```javascript
import sqlutil from 'sqlutil';
import Promise from 'bluebird';

let table;
let db = new sqlutil.Db();

db.open(':memory:').then(() => {
  table = new sqlutil.Table(db, {
      name: 'testtable',
      columns: {
        id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
        name: {type: sqlutil.DataType.TEXT, unique: true},
        value: {type: sqlutil.DataType.FLOAT}
      }
    });
  return table.createTable()
    .then(() => Promise.all([
      table.insert({name: 'key1', value: 42}),
      table.insert({name: 'key2', value: 42}),
      table.insert({name: 'key3', value: 43})
    ]);
});
```

We can perform queries on a table:
```javascript
table.find({name: 'key1'}).get()
  .then(row => {
    assert.equal(row.value, 42);
  });
```

And we can get a stream of rows
```javascript
table.find({value: {$gt: 42}}).stream();
```
