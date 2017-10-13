import chai from 'chai';
import chaiAsPromised from 'chai-as-promised';
import streamutil from 'streamutil';

import * as sqlutil from '../lib/sqlutil';

chai.use(chaiAsPromised);

let assert = chai.assert;
let expect = chai.expect;

describe('sqlutil', () => {
  let db;

  describe('table', () => {
    let table;

    it('should be able to create a database in memory', () => {
      db = new sqlutil.Db();
      return db.open(':memory:');
    });

    it('should be possible to create new tables', () => {
      table = new sqlutil.Table(db, {
        name: 'testtable',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.FLOAT}
        }
      });

      return table.createTable();
    });

    it('should not be possible to create an existing table', () => {
      return expect(table.createTable()).to.eventually.be.rejected;
    });

    it('should be possible to create an existing table with if not exists', () => {
      return expect(table.createTableIfNotExists()).to.eventually.
        deep.equal({wasCreated: false});
    });


    it('should reject creating tables with unknown data types', () => {
      let badTable = new sqlutil.Table(db, {
        name: 'badtable',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.BAD_TYPE}
        }
      });

      return assert.isRejected(badTable.createTable());
    });

    it('should be possible to add rows to the table', () => {
      return table.insert({name: 'key1', value: 42})
        .then(() => {
          return table.insert({name: 'key2', value: 42});
        }).then(() => {
          return table.insert({name: 'key3', value: 43});
        });
    });

    it('should be possible to count number of rows in a table', () => {
      return expect(table.find().count()).to.eventually.equal(3);
    });

    it('should be possible to count number of rows from a query', () => {
      return expect(table.find({name: 'key1'}).count()).to.eventually.equal(1);
    });

    it('should be possible to count number of rows from an empty query', () => {
      return expect(table.find({name: 'non-existing-key'}).count()).to.eventually.equal(0);
    });

    it('should be possible to have default values for columns', () => {
      let defaultValueTable = new sqlutil.Table(db, {
        name: 'defaultValueTable',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.FLOAT, notNull: true, defaultValue: 3.0}
        }
      });

      return defaultValueTable.createTable()
        .then(() => defaultValueTable.insert({name: 'keyWithValue', value: 1.0}))
        .then(() => defaultValueTable.insert({name: 'keyWithoutValue'}))
        .then(() => {
          return expect(defaultValueTable.find({name: 'keyWithValue'}).get())
            .to.eventually.deep.equal({id: 1, name: 'keyWithValue', value: 1.0});
        })
        .then(() => {
          return expect(defaultValueTable.find({name: 'keyWithoutValue'}).get())
            .to.eventually.deep.equal({id: 2, name: 'keyWithoutValue', value: 3.0});
        });
    });

    // Zero needs extra care since it is also 'false'
    it('should be possible to have default value 0 for columns', () => {
      let defaultValueTable = new sqlutil.Table(db, {
        name: 'defaultValueTableZero',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.INTEGER, notNull: true, defaultValue: 0}
        }
      });

      return defaultValueTable.createTable()
        .then(() => defaultValueTable.insert({name: 'keyWithValue', value: 1}))
        .then(() => defaultValueTable.insert({name: 'keyWithoutValue'}))
        .then(() => {
          return expect(defaultValueTable.find({name: 'keyWithValue'}).get())
            .to.eventually.deep.equal({id: 1, name: 'keyWithValue', value: 1});
        })
        .then(() => {
          return expect(defaultValueTable.find({name: 'keyWithoutValue'}).get())
            .to.eventually.deep.equal({id: 2, name: 'keyWithoutValue', value: 0});
        });
    });

    it('should be possible to have default value \'\' for columns', () => {
      let defaultValueTable = new sqlutil.Table(db, {
        name: 'defaultValueTableEmptyString',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.TEXT, notNull: true, defaultValue: ''}
        }
      });

      return defaultValueTable.createTable()
        .then(() => defaultValueTable.insert({name: 'keyWithValue', value: '1'}))
        .then(() => defaultValueTable.insert({name: 'keyWithoutValue'}))
        .then(() => {
          return expect(defaultValueTable.find({name: 'keyWithValue'}).get())
            .to.eventually.deep.equal({id: 1, name: 'keyWithValue', value: '1'});
        })
        .then(() => {
          return expect(defaultValueTable.find({name: 'keyWithoutValue'}).get())
            .to.eventually.deep.equal({id: 2, name: 'keyWithoutValue', value: ''});
        });
    });

    it('should be possible to have non-unique indices', () => {
      let indexTable = new sqlutil.Table(db, {
        name: 'indexTable',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.FLOAT, notNull: true, index: true}
        }
      });

      return indexTable.createTable();
    });

    it('should be possible to have index on multiple columns', () => {
      let indexTable = new sqlutil.Table(db, {
        name: 'multiIndexTable',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.FLOAT, notNull: true, index: true}
        },
        indices: {
          nameValue: {columns: ['name', 'value']}
        }
      });

      return indexTable.createTable();
    });


    it('should be a failure to name non-existing columns as part of an index', () => {
      let indexTable = new sqlutil.Table(db, {
        name: 'multiIndexTableFail',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.FLOAT, notNull: true, index: true}
        },
        indices: {
          nameValue: {columns: ['nonExisting', 'value']}
        }
      });

      return expect(indexTable.createTable()).to.eventually.be.rejected;
    });

    it('should be possible to have foreign keys between tables', () => {
      return db.enableForeignKeys().then(() => {
        let parentTable = new sqlutil.Table(db, {
          name: 'parent',
          columns: {
            id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
            name: {type: sqlutil.DataType.TEXT, unique: true}
          }
        });

        let childTable = new sqlutil.Table(db, {
          name: 'child',
          columns: {
            id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
            value: {type: sqlutil.DataType.TEXT, unique: true},
            parentId: {type: sqlutil.DataType.INTEGER}
          },
          foreignKeys: [
            {from: 'parentId', references: {parent: 'id'}}
          ]
        });

        return Promise.all([
          parentTable.createTable(),
          childTable.createTable()
        ]).then(() => {
          // Insert a parent value
          return parentTable.insert({id: 1, name: 'Banan'})
            .then(() => childTable.insert({parentId: 1, value: 'Apa'}))
            .then(() => childTable.insert({parentId: 1, value: 'Ape'}));
        });
      });
    });

    it('should be possible to retrieve single rows from the table', () => {
      return table.find({name: 'key1'}).get().then(row => {
        assert.isObject(row);
        assert.equal(row.name, 'key1');
        assert.equal(row.value, 42);
      });
    });

    it('should be possible to retrieve multiple rows from the table', () => {
      return table.find({value: 42}).all().then(rows => {
        assert.isArray(rows);
        assert.equal(rows.length, 2);
        assert.equal(rows[0].name, 'key1');
        assert.equal(rows[1].name, 'key2');
      });
    });

    it('should be possible to order the result rows without specifying order (default is ascending)', () => {
      return table.find({value: 42}).orderBy(['name']).all().then(rows => {
        assert.isArray(rows);
        assert.equal(rows.length, 2);
        assert.equal(rows[0].name, 'key1');
        assert.equal(rows[1].name, 'key2');
      });
    });

    it('should be possible to order the result rows ascending', () => {
      return table.find({value: 42}).orderBy([{name: 1}]).all().then(rows => {
        assert.isArray(rows);
        assert.equal(rows.length, 2);
        assert.equal(rows[0].name, 'key1');
        assert.equal(rows[1].name, 'key2');
      });
    });

    it('should be possible to order the result rows descending', () => {
      return table.find({value: 42}).orderBy([{name: -1}]).all().then(rows => {
        assert.isArray(rows);
        assert.equal(rows.length, 2);
        assert.equal(rows[0].name, 'key2');
        assert.equal(rows[1].name, 'key1');
      });
    });

    it('should be possible to limit number of rows retreived', () => {
      return table.find({value: 42}).limit(1).all().then(rows => {
        assert.isArray(rows);
        assert.equal(rows.length, 1);
        assert.equal(rows[0].name, 'key1');
      });
    });

    it('should be possible to iterate over rows from the table', () => {
      let rowCount = 0;
      return table.find({value: 42}).each(row => {
        rowCount++;
        assert(row.name === 'key1' || row.name === 'key2');
      }).then(() => {
        assert.equal(rowCount, 2);
      });
    });

    it('should be possible to stream rows from the table', () => {
      let sqlStream = table.find({value: 42}).stream();

      assert.eventually.lengthOf(streamutil.streamToArray(sqlStream), 2);
    });

    it('should be possible to stream rows into a table', () => {
      let sourceStream = streamutil.arrayToStream([
        {name: 'key5', value: 62},
        {name: 'key6', value: 63},
        {name: 'key7', value: 64}
      ]);

      let sqlWriter = table.createWriteStream();

      let sqlPipeline = streamutil.pipeline([
        sourceStream,
        sqlWriter
      ]);

      return streamutil.waitForStream(sqlPipeline);
    });

    it('should be possible to lookup unique fields with a stream', () => {
      let sourceStream = streamutil.arrayToStream([
        {name: 'key5'},
        {name: 'key6'},
        {name: 'key7', value: 1}, // Will override value from the table
        {name: 'key8'} // does not exist
      ]);

      let sqlTransform = table.createExtendStream();

      return streamutil.streamToArray(streamutil.pipeline([
        sourceStream,
        sqlTransform
      ])).then(rows => {
        assert.lengthOf(rows, 4);

        assert.isNumber(rows[0].id);
        assert.equal(rows[0].name, 'key5');
        assert.equal(rows[0].value, 62);

        assert.isNumber(rows[1].id);
        assert.equal(rows[1].name, 'key6');
        assert.equal(rows[1].value, 63);

        assert.isNumber(rows[2].id);
        assert.equal(rows[2].name, 'key7');
        assert.equal(rows[2].value, 1); // overridden value

        assert.isUndefined(rows[3].id);
        assert.equal(rows[3].name, 'key8');
        assert.isUndefined(rows[3].value);
      });
    });

    it('should be possible to select using multiple matchers', () => {
      return table.find({
        name: 'key1',
        value: 42
      }).get().then(row => {
        assert.isObject(row);
        assert.equal(row.name, 'key1');
        assert.equal(row.value, 42);
      });
    });

    it('should be possible to select using multiple matchers as separate find calls', () => {
      return table
        .find({name: 'key1'})
        .find({value: 42})
        .get().then(row => {
          assert.isObject(row);
          assert.equal(row.name, 'key1');
          assert.equal(row.value, 42);
        });
    });

    it('should be possible select using binary operators', () => {
      let rowCount = 0;
      return table.find({value: {$le: 42}}).each(row => {
        rowCount++;
        assert.isObject(row);
        assert(row.name === 'key1' || row.name === 'key2');
      }).then(() => {
        assert.equal(rowCount, 2);
      });
    });


    it('should be possible to select using explicit $and', () => {
      return table.find({$and: [
        {name: 'key1'},
        {value: 42}
      ]}).get().then(row => {
        assert.equal(row.name, 'key1');
        assert.equal(row.value, 42);
      });
    });

    it('should not be possible to retrieve non-existing rows', () => {
      return table.find({name: 'non-existing-key'}).get().then(row => {
        assert.isUndefined(row);
      });
    });

    it('should be possible to update rows', () => {
      return expect(table.insertUpdateUnique({name: 'key1', value: 50}))
        .to.eventually.deep.equal({
          id: 1,
          name: 'key1',
          value: 50
        });
    });

    it('should be possible to update rows with null values', () => {
      return expect(table.insertUpdateUnique({name: 'key1', value: null}))
        .to.eventually.deep.equal({
          id: 1,
          name: 'key1',
          value: null
        });
    });

    it('should be possible to create unique rows', () => {
      return table.insertUpdateUnique({name: 'new-unique-key', value: 33}).then(row => {
        expect(row).to.deep.equal({
          id: 7,
          name: 'new-unique-key',
          value: 33
        });

        // Update the row
        return expect(table.insertUpdateUnique({
          id: row.id,
          value: 34
        })).to.eventually.deep.equal({
          id: row.id,
          name: 'new-unique-key',
          value: 34
        });
      });
    });

    it('should be possible to delete rows', () => {
      return table.find({name: 'key1'}).remove().then(() => {
        return table.find({name: 'key1'}).get();
      }).then(row => {
        assert.isUndefined(row);

        // Restore the table
        return table.insert({name: 'key1', value: 42});
      });
    });

    it('should be possible to create a table multiple times with createTableIfNotExists', () => {
      return table.createTableIfNotExists().then(() => {
        // Verify that old data still remains
        return expect(table.find({name: 'key1'}).get())
          .to.eventually.deep.equal({
            id: 8,
            name: 'key1',
            value: 42
          });
      });
    });

    it('should give an error to create a table that already exists', () => {
      return expect(table.createTable()).to.eventually.be.rejected;
    });

    it('should not be possible to add columns to the table', () => {
      let newTable = new sqlutil.Table(db, {
        name: 'testtable', // same name as before
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.FLOAT},
          value2: {type: sqlutil.DataType.FLOAT, index: true, defaultValue: 3.0}
        }
      });

      return expect(newTable.createTableIfNotExists()).to.eventually.be.rejected;
    });

    it('should not be possible to remove columns from the table', () => {
      let newTable = new sqlutil.Table(db, {
        name: 'testtable', // same name as before
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true}
        }
      });

      return expect(newTable.createTableIfNotExists()).to.eventually.be.rejected;
    });
  });

  describe('MapTable', () => {
    let mapTable;

    it('can be constructed', () => {
      mapTable = new sqlutil.MapTable(db, 'testMapTable');
      return mapTable.create();
    });

    it('should be possible to add values to the map', () => {
      return Promise.all([
        mapTable.set('key1', 'value1'),
        mapTable.set('key2', 'value2'),
        mapTable.set('key3', 'value3')
      ]);
    });

    it('should be possible to retrieve values from the map', () => {
      return mapTable.get('key1').then(value => {
        assert.equal(value, 'value1');
      });
    });

    it('should not return any values for undefined keys', () => {
      return mapTable.get('undefined key').then(value => {
        assert.typeOf(value, 'undefined');
      });
    });

    it('should be possible to update keys in the map', () => {
      return mapTable.set('key1', 'updated value1')
        .then(() => mapTable.get('key1'))
        .then(value => {
          assert.equal(value, 'updated value1');
        });
    });
  });
});
