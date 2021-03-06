import chai from 'chai';
import chaiAsPromised from 'chai-as-promised';
import streamutil from 'streamutil';

import * as sqlutil from '../lib';

chai.use(chaiAsPromised);

let assert = chai.assert;
let expect = chai.expect;

describe('sqlutil', () => {
  let db;

  describe('basic operations', () => {
    it('should be able to create a database in memory', () => {
      db = new sqlutil.Db();
      return db.open(':memory:');
    });

    it('should be possible to create new tables', () => {
      let table = new sqlutil.Table(db, {
        name: 'testtable',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.FLOAT}
        }
      });

      return table.createTable();
    });

    it('should be possible to close the database', () => {
      return db.close();
    });
  });

  describe('query', () => {
    let table;

    beforeEach(() => {
      db = new sqlutil.Db();

      table = new sqlutil.Table(db, {
        name: 'testtable',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.FLOAT}
        }
      });

      return db.open(':memory:')
        .then(() => table.createTable());
    });

    function insertSomeData() {
      return table.insert({name: 'key1', value: 42})
        .then(() => table.insert({name: 'key2', value: 42}))
        .then(() => table.insert({name: 'key3', value: 43}));
    }

    it('allows custom queries with get interface', async () => {
      await insertSomeData();

      let q = sqlutil.query(db, 'SELECT * from testtable WHERE name = "key1"');
      let row = await q.get();
      expect(row).to.deep.equal({id: 1, name: 'key1', value: 42});
    });

    it('allows custom queries with params', async () => {
      await insertSomeData();

      let q = sqlutil.query(db, 'SELECT * from testtable WHERE name = $name', {
        $name: 'key1'
      });
      let row = await q.get();
      expect(row).to.deep.equal({id: 1, name: 'key1', value: 42});
    });

    it('can change params when getting', async () => {
      await insertSomeData();

      let q = sqlutil.query(db, 'SELECT * from testtable WHERE name = $name', {
        $name: 'key1'
      });
      let row = await q.get({$name: 'key3'});
      expect(row).to.deep.equal({id: 3, name: 'key3', value: 43});
    });

    it('can retrieve all results', async () => {
      await insertSomeData();

      let q = sqlutil.query(db, 'SELECT * from testtable WHERE value = $value', {
        $value: 42
      });
      let row = await q.all();
      expect(row).to.deep.equal([
        {id: 1, name: 'key1', value: 42},
        {id: 2, name: 'key2', value: 42}
      ]);
    });

    it('can prepare statement', async () => {
      await insertSomeData();

      let q = sqlutil.query(db, 'SELECT * from testtable WHERE name = $name', {
        $name: 'key1'
      });
      let sqlStatement = await q.prepare();
      let row = await sqlStatement.getAsync();
      expect(row).to.deep.equal({id: 1, name: 'key1', value: 42});
    });

    it('can stream results', async () => {
      await insertSomeData();

      let q = sqlutil.query(db, 'SELECT * from testtable WHERE value = $value', {
        $value: 42
      });
      let sqlStream = q.stream();
      return assert.eventually.lengthOf(streamutil.streamToArray(sqlStream), 2);
    });
  });

  describe('statement', () => {
    let table;

    beforeEach(() => {
      db = new sqlutil.Db();

      table = new sqlutil.Table(db, {
        name: 'testtable',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.FLOAT}
        }
      });

      return db.open(':memory:')
        .then(() => table.createTable());
    });

    function insertSomeData() {
      return table.insert({name: 'key1', value: 42})
        .then(() => table.insert({name: 'key2', value: 42}))
        .then(() => table.insert({name: 'key3', value: 43}));
    }

    it('allows custom queries with get interface', async () => {
      await insertSomeData();

      let q = await sqlutil.statement(db, 'SELECT * from testtable WHERE name = "key1"');
      let row = await q.get();
      expect(row).to.deep.equal({id: 1, name: 'key1', value: 42});
    });

    it('allows custom queries with params', async () => {
      await insertSomeData();

      let q = await sqlutil.statement(db, 'SELECT * from testtable WHERE name = $name', {
        $name: 'key1'
      });
      let row = await q.get();
      expect(row).to.deep.equal({id: 1, name: 'key1', value: 42});
    });

    it('can change params when getting', async () => {
      await insertSomeData();

      let q = await sqlutil.statement(db, 'SELECT * from testtable WHERE name = $name', {
        $name: 'key1'
      });
      let row = await q.get({$name: 'key3'});
      expect(row).to.deep.equal({id: 3, name: 'key3', value: 43});
    });

    it('can retrieve all results', async () => {
      await insertSomeData();

      let q = await sqlutil.statement(db, 'SELECT * from testtable WHERE value = $value', {
        $value: 42
      });
      let row = await q.all();
      expect(row).to.deep.equal([
        {id: 1, name: 'key1', value: 42},
        {id: 2, name: 'key2', value: 42}
      ]);
    });

    it('can stream results', async () => {
      await insertSomeData();

      let q = await sqlutil.statement(db, 'SELECT * from testtable WHERE value = $value', {
        $value: 42
      });
      let sqlStream = q.stream();
      return assert.eventually.lengthOf(streamutil.streamToArray(sqlStream), 2);
    });
  });

  describe('table', () => {
    let table;

    beforeEach(() => {
      db = new sqlutil.Db();

      table = new sqlutil.Table(db, {
        name: 'testtable',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.FLOAT}
        }
      });

      return db.open(':memory:')
        .then(() => table.createTable());
    });

    function insertSomeData() {
      return table.insert({name: 'key1', value: 42})
        .then(() => table.insert({name: 'key2', value: 42}))
        .then(() => table.insert({name: 'key3', value: 43}));
    }

    it('should not be possible to create an existing table', () => {
      return expect(table.createTable()).to.eventually.be.rejected;
    });

    it('should be possible to create an existing table with if not exists', () => {
      return expect(table.createTableIfNotExists()).to.eventually
        .deep.equal({wasCreated: false});
    });

    it('should be possible to update an existing table', () => {
      return expect(table.createOrUpdateTable()).to.eventually
        .deep.equal({wasCreated: false, wasUpdated: false});
    });

    it('should reject creating tables with unknown data types', () => {
      let badTable = new sqlutil.Table(db, {
        name: 'badtable',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.BAD_TYPE}
        }
      });

      return expect(badTable.createTable()).to.eventually.be.rejected;
    });

    it('should be possible to add rows to the table', () => {
      return insertSomeData();
    });

    it('should be possible to count number of rows in a table', () => {
      return insertSomeData()
        .then(() => expect(table.find().count()).to.eventually.equal(3));
    });

    it('should be possible to count number of rows from a query', () => {
      return insertSomeData()
        .then(() => expect(table.find({name: 'key1'}).count()).to.eventually.equal(1));
    });

    it('should be possible to count number of rows from an empty query', () => {
      return insertSomeData()
        .then(() => expect(table.find({name: 'non-existing-key'}).count()).to.eventually.equal(0));
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

    it('should return correct result for getSchema when a column have default value \'\'', () => {
      let defaultValueTable = new sqlutil.Table(db, {
        name: 'defaultValueTableEmptyString',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.TEXT, notNull: true, defaultValue: ''}
        }
      });

      return defaultValueTable.createTable()
        .then(() => defaultValueTable.getSchemaFromDatabase())
        .then(decodedSchema => {
          expect(decodedSchema.columns.value.defaultValue).to.equal('');
        });
    });

    it('should return correct result for getSchema when a column have default value 0', () => {
      let defaultValueTable = new sqlutil.Table(db, {
        name: 'defaultValueTableEmptyString',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.NUMERIC, notNull: true, defaultValue: 0}
        }
      });

      return defaultValueTable.createTable()
        .then(() => defaultValueTable.getSchemaFromDatabase())
        .then(decodedSchema => {
          expect(decodedSchema.columns.value.defaultValue).to.equal(0);
        });
    });

    it('should return correct result for getSchema when a column have default value 0.1', () => {
      let defaultValueTable = new sqlutil.Table(db, {
        name: 'defaultValueTableEmptyString',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.NUMERIC, notNull: true, defaultValue: 0.1}
        }
      });

      return defaultValueTable.createTable()
        .then(() => defaultValueTable.getSchemaFromDatabase())
        .then(decodedSchema => {
          expect(decodedSchema.columns.value.defaultValue).to.equal(0.1);
        });
    });

    it('should be possible to specify collate for columns', () => {
      let collateTable = new sqlutil.Table(db, {
        name: 'collateTable',
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true, collate: 'nocase'},
          value: {type: sqlutil.DataType.FLOAT, notNull: true, index: true}
        }
      });

      return collateTable.createTable()
        .then(() => collateTable.insert({name: 'a', value: 1.0}))
        .then(() => collateTable.find({name: 'A'}).get())
        .then(row => {
          expect(row).to.deep.equal({id: 1, name: 'a', value: 1.0});
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

    it('should be possible to retrieve single rows from the table', () => {
      return insertSomeData()
        .then(() => table.find({name: 'key1'}).get())
        .then(row => {
          assert.isObject(row);
          assert.equal(row.name, 'key1');
          assert.equal(row.value, 42);
        });
    });

    it('should be possible to retrieve multiple rows from the table', () => {
      return insertSomeData()
        .then(() => table.find({value: 42}).all())
        .then(rows => {
          assert.isArray(rows);
          assert.equal(rows.length, 2);
          assert.equal(rows[0].name, 'key1');
          assert.equal(rows[1].name, 'key2');
        });
    });

    it('should be possible to order the result rows without specifying order (default is ascending)', () => {
      return insertSomeData()
        .then(() => table.find({value: 42}).orderBy(['name']).all())
        .then(rows => {
          assert.isArray(rows);
          assert.equal(rows.length, 2);
          assert.equal(rows[0].name, 'key1');
          assert.equal(rows[1].name, 'key2');
        });
    });

    it('should be possible to order the result rows ascending', () => {
      return insertSomeData()
        .then(() => table.find({value: 42}).orderBy([{name: 1}]).all())
        .then(rows => {
          assert.isArray(rows);
          assert.equal(rows.length, 2);
          assert.equal(rows[0].name, 'key1');
          assert.equal(rows[1].name, 'key2');
        });
    });

    it('should be possible to order the result rows descending', () => {
      return insertSomeData()
        .then(() => table.find({value: 42}).orderBy([{name: -1}]).all())
        .then(rows => {
          assert.isArray(rows);
          assert.equal(rows.length, 2);
          assert.equal(rows[0].name, 'key2');
          assert.equal(rows[1].name, 'key1');
        });
    });

    it('should be possible to limit number of rows retreived', () => {
      return insertSomeData()
        .then(() => table.find({value: 42}).limit(1).all())
        .then(rows => {
          assert.isArray(rows);
          assert.equal(rows.length, 1);
          assert.equal(rows[0].name, 'key1');
        });
    });

    it('should be possible to iterate over rows from the table', () => {
      return insertSomeData()
        .then(() => {
          let rowCount = 0;
          return table.find({value: 42}).each(row => {
            rowCount++;
            assert(row.name === 'key1' || row.name === 'key2');
          }).then(() => {
            assert.equal(rowCount, 2);
          });
        });
    });

    it('should be possible to stream rows from the table', () => {
      return insertSomeData()
        .then(() => {
          let sqlStream = table.find({value: 42}).stream();
          return assert.eventually.lengthOf(streamutil.streamToArray(sqlStream), 2);
        });
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

      return streamutil.waitForStream(sqlPipeline)
        .then(() => expect(table.find().orderBy(['name']).all())
          .to.eventually.deep.equal([
            {id: 1, name: 'key5', value: 62},
            {id: 2, name: 'key6', value: 63},
            {id: 3, name: 'key7', value: 64}
          ]));
    });

    it('should be possible to lookup unique fields with a stream', () => {
      return insertSomeData()
        .then(() => table.insert({name: 'key5', value: 62}))
        .then(() => table.insert({name: 'key6', value: 63}))
        .then(() => table.insert({name: 'key7', value: 64}))
        .then(() => {
          let sourceStream = streamutil.arrayToStream([
            {name: 'key5'},
            {name: 'key6'},
            {name: 'key7', value: 1}, // Will override value from the table
            {name: 'key8'} // Does not exist
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
            assert.equal(rows[2].value, 1); // Overridden value

            assert.isUndefined(rows[3].id);
            assert.equal(rows[3].name, 'key8');
            assert.isUndefined(rows[3].value);
          });
        });
    });

    it('should be possible to select using multiple matchers', () => {
      return insertSomeData()
        .then(() => table.find({
          name: 'key1',
          value: 42
        }).get())
        .then(row => {
          assert.isObject(row);
          assert.equal(row.name, 'key1');
          assert.equal(row.value, 42);
        });
    });

    it('should be possible to select using multiple matchers as separate find calls', () => {
      return insertSomeData()
        .then(() => table
          .find({name: 'key1'})
          .find({value: 42})
          .get())
        .then(row => {
          assert.isObject(row);
          assert.equal(row.name, 'key1');
          assert.equal(row.value, 42);
        });
    });

    it('should be possible select using binary operators', () => {
      let rowCount = 0;
      return insertSomeData()
        .then(() => table.find({value: {$le: 42}}).each(row => {
          rowCount++;
          assert.isObject(row);
          assert(row.name === 'key1' || row.name === 'key2');
        })).then(() => {
          assert.equal(rowCount, 2);
        });
    });


    it('should be possible to select using explicit $and', () => {
      return insertSomeData()
        .then(() => table.find({$and: [
          {name: 'key1'},
          {value: 42}
        ]}).get())
        .then(row => {
          assert.equal(row.name, 'key1');
          assert.equal(row.value, 42);
        });
    });

    it('should not be possible to retrieve non-existing rows', () => {
      return insertSomeData()
        .then(() => table.find({name: 'non-existing-key'}).get())
        .then(row => {
          assert.isUndefined(row);
        });
    });

    it('should be possible to update rows', () => {
      return insertSomeData()
        .then(() => expect(table.insertUpdateUnique({name: 'key1', value: 50}))
          .to.eventually.deep.equal({
            id: 1,
            name: 'key1',
            value: 50
          }));
    });

    it('should be possible to update rows with null values', () => {
      return insertSomeData()
        .then(() => expect(table.insertUpdateUnique({name: 'key1', value: null}))
          .to.eventually.deep.equal({
            id: 1,
            name: 'key1',
            value: null
          }));
    });

    it('should be possible to create unique rows', () => {
      return insertSomeData()
        .then(() => table.insertUpdateUnique({
          name: 'new-unique-key',
          value: 33
        }))
        .then(row => {
          expect(row).to.deep.equal({
            id: 4,
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
      return insertSomeData()
        .then(() => table.find({name: 'key1'}).remove())
        .then(() => table.find({name: 'key1'}).get())
        .then(row => {
          assert.isUndefined(row);
        });
    });

    it('should be possible to create a table multiple times with createTableIfNotExists', () => {
      return insertSomeData()
        .then(() => table.createTableIfNotExists())
        .then(() => {
          // Verify that old data still remains
          return expect(table.find({name: 'key1'}).get())
            .to.eventually.deep.equal({
              id: 1,
              name: 'key1',
              value: 42
            });
        });
    });

    it('should give an error to create a table that already exists', () => {
      return expect(table.createTable()).to.eventually.be.rejected;
    });

    it('should be possible to add columns to the table', () => {
      let newTable = new sqlutil.Table(db, {
        name: 'testtable', // Same name as before
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true},
          value: {type: sqlutil.DataType.FLOAT},
          value2: {type: sqlutil.DataType.FLOAT, index: true, defaultValue: 3.0}
        }
      });

      return insertSomeData()
        .then(() => expect(newTable.createOrUpdateTable()).to.eventually.deep.equal({wasCreated: false, wasUpdated: true}))
        .then(() => expect(newTable.find({name: 'key3'}).get()).to.eventually.deep.equal({id: 3, name: 'key3', value: 43, value2: 3}));
    });

    it('should be possible to remove columns from the table', () => {
      let newTable = new sqlutil.Table(db, {
        name: 'testtable', // Same name as before
        columns: {
          id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
          name: {type: sqlutil.DataType.TEXT, unique: true}
          // Removed column "value"
        }
      });

      return insertSomeData()
        .then(() => expect(newTable.createOrUpdateTable())
          .to.eventually.deep.equal({wasCreated: false, wasUpdated: true}))
        .then(() => expect(newTable.find({name: 'key3'}).get())
          .to.eventually.deep.equal({id: 3, name: 'key3'}));
    });
  });
});
