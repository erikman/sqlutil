import chai from 'chai';
import chaiAsPromised from 'chai-as-promised';

import * as sqlutil from '../lib/sqlutil';

chai.use(chaiAsPromised);
let expect = chai.expect;

describe('table with foreign keys', () => {
  let db;
  let parentTable;
  let childTable;

  beforeEach(() => {
    db = new sqlutil.Db();

    parentTable = new sqlutil.Table(db, {
      name: 'parent',
      columns: {
        id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
        name: {type: sqlutil.DataType.TEXT, unique: true}
      }
    });

    childTable = new sqlutil.Table(db, {
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

    return db.open(':memory:')
      .then(() => parentTable.createTable())
      .then(() => childTable.createTable());
  });

  function insertSomeData() {
    return parentTable.insert({id: 1, name: 'Banan'})
      .then(() => childTable.insert({parentId: 1, value: 'Apa'}))
      .then(() => childTable.insert({parentId: 1, value: 'Ape'}));
  }

  it('should be possible to enable support for foreign keys', () => {
    return expect(db.isForeignKeysEnabled(), 'initially disabled').to.eventually.equal(false)
      .then(() => db.enableForeignKeys())
      .then(() => expect(db.isForeignKeysEnabled(), 'enabled after requested').to.eventually.equal(true))
      .then(() => db.enableForeignKeys(false))
      .then(() => expect(db.isForeignKeysEnabled(), 'disabled after requested').to.eventually.equal(false));
  });

  it('should be possible to insert valid rows', () => {
    return db.enableForeignKeys().then(() => insertSomeData());
  });

  it('should be possible to insert foreign key validations when foreign keys are disabled', () => {
    return expect(childTable.insert({parentId: 42, value: 'Ape'}))
      .to.eventually.be.fulfilled;
  });

  it('should not be possible to insert invalid foreign keys when foreign keys are enabled', () => {
    return db.enableForeignKeys().then(() => {
      return expect(childTable.insert({parentId: 42, value: 'Ape'}))
        .to.eventually.be.rejectedWith(/FOREIGN KEY constraint failed/);
    });
  });

  it('should not be possible to update table when foreign keys are enabled', () => {
    return db.enableForeignKeys()
      .then(() => expect(childTable.createOrUpdateTable()).to.eventually.be.rejected);
  });

  it('should be possible to remove foreign keys from a table', () => {
    return insertSomeData()
      .then(() => {
        let newChildTable = new sqlutil.Table(db, {
          name: 'child',
          columns: {
            id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
            value: {type: sqlutil.DataType.TEXT, unique: true},
            parentId: {type: sqlutil.DataType.INTEGER}
          }
        });

        return expect(newChildTable.createOrUpdateTable())
          .to.eventually.deep.equal({wasCreated: false, wasUpdated: true});
      });
  });

  it('should be possible to add foreign keys to a table', () => {
    let newChildTable1 = new sqlutil.Table(db, {
      name: 'newChild',
      columns: {
        id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
        value: {type: sqlutil.DataType.TEXT, unique: true},
        parentId: {type: sqlutil.DataType.INTEGER}
      }
    });

    let newChildTable2 = new sqlutil.Table(db, {
      name: 'newChild',
      columns: {
        id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
        value: {type: sqlutil.DataType.TEXT, unique: true},
        parentId: {type: sqlutil.DataType.INTEGER}
      },
      foreignKeys: [
        {from: 'parentId', references: {parent: 'id'}}
      ]
    });

    return newChildTable1.createTable()
      .then(() => expect(newChildTable2.createOrUpdateTable())
            .to.eventually.deep.equal({wasCreated: false, wasUpdated: true}));
  });

  it('table should only be recreated when needed', () => {
    let newChildTable = new sqlutil.Table(db, {
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

    return newChildTable.createTableIfNotExists()
      .then(status => expect(status.wasCreated).to.be.false);
  });

  it('should be possible to update parent table when foreign keys are disabled', () => {
    let newParentTable = new sqlutil.Table(db, {
      name: 'parent',
      columns: {
        id: {type: sqlutil.DataType.INTEGER, primaryKey: true},
        name: {type: sqlutil.DataType.TEXT, unique: true},
        value: {type: sqlutil.DataType.FLOAT, defaultValue: 3.0}
      }
    });

    return insertSomeData()
      .then(() => expect(newParentTable.createOrUpdateTable())
            .to.eventually.deep.equal({wasCreated: false, wasUpdated: true}))
      .then(() => expect(newParentTable.find({name: 'Banan'}).get())
            .to.eventually.deep.equal({id: 1, name: 'Banan', value: 3}))
      .then(() => expect(childTable.find({value: 'Apa'}).get())
            .to.eventually.deep.equal({id: 1, value: 'Apa', parentId: 1}));
  });
});
