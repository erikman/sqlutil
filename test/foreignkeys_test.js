import chai from 'chai';
import chaiAsPromised from 'chai-as-promised';
import streamutil from 'streamutil';

import * as sqlutil from '../lib/sqlutil';

chai.use(chaiAsPromised);
let expect = chai.expect;

describe('table with foreign keys', () => {
  let db;
  let table;
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
      .then(() => db.enableForeignKeys())
      .then(() => parentTable.createTable())
      .then(() => childTable.createTable());
  });

  function insertSomeData() {
    return parentTable.insert({id: 1, name: 'Banan'})
      .then(() => childTable.insert({parentId: 1, value: 'Apa'}))
      .then(() => childTable.insert({parentId: 1, value: 'Ape'}));
  }
  
  it('should be possible to insert valid rows', () => {
    return insertSomeData();
  });

  it('should not be possible to insert foreign key validations', () => {
    return expect(childTable.insert({parentId: 42, value: 'Ape'})).to.eventually.be.rejectedWith(/FOREIGN KEY constraint failed/);
  });

  it('should not be possible to remove foreign keys from a table', () => {
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

        return expect(newChildTable.createTableIfNotExists())
          .to.eventually.be.rejected;
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

    return newChildTable1.createTableIfNotExists()
      .then(status => {
        expect(status.wasCreated).to.be.true;
        return expect(newChildTable2.createTable()).to.eventually.be.rejected;
      })
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
});
