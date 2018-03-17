import chai from 'chai';
import chaiAsPromised from 'chai-as-promised';

import * as sqlutil from '../lib';

chai.use(chaiAsPromised);

let assert = chai.assert;

describe('MapTable', () => {
  let db;
  let mapTable;

  before(() => {
    db = new sqlutil.Db();
    return db.open(':memory:');
  });

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
