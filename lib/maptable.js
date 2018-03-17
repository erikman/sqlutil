import {DataType, Table} from './table';
import _ from 'lodash';

// Small key-value table
export class MapTable {
  constructor(db, name) {
    this.db = db;
    this.table = new Table(db, {
      name,
      columns: {
        id: {type: DataType.INTEGER, primaryKey: true},
        key: {type: DataType.TEXT, unique: true},
        value: {type: DataType.TEXT}
      }
    });
  }

  create() {
    return this.table.createTable();
  }

  get(key) {
    return this.table.find({key}).get().then(row => {
      return _.isUndefined(row) ? undefined : row.value;
    });
  }

  set(key, value) {
    return this.table.insertUpdateUnique({key, value});
  }
}
