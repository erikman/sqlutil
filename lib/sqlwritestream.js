import stream from 'stream';
import Promise from 'bluebird';
import _ from 'lodash';
import assert from 'assert';

export class SqlWriteStream extends stream.Writable {
  constructor(options, db, tableName) {
    super({
      highWaterMark: 256,
      ...options,
      objectMode: true
    });

    assert(db);
    assert(tableName);

    this.db = db;
    this.tableName = tableName;

    this.insertOrReplace = options.insertOrReplace;

    this.insertStatementPromise = null;
  }

  _createInsertStatement(row) {
    let columns = [];
    let values = [];

    for (let columnName in row) {
      if (_.has(row, columnName)) {
        columns[columns.length] = columnName;
        values[values.length] = `$${columnName}`;
      }
    }

    assert(columns.length > 0);

    let type = this.insertOrReplace ? 'INSERT OR REPLACE' : 'INSERT';

    let sql = `${type} INTO ${this.tableName} (${columns.join(', ')}) VALUES(${values.join(', ')})`;
    return this.db.prepare(sql);
  }

  _paramsFromRow(row) {
    let parsedValues = {};
    for (let columnName in row) {
      if (_.has(row, columnName)) {
        let paramName = `$${columnName}`;
        parsedValues[paramName] = row[columnName];
      }
    }
    return parsedValues;
  }

  _writeRows(rows) {
    if (rows.length === 0) {
      return Promise.resolve();
    }

    if (!this.insertStatementPromise) {
      this.insertStatementPromise = this._createInsertStatement(rows[0]);
    }

    return this.insertStatementPromise.then(insertStatement => {
      let promises = [];
      this.db.db.serialize(() => {
        promises.push(this.db.run('BEGIN TRANSACTION'));

        for (let i = 0; i < rows.length; i++) {
          promises.push(insertStatement.run(this._paramsFromRow(rows[i])));
        }

        promises.push(this.db.run('END TRANSACTION'));
      });

      return Promise.all(promises);
    });
  }

  _write(chunk, encoding, done) {
    this._writeRows([chunk]).then(() => done(), done);
  }

  _writev(chunks, done) {
    let rows = chunks.map(chunk => chunk.chunk);
    this._writeRows(rows).then(() => done(), done);
  }
}
