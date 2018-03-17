import stream from 'stream';

/**
 * Extends a stream of objects with data from a table.
 *
 * For example a stream of primaryKeys:
 * [{id: 1}, {id: 2}, {id: 3}, ...];
 * can beome
 * [{id: 1, name: 'key1', value: 42}, ...]
 */
export class SqlExtendStream extends stream.Transform {
  constructor(options, table) {
    super({
      ...options,
      objectMode: true
    });

    this.table = table;
  }

  _transform(chunk, encoding, done) {
    let uniqueColumns = this.table._extractUniqueColumns(chunk);
    return this.table.find(uniqueColumns).get().then(row => {
      let newRow = {
        ...row,
        ...chunk
      };
      this.push(newRow);
      done();
    });
  }
}
