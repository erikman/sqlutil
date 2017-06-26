import sqlite3 from 'sqlite3';
import stream from 'stream';
import Promise from 'bluebird';
import _ from 'lodash';
import assert from 'assert';

export const DataType = {
  INTEGER: 'INTEGER',
  BOOLEAN: 'INTEGER',
  DATE: 'INTEGER',
  TEXT: 'TEXT',
  FLOAT: 'REAL',
  REAL: 'REAL'
};

// sqlite3.verbose();

// Like promisifyAll, but with specific list of functions (sqlite3 functions
// are not iterable for the native classes).
function promisifyFunctions(klass, promiseFunctions) {
  for (let i = 0; i < promiseFunctions.length; i++) {
    let fnc = promiseFunctions[i];
    let asyncFnc = `${fnc}Async`;

    if (klass.prototype[fnc] && !klass.prototype[asyncFnc]) {
      klass.prototype[asyncFnc] = function (...params) {
        return Promise.fromCallback(klass.prototype[fnc].bind(this, ...params));
      };
    }
  }
}

promisifyFunctions(sqlite3.Statement, [
  'all', 'bind', 'get', 'getMultiple'
]);

export class Db {
  constructor(db = null) {
    this.db = db;
  }

  open(...params) {
    return new Promise((resolve, reject) => {
      this.db = new sqlite3.Database(...params, err => {
        if (err) {
          reject(err);
        }
        else {
          resolve();
        }
      });
    });
  }

  run(sql, params) {
    if (!params) {
      params = [];
    }

    const self = this;
    return new Promise((resolve, reject) => {
      self.db.run(sql, params, function (err, ...results) {
        if (err) {
          reject(new Error(`${err.message}, when running ${sql} with ${JSON.stringify(params)}`));
        }
        else {
          // this is the statement object and contains lastID and changes
          // which contain the value of the last inserted row ID (if the sql
          // was an insert statement) and the number of rows affected by this
          // query respectively.
          resolve(this, ...results);
        }
      });
    });
  }

  get(sql, params) {
    if (!params) {
      params = [];
    }

    const self = this;
    return new Promise((resolve, reject) => {
      self.db.get(sql, params, (err, row) => {
        if (err) {
          reject(new Error(`${err.message}, when running ${sql} with ${JSON.stringify(params)}`));
        }
        else {
          // this is the statement object and contains lastID and changes
          // which contain the value of the last inserted row ID (if the sql
          // was an insert statement) and the number of rows affected by this
          // query respectively.
          resolve(row);
        }
      });
    });
  }

  all(sql, params) {
    if (!params) {
      params = [];
    }

    const self = this;
    return new Promise((resolve, reject) => {
      self.db.all(sql, params, (err, rows) => {
        if (err) {
          reject(new Error(`${err.message}, when running ${sql} with ${JSON.stringify(params)}`));
        }
        else {
          // this is the statement object and contains lastID and changes
          // which contain the value of the last inserted row ID (if the sql
          // was an insert statement) and the number of rows affected by this
          // query respectively.
          resolve(rows);
        }
      });
    });
  }

  each(sql, params, callback) {
    if (!params) {
      params = [];
    }

    const self = this;
    return new Promise((resolve, reject) => {
      self.db.each(sql, params, (err, row) => {
        // Callback for each row
        if (err) {
          reject(new Error(`${err.message}, when running ${sql} with ${JSON.stringify(params)}`));
        }
        else {
          callback(row);
        }
      }, (err, rowCount) => {
        // Callback after all rows have been fetched
        if (err) {
          console.error(`each ${sql} returned error ${err}`);
          reject(err);
        }
        else {
          resolve(rowCount);
        }
      });
    });
  }

  prepare(sql, params) {
    if (!params) {
      params = [];
    }

    return new Promise((resolve, reject) => {
      let statement = this.db.prepare(sql, params, err => {
        if (err) {
          statement.finalize();
          reject(new Error(`${err.message}, when preparing ${sql} with ${JSON.stringify(params)}`));
        }
        else {
          resolve(statement);
        }
      });
    });
  }
}


/**
 * @brief Class that creates a Readable stream from a SQL statement.
 *
 * Example:
 * @code
 * let stmt = Database.prepare('SELECT * FROM myTable');
 * let stream = new SqlReadStream(stmt);
 *
 * // and then
 * stream.pipe(...)
 * // or
 * stream.on('data', row => {
 *   console.log(row);
 * });
 * @endcode
 */
export class SqlReadStream extends stream.Readable {
  /**
   * Extra options:
   * highWaterMark [int, 1024]. The number of rows to cache.
   * finalizeStatement [bool, default=true], True if the statement should be
   *   finalized when the stream ends or gets an error.
   *
   * @param statementPromise statement or promise for a statement that we will
   *   retrieve rows from.
   */
  constructor(options, statementPromise) {
    if (options instanceof sqlite3.Statement || _.isFunction(options.then)) {
      statementPromise = options;
      options = undefined;
    }

    options = {
      highWaterMark: 1024, // default is 16
      finalizeStatement: true,
      ...options,
      objectMode: true
    };
    super(options);

    // Save the parameters
    this.readChunkSize = options.highWaterMark;
    this.finalizeStatement = options.finalizeStatement;

    assert(_.isObject(statementPromise));
    if (!_.isFunction(statementPromise.then)) {
      statementPromise = Promise.resolve(statementPromise);
    }

    this.statementPromise = statementPromise;
    this.asyncFetch = null;

    this.hasError = false;
    this.hasBeenDestroyed = false;
    this.endOfData = false;

    this.rows = null;
    this.currentRow = 0;

    this.totalPushedRows = 0;

    this.on('error', () => this.destroy());
    this.on('end', () => this.destroy());
  }

  close() {
    this.destroy();
  }

  destroy() {
    if (!this.hasBeenDestroyed) {
      this.hasBeenDestroyed = true;

      process.nextTick(() => this.emit('close'));

      if (this.finalizeStatement) {
        this.statementPromise.then(statement => statement.finalize());
      }
    }
  }

  _asyncFetchChunk() {
    if (this.asyncFetch) {
      // Fetch is already in progress
      return this.asyncFetch;
    }
    if (this.endOfData) {
      return Promise.resolve();
    }

    this.asyncFetch = this.statementPromise
      .then(statement => {
        // Do we have support for getMultiple?
        if (statement.getMultipleAsync) {
          return statement.getMultipleAsync(this.readChunkSize);
        }

        let promises = [];
        for (let i = 0; i < this.readChunkSize; i++) {
          promises.push(statement.getAsync());
        }
        return Promise.all(promises).then(rows => {
          // We will get 'undefined' after EOF
          if (!rows[rows.length - 1]) {
            for (let i = 0; i < rows.length; i++) {
              if (!rows[i]) {
                rows.splice(i, rows.length);
                break;
              }
            }
          }
          return rows;
        });
      })
      .then(rows => {
        this.asyncFetch = null;

        if (this.rows) {
          this.rows = this.rows.concat(rows);
        }
        else {
          this.rows = rows;
        }

        // Push an extra 'null' at end of stream
        if (this.rows.length < this.readChunkSize) {
          this.endOfData = true;
          this.rows.push(null);
        }
      });

    return this.asyncFetch;
  }

  _read() {
    if (!this.hasError) {
      // Possibly start fetching more data
      this._asyncFetchChunk();

      // Do we have rows from a previous fetch that we should return?
      if (this.rows) {
        for (; this.currentRow < this.rows.length; this.currentRow++) {
          this.totalPushedRows++;
          if (!this.push(this.rows[this.currentRow])) {
            // Wait with pushing more data into the stream until next handler
            // is ready for it.
            this.currentRow++;
            break;
          }
        }

        // Have we processed all rows in the cunk?
        if (this.currentRow === this.rows.length) {
          // Nothing left in the chunk, we need to fetch more
          this.rows = null;
          this.currentRow = 0;
        }
      }
      else {
        // We have completely exhausted the rows we have
        this._asyncFetchChunk()
          .then(() => {
            // Push one row into the stream so _read gets called again
            // (only return one row when we are in async callback, _read
            // might get invoked recursively as part of the push call).
            this.currentRow = 1;
            let dataToPush = this.rows[0];

            // Have we processed all rows in the chunk?
            if (this.currentRow === this.rows.length) {
              // Nothing left in the chunk, we need to fetch more
              this.rows = null;
              this.currentRow = 0;
            }

            this.totalPushedRows++;
            this.push(dataToPush);
          })
          .catch(err => {
            this.hasError = true;
            process.nextTick(() => this.emit('error', err));
          });
      }
    }
  }
}


/**
 * Extends a stream of objects with data from a table.
 *
 * For example a stream of primaryKeys:
 * [{id: 1}, {id: 2}, {id: 3}, ...];
 * can beome
 * [{id: 1, name: 'key1', value: 42}, ...]
 */
class SqlExtendStream extends stream.Transform {
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


class SqlWriteStream extends stream.Writable {
  constructor(options, table) {
    super({
      highWaterMark: 256,
      ...options,
      objectMode: true
    });

    assert(table);
    assert(table instanceof Table);

    this.table = table;
    this.db = table.db;

    this.insertOrReplace = options.insertOrReplace;

    this.insertStatementPromise = null;
  }

  _createInsertStatement(row) {
    let columns = [];
    let values = [];

    for (let columnName in row) {
      if (row.hasOwnProperty(columnName)) {
        columns[columns.length] = columnName;
        values[values.length] = `$${columnName}`;
      }
    }

    assert(columns.length > 0);

    let type = this.insertOrReplace ? 'INSERT OR REPLACE' : 'INSERT';

    let sql = `${type} INTO ${this.table.schema.name} (${columns.join(', ')}) VALUES(${values.join(', ')})`;
    return this.db.prepare(sql);
  }

  _paramsFromRow(row) {
    let parsedValues = {};
    for (let columnName in row) {
      if (row.hasOwnProperty(columnName)) {
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


class SqlStatementParameters {
  constructor() {
    this.params = {};
    this.id = 1;
  }

  _sqliteConvertValue(x) {
    switch (typeof x) {
      case 'bool':
        return x ? 0 : 1;

      case 'object':
        if (x instanceof Date) {
          return x.getUtc();
        }
        if (x === null) {
          return x;
        }
        throw new Error(`Invalid type ${typeof x} for sql parameter ${JSON.stringify(x)}`);

      default:
        return x;
    }
  }

  addParam(value) {
    let uniqueIdentifier = `$p${this.id}`;
    this.id++;
    this.params[uniqueIdentifier] = this._sqliteConvertValue(value);
    return uniqueIdentifier;
  }
}

export class SqlQuery {
  constructor(db) {
    this.db = db;
    this.selectColumnList = null;
    this.fromTable = null;
    this.whereFilter = null;
    this.limitCount = 0;
    this.offsetCount = null;
    this.orderByColumns = [];
  }

  select(selectColumnList) {
    this.selectColumnList = selectColumnList;
    return this;
  }

  from(fromTable) {
    this.fromTable = fromTable;
    return this;
  }

  find(queryColumns) {
    if (Object.keys(queryColumns).length === 0) {
      throw new Error('No columns specified for where clause');
    }
    this.whereFilter = {
      ...this.whereFilter,
      ...queryColumns
    };
    return this;
  }

  groupBy(columns) {
    this.groupByColumns = columns;
    return this;
  }

  limit(count) {
    if (count <= 0) {
      throw new Error(`Invalid limit: ${count}`);
    }
    this.limitCount = count;
    return this;
  }

  offset(count) {
    if (count < 0) {
      throw new Error(`Invalid offset: ${count}`);
    }
    this.offsetCount = count;
    return this;
  }

  orderBy(columns) {
    this.orderByColumns = this.orderByColumns.concat(columns);
    return this;
  }

  _parseUnaryExpression(parameters, expression) {
    if ((_.isString(expression) && expression.charAt(0) !== '$')
        || _.isNumber(expression)) {
      let identifier = parameters.addParam(expression);
      return `${identifier}`;
    }
    return null;
  }

  _parseLogicalExpression(parameters, left, rest) {
    if (_.isArray(rest)) {
      const LOGICAL_OPERATORS = {
        $and: ' AND ',
        $or: ' OR '
      };

      for (let key in LOGICAL_OPERATORS) {
        if (LOGICAL_OPERATORS.hasOwnProperty(key)) {
          let op = LOGICAL_OPERATORS[key];

          if (left === key) {
            let sqlExpressions = rest.map(expression => {
              let value = this._parseExpression(parameters, expression);
              if (!value) {
                throw new Error(`Invalid syntax for ${JSON.stringify(expression)}`);
              }
              return value;
            });

            return `(${sqlExpressions.join(op)})`;
          }
        }
      }
    }
  }

  _parseBinaryExpression(parameters, left, rest) {
    if (_.isObject(rest)) {
      if (Object.keys(rest).length !== 1) {
        throw new Error(`Invalid syntax for ${JSON.stringify(rest)}`);
      }

      const BINARY_OPERATORS = {
        $eq: '=',
        $gt: '>',
        $lt: '<',
        $ge: '>=',
        $le: '<='
      };

      for (let key in BINARY_OPERATORS) {
        if (BINARY_OPERATORS.hasOwnProperty(key)) {
          let op = BINARY_OPERATORS[key];
          if (rest.hasOwnProperty(key)) {
            let rightExpression = rest[key];
            let rightValue = this._parseExpression(parameters, rightExpression);
            if (!rightValue) {
              throw new Error(`Invalid syntax for ${JSON.stringify(rightExpression)}`);
            }

            return `(${left} ${op} ${rightValue})`;
          }
        }
      }
    }
  }

  // We can handle
  // {a: b}           => (a = b)
  // {a: {$eq: b}}    => (a = b)
  // {a: {$gt: b}}    => (a > b)
  // {$and: [a, b, c]}   => (a AND b AND c)
  _parseExpression(parameters, expression) {
    // Try to parse as unary expression
    let unaryValue = this._parseUnaryExpression(parameters, expression);
    if (unaryValue) {
      return unaryValue;
    }

    if (_.isObject(expression)) {
      let expressionKeys = Object.keys(expression);
      if (expressionKeys.length > 1) {
        // Implicit AND operation between the elements
        return expressionKeys.map(key => {
          let value = expression[key];
          return this._parseExpression(parameters, {[key]: value});
        }).join(' AND ');
      }

      if (expressionKeys.length !== 1) {
        throw new Error(`Invalid syntax for ${JSON.stringify(expression)}`);
      }

      let left = Object.keys(expression)[0];
      let right = expression[left];

      // Implicit equals comparison
      let unaryRightValue = this._parseUnaryExpression(parameters, right);
      if (unaryRightValue) {
        return `(${left} = ${unaryRightValue})`;
      }

      // Try to parse as logical expression
      let logicalValue = this._parseLogicalExpression(parameters, left, right);
      if (logicalValue) {
        return logicalValue;
      }

      // Try to parse as binary expression
      let binaryValue = this._parseBinaryExpression(parameters, left, right);
      if (binaryValue) {
        return binaryValue;
      }
    }

    throw new Error(`Invalid syntax for ${JSON.stringify(expression)}`);
  }

  _buildSqlWhere(parameters) {
    let expressionSql = this._parseExpression(parameters, this.whereFilter);
    return `WHERE (${expressionSql})`;
  }

  _buildSqlQuery() {
    let parameters = new SqlStatementParameters();

    let whereSql = '';
    if (this.whereFilter) {
      whereSql = this._buildSqlWhere(parameters);
    }

    let selectColumnsSql = '*';
    if (this.selectColumnList) {
      if (_.isString(this.selectColumnList)) {
        selectColumnsSql = this.selectColumnList;
      }
      else {
        selectColumnsSql = this.selectColumnList.join(', ');
      }
    }

    let limitSql = '';
    if (this.limitCount > 0) {
      limitSql = `LIMIT ${this.limitCount}`;
    }

    let offsetSql = '';
    if (this.offsetCount > 0) {
      offsetSql = `OFFSET ${this.offsetCount}`;
    }

    let groupBySql = '';
    if (this.groupByColumns) {
      const groupByList = this.groupByColumns.join(', ');
      groupBySql = `GROUP BY ${groupByList}`;
    }

    let orderBySql = '';
    if (this.orderByColumns.length > 0) {
      const columnList = this.orderByColumns.join(', ');
      orderBySql = `ORDER BY ${columnList}`;
    }

    return {
      columns: selectColumnsSql,
      from: `FROM ${this.fromTable}`,
      where: whereSql,
      limit: limitSql,
      offset: offsetSql,
      groupBy: groupBySql,
      orderBy: orderBySql,
      parameters,
    };
  }

  _buildSelectQuery() {
    const query = this._buildSqlQuery();
    let sql = `SELECT ${query.columns} ${query.from} ${query.where} ${query.groupBy} ${query.orderBy} ${query.limit} ${query.offset};`;
    return {
      sql,
      params: query.parameters.params
    };
  }

  all() {
    const query = this._buildSelectQuery();
    return this.db.all(query.sql, query.params);
  }

  get() {
    const query = this._buildSelectQuery();
    return this.db.get(query.sql, query.params);
  }

  each(callback) {
    const query = this._buildSelectQuery();
    return this.db.each(query.sql, query.params, callback);
  }

  prepareSelect() {
    const query = this._buildSelectQuery();
    return this.db.prepare(query.sql, query.params);
  }

  stream() {
    return new SqlReadStream(this.prepareSelect());
  }

  remove() {
    const query = this._buildSqlQuery();
    if (query.groupBy.length > 0) {
      throw new Error('Invalid remove with groupBy');
    }
    if (query.limit.length > 0) {
      // Should this be allowed?
      throw new Error('Invalid remove with limit');
    }
    if (query.offset.length > 0) {
      // Should this be allowed?
      throw new Error('Invalid remove with offset');
    }
    if (query.orderBy.length > 0) {
      throw new Error('Invalid remove with orderBy');
    }

    const sql = `DELETE ${query.from} ${query.where}`;
    return this.db.run(sql, query.parameters.params);
  }

  count() {
    return Promise.try(() => {
      const query = this._buildSqlQuery();
      if (query.limit.length > 0) {
        // Should this be allowed?
        throw new Error('Invalid count with limit');
      }
      if (query.offset.length > 0) {
        // Should this be allowed?
        throw new Error('Invalid count with offset');
      }
      if (query.orderBy.length > 0) {
        throw new Error('Invalid count with orderBy');
      }

      const sql = `SELECT COUNT(*) as count FROM ${this.fromTable} ${query.where} ${query.groupBy}`;
      return this.db.get(sql, query.parameters.params)
        .then(rowCount => rowCount.count);
    });
  }

  update(newValues) {
    return Promise.try(() => {
      const query = this._buildSqlQuery();
      if (query.groupBy.length > 0) {
        throw new Error('Invalid update with groupBy');
      }
      if (query.limit.length > 0) {
        // Should this be allowed?
        throw new Error('Invalid update with limit');
      }
      if (query.offset.length > 0) {
        // Should this be allowed?
        throw new Error('Invalid update with offset');
      }
      if (query.orderBy.length > 0) {
        throw new Error('Invalid update with orderBy');
      }

      let setColumns = [];
      for (const columnName in newValues) {
        if (newValues.hasOwnProperty(columnName)) {
          let parameterName = query.parameters.addParam(newValues[columnName]);
          setColumns.push(`${columnName} = ${parameterName}`);
        }
      }

      const sql = `UPDATE ${this.fromTable} SET ${setColumns.join(', ')} ${query.where}`;
      return this.db.run(sql, query.parameters.params);
    });
  }
}

export class Table {
  constructor(db, schema) {
    if (!(db instanceof Db)) {
      throw new Error('db must be instance of Db');
    }

    this.db = db;
    this.schema = schema;
  }

  createTable() {
    return this._tableExists().then(tableExists => {
      return tableExists ? this._recreateTableIfNeeded() : this._createTable();
    });
  }

  _createTable() {
    return Promise.try(() => {
      let indices = this.schema.indices || {};

      let columnDescriptions = [];
      for (const columnName in this.schema.columns) {
        if (this.schema.columns.hasOwnProperty(columnName)) {
          const column = this.schema.columns[columnName];

          if (!_.isString(column.type)) {
            throw new Error(`Invalid sql type ${typeof column.type} for field ${columnName}`);
          }

          let primaryKey = column.primaryKey ? ' PRIMARY KEY' : '';
          let unique = column.unique && !column.primaryKey ? ' UNIQUE' : '';
          let notNull = column.notNull && !column.primaryKey ? ' NOT NULL' : '';
          let defaultValue = !_.isUndefined(column.defaultValue) && !_.isNull(column.defaultValue) ? ` DEFAULT ${column.defaultValue}` : '';

          columnDescriptions.push(`${columnName} ${column.type}${primaryKey}${unique}${notNull}${defaultValue}`);

          if (column.index) {
            // sqlite doesn't allow creation of non-unique indices while
            // creating the table, so add index to list of indices we will create
            // later.
            indices[columnName] = {columns: [columnName]};
          }
        }
      }

      if (this.schema.primaryKey) {
        const columnListing = this.schema.primaryKey.join(',');
        columnDescriptions.push(`PRIMARY KEY (${columnListing})`);
      }

      const sql = `CREATE TABLE ${this.schema.name} (\n${columnDescriptions.join(',\n')}\n);`;

      return this.db.run(sql).then(() => {
        // Create non-unique indices
        if (indices) {
          return Promise.map(Object.getOwnPropertyNames(indices), indexName => {
            let index = indices[indexName];

            let unique = index.unique ? 'UNIQUE' : '';
            let columnListing = index.columns.join(',');
            let sql = `CREATE ${unique} INDEX IF NOT EXISTS ix_${this.schema.name}_${indexName} ON ${this.schema.name} (${columnListing})`;
            return this.db.run(sql);
          });
        }
      });
    });
  }

  _tableExists() {
    let sql = `PRAGMA TABLE_INFO(${this.schema.name})`;
    return this.db.get(sql).then(field => Boolean(field));
  }

  _recreateTableIfNeeded() {
    let sql = `PRAGMA TABLE_INFO(${this.schema.name})`;
    return this.db.all(sql).then(oldColumns => {
      let namedOldColumns = _.keyBy(oldColumns, 'name');
      let columnsInBoth = _.intersection(Object.keys(this.schema.columns), Object.keys(namedOldColumns));

      // Check if there are any changes to the columns.
      let recreateTable = (columnsInBoth.length !== oldColumns.length)
          || (columnsInBoth.length !== Object.keys(this.schema.columns).length);

      if (!recreateTable) {
        // Check if the types and column parameters match
        recreateTable = columnsInBoth.map(columnName => {
          let thisColumn = this.schema.columns[columnName];
          let oldColumn = namedOldColumns[columnName];

          let thisNotNull = Boolean(thisColumn.notNull)
              && !thisColumn.primaryKey;
          let thisDefaultValue = _.isUndefined(thisColumn.defaultValue)
              ? null : thisColumn.defaultValue;

          // Note: oldcolumn.notnull is the name from the pragma query, it is
          // not camelcased and returned as an interger (0 or 1)
          let oldNotNull = Boolean(oldColumn.notnull);

          // TODO: Better type conversions for oldDefaultValue
          let oldDefaultValue = oldColumn.dflt_value;
          if (oldDefaultValue) {
            switch (oldColumn.type) {
              case 'INTEGER':
                oldDefaultValue = Number.parseInt(oldDefaultValue, 10);
                break;

              default:
                break;
            }
          }

          return ((thisColumn.type !== oldColumn.type)
                  || (thisNotNull !== oldNotNull)
                  || (thisDefaultValue !== oldDefaultValue));
        }).reduce((x, y) => x || y, false);
      }

      // Recreate the tables if required
      if (recreateTable) {
        // TODO: Better random naming
        let backupTableName = `${this.schema.name}_backup`;

        let sql = `ALTER TABLE ${this.schema.name} RENAME TO ${backupTableName}`;
        return this.db.run(sql).then(() => this.createTable())
          .then(() => {
            let joinedFiledsInBoth = columnsInBoth.join(', ');
            return this.db.run(`INSERT INTO ${this.schema.name} (${joinedFiledsInBoth}) SELECT ${joinedFiledsInBoth} FROM ${backupTableName}`);
          })
          .then(() => this.db.run(`DROP TABLE ${backupTableName}`));
      }
    });
  }

  createWriteStream(options = {}) {
    return new SqlWriteStream(options, this);
  }

  createExtendStream(options = {}) {
    return new SqlExtendStream(options, this);
  }

  insert(row) {
    let columns = '';
    let values = '';
    let parsedValues = {};
    for (const columnName in row) {
      if (columns.length > 0) {
        columns += ', ';
        values += ', ';
      }
      columns += columnName;
      values += `$${columnName}`;
      parsedValues[`$${columnName}`] = row[columnName];
    }

    const sql = `INSERT INTO ${this.schema.name} (${columns}) VALUES(${values});`;
    return this.db.run(sql, parsedValues);
  }

  find(queryColumns) {
    var query = new SqlQuery(this.db);
    query.from(this.schema.name);

    if (queryColumns) {
      query.find(queryColumns);
    }
    return query;
  }

  insertUpdateUnique(row) {
    let updateOldRow = oldRow => {
      if (!oldRow) {
        // Insert new columns
        return this.insert(row).then(sqlStatement => {
          let res = {
            ...row,
            id: sqlStatement.lastID
          };

          let primaryKeyName = this._extractPrimaryKey();
          if (primaryKeyName) {
            res[primaryKeyName] = sqlStatement.lastID;
          }
          return res;
        });
      }

      return this.find({id: oldRow.id}).update(row).then(() => {
        return {
          ...oldRow,
          ...row
        };
      });
    };

    // Check if we have the result
    let uniqueColumns = this._extractUniqueColumns(row);
    return this.find(uniqueColumns).get()
      .then(updateOldRow)
      .catch(err => {
        // If we get 'UNIQUE constraint failed' then the row might have been
        // inserted between when we queried for it and when we tried to insert
        // it.
        if (err.message.indexOf('UNIQUE constraint failed') >= 0) {
          return this.find(uniqueColumns).get().then(updateOldRow);
        }
        throw err;
      });
  }

  updateUnique(row) {
    return this.find(this._extractUniqueColumns(row)).update(row);
  }

  _extractUniqueColumns(row) {
    let uniqueColumns = {};
    for (const columnName in row) {
      if (row.hasOwnProperty(columnName)) {
        const schemaRow = this.schema.columns[columnName];
        if (!schemaRow) {
          throw new Error(`Column ${columnName} is not part of schema`);
        }
        if (schemaRow.unique || schemaRow.primaryKey) {
          // We can use this for looking up the element
          uniqueColumns[columnName] = row[columnName];
          break;
        }
      }
    }

    if (Object.keys(uniqueColumns).length === 0) {
      // TODO: Check indices with multiple fields
      throw new Error('No unique columns for identifying the row');
    }

    return uniqueColumns;
  }

  _extractPrimaryKey() {
    for (const columnName in this.schema.columns) {
      if (this.schema.columns.hasOwnProperty(columnName)) {
        const schemaRow = this.schema.columns[columnName];
        if (!schemaRow) {
          throw new Error(`Column ${columnName} is not part of schema`);
        }
        if (schemaRow.primaryKey) {
          return schemaRow.primaryKey;
        }
      }
    }
  }
}

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


export function buildQuery(db) {
  return new SqlQuery(db);
}
