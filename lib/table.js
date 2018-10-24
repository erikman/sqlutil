import Promise from 'bluebird';
import _ from 'lodash';
import assert from 'assert';

import {Db} from './db';
import {SqlQuery} from './sqlquery';
import {SqlExtendStream} from './sqlextendstream';
import {SqlWriteStream} from './sqlwritestream';

export const DataType = {
  INTEGER: 'INTEGER',
  BOOLEAN: 'INTEGER',
  DATE: 'INTEGER',
  NUMERIC: 'NUMERIC',
  TEXT: 'TEXT',
  FLOAT: 'REAL',
  REAL: 'REAL',
  BLOB: 'BLOB'
};

export class Table {
  constructor(db, schema) {
    if (!(db instanceof Db)) {
      throw new Error('db must be instance of Db');
    }

    this.db = db;
    this.schema = schema;
  }

  // Create table
  createTable() {
    return Promise.try(() => {
      let sql = `${this._createTableSql()}`;
      return this.db.run(sql);
    });
  }

  createTableIfNotExists() {
    return this.tableExists().then(exists => {
      if (exists) {
        return this.schemaMatchesDatabase().then(match => {
          if (!match) {
            throw new Error(`Table ${this.schema.name} already exists and don't match database`);
          }
          return {wasCreated: false};
        });
      }

      return this.createTable()
        .then(() => {
          return {wasCreated: true};
        });
    });
  }

  createOrUpdateTable() {
    return Promise.join(this.tableExists(), this.db.isForeignKeysEnabled(), (exists, foreignKeysEnabled) => {
      if (foreignKeysEnabled) {
        throw new Error('Foreign keys must be disabled when using createOrUpdateTable');
      }

      if (exists) {
        return this.getSchemaFromDatabase().then(oldSchema => {
          if (!this._schemaMatchesDatabase(oldSchema)) {
            let backupTableName = `backup_${this.schema.name}_${Date.now()}`;
            let fieldsInBoth = _.intersection(Object.keys(oldSchema.columns), Object.keys(this.schema.columns));
            let joinedFiledsInBoth = fieldsInBoth.join(', ');

            let sql = `BEGIN TRANSACTION;\n`
              + `ALTER TABLE ${this.schema.name} RENAME TO ${backupTableName};\n`
              + `${this._createTableSql()}\n`
              + `INSERT INTO ${this.schema.name} (${joinedFiledsInBoth}) SELECT ${joinedFiledsInBoth} FROM ${backupTableName};\n`
              + `DROP TABLE ${backupTableName};\n`
              + `COMMIT;\n`;
            return this.db.exec(sql)
              .then(() => {
                return {wasCreated: false, wasUpdated: true};
              });
          }

          return {wasCreated: false, wasUpdated: false};
        });
      }

      return this.createTable()
        .then(() => {
          return {wasCreated: true};
        });
    });
  }

  _createTableSql() {
    let indices = this.schema.indices || {};

    let columnDescriptions = [];
    for (const columnName in this.schema.columns) {
      if (_.has(this.schema.columns, columnName)) {
        const column = this.schema.columns[columnName];

        if (!_.isString(column.type)) {
          throw new Error(`Invalid sql type ${typeof column.type} for field ${columnName}`);
        }

        let primaryKey = column.primaryKey ? ' PRIMARY KEY' : '';
        let unique = column.unique && !column.primaryKey ? ' UNIQUE' : '';
        let notNull = column.notNull && !column.primaryKey ? ' NOT NULL' : '';
        let collate = column.collate ? ` COLLATE ${column.collate}` : '';
        let defaultValue = '';
        if (!_.isUndefined(column.defaultValue) && !_.isNull(column.defaultValue)) {
          if (_.isNumber(column.defaultValue)) {
            defaultValue = ` DEFAULT ${column.defaultValue}`;
          }
          else if (_.isString(column.defaultValue)) {
            defaultValue = ` DEFAULT "${column.defaultValue}"`;
          }
          else {
            throw new Error(`Invalid type for default value for column ${columnName}`);
          }
        }

        columnDescriptions.push(`${columnName} ${column.type}${primaryKey}${unique}${notNull}${defaultValue}${collate}`);

        if (column.index) {
          // Sqlite doesn't allow creation of non-unique indices while
          // creating the table, so add index to list of indices we will create
          // later.
          indices[columnName] = {columns: [`${columnName}${collate}`]};
        }
      }
    }

    if (this.schema.foreignKeys) {
      this.schema.foreignKeys.forEach(foreignKey => {
        let referencesTables = Object.keys(foreignKey.references);
        if (referencesTables.length !== 1) {
          throw new Error('Invalid foreign key description');
        }
        let referencesTable = referencesTables[0];
        let referencesColumns = foreignKey.references[referencesTable];
        if (!_.isArray(referencesColumns)) {
          referencesColumns = [referencesColumns];
        }

        let fromColumns = _.isArray(foreignKey.from)
          ? foreignKey.from : [foreignKey.from];

        columnDescriptions.push(`FOREIGN KEY(${fromColumns.join(', ')}) REFERENCES ${referencesTable}(${referencesColumns.join(', ')})`);
      });
    }

    if (this.schema.primaryKey) {
      const columnListing = this.schema.primaryKey.join(',');
      columnDescriptions.push(`PRIMARY KEY (${columnListing})`);
    }

    let sql = [
      `CREATE TABLE ${this.schema.name} (\n${columnDescriptions.join(',\n')}\n);`
    ];

    if (indices) {
      sql = sql.concat(Object.getOwnPropertyNames(indices).map(indexName => {
        let index = indices[indexName];
        index.columns.forEach(columnName => {
          if (!_.has(this.schema.columns, columnName)) {
            throw new Error(`Unknown field ${columnName} for index ${indexName};`);
          }
        });

        let unique = index.unique ? 'UNIQUE ' : '';
        let columnListing = index.columns.join(',');
        return `CREATE ${unique}INDEX IF NOT EXISTS ix_${this.schema.name}_${indexName} ON ${this.schema.name} (${columnListing});`;
      }));
    }

    return sql.join('\n');
  }

  tableExists() {
    let sql = `PRAGMA TABLE_INFO(${this.schema.name})`;
    return this.db.get(sql).then(field => Boolean(field));
  }

  getSchemaFromDatabase() {
    let columnsPromise = this.db.all(`PRAGMA TABLE_INFO(${this.schema.name})`)
      .then(sqlColumns => {
        let namedColumns = _.keyBy(sqlColumns, 'name');
        let columns = {};
        _.forEach(namedColumns, (column, name) => {
          // Note: oldcolumn.notnull is the name from the pragma query, it is
          // not camelcased and returned as an interger (0 or 1)
          let notNull = Boolean(column.notnull);

          let type;
          switch (column.type) {
            case 'INTEGER':
              type = DataType.INTEGER;
              break;

            case 'NUMERIC':
              type = DataType.NUMERIC;
              break;

            case 'TEXT':
              type = DataType.TEXT;
              break;

            case 'REAL':
              type = DataType.FLOAT;
              break;

            default:
              type = 'BLOB';
          }

          // TODO: Better type conversions for defaultValue
          let defaultValue = column.dflt_value;
          if (defaultValue && _.isString(defaultValue)) {
            // We need to parse the default value.
            if (defaultValue[0] === '"' && defaultValue[defaultValue.length - 1] === '"') {
              defaultValue = defaultValue.substring(1, defaultValue.length - 1);
            }
            else {
              defaultValue = Number.parseFloat(defaultValue);
            }
          }

          columns[name] = {
            type,
            notNull,
            defaultValue
          };
        });

        return columns;
      });

    let foreignKeysPromise
        = this.db.all(`PRAGMA foreign_key_list(${this.schema.name})`)
          .then(foreignKeys => {
            return foreignKeys.map(foreignKey => {
              return {
                from: foreignKey.from,
                references: {[foreignKey.table]: foreignKey.to}
              };
            });
          });

    return Promise.join(columnsPromise, foreignKeysPromise, (columns, foreignKeys) => {
      return {
        name: this.schema.name,
        columns,
        foreignKeys
      };
    });
  }

  _schemaMatchesDatabase(oldSchema) {
    function columnIsNotNull(column) {
      return Boolean(column.notNull) && !column.primaryKey;
    }
    function columnDefaultValue(column) {
      let defaultValue = null;
      if (_.isString(column.defaultValue)) {
        defaultValue = `"${column.defaultValue}"`;
      }
      else if (!_.isUndefined(column.defaultValue)) {
        defaultValue = column.defaultValue;
      }
      return defaultValue;
    }
    function foreignKeyReferences(foreignKey) {
      if (!foreignKey) {
        return undefined;
      }

      let oldReferencesKeys = Object.keys(foreignKey.references);
      assert(oldReferencesKeys.length === 1);

      let table = oldReferencesKeys[0];
      let field = foreignKey.references[table];
      return {table, field};
    }

    let columnsInBoth = _.intersection(Object.keys(this.schema.columns), Object.keys(oldSchema.columns));

    // Check if there are any changes to the columns.
    let schemasAreEqual = (columnsInBoth.length === Object.keys(oldSchema.columns).length)
        && (columnsInBoth.length === Object.keys(this.schema.columns).length);
    if (schemasAreEqual) {
      // Check if the types and column parameters match
      schemasAreEqual = columnsInBoth.map(columnName => {
        let thisColumn = this.schema.columns[columnName];
        let oldColumn = oldSchema.columns[columnName];

        return ((thisColumn.type === oldColumn.type)
                && columnIsNotNull(thisColumn) === columnIsNotNull(oldColumn)
                && columnDefaultValue(thisColumn) === columnDefaultValue(oldColumn));
      }).reduce((x, y) => x && y, true);
    }

    // Check if there are any changes to the foreign keys
    if (schemasAreEqual) {
      let oldForeignKeys = oldSchema.foreignKeys || [];
      let foreignKeys = this.schema.foreignKeys || [];
      schemasAreEqual = (oldForeignKeys.length === foreignKeys.length);

      // Check if the foreign key from/references have changed
      for (let i = 0; i < oldForeignKeys.length; i++) {
        let oldForeignKey = oldForeignKeys[i];
        let newForeignKey = _.find(foreignKeys, foreignKey => {
          return foreignKey.from === oldForeignKey.from;
        });

        let oldReferences = foreignKeyReferences(oldForeignKey);
        let newReferences = foreignKeyReferences(newForeignKey);

        schemasAreEqual = schemasAreEqual && (newForeignKey !== null)
          && (oldReferences.table === newReferences.table)
          && (oldReferences.field === newReferences.field);
      }
    }

    return schemasAreEqual;
  }

  /** Check if the schema matches the structure in the database */
  schemaMatchesDatabase() {
    return this.getSchemaFromDatabase()
      .then(oldSchema => this._schemaMatchesDatabase(oldSchema));
  }

  createWriteStream(options = {}) {
    return new SqlWriteStream(options, this.db, this.schema.name);
  }

  createExtendStream(options = {}) {
    return new SqlExtendStream(options, this);
  }

  insert(row) {
    let columns = '';
    let values = '';
    let parsedValues = {};
    for (const columnName in row) {
      if (Object.prototype.hasOwnProperty.call(row, columnName)) {
        if (columns.length > 0) {
          columns += ', ';
          values += ', ';
        }
        columns += columnName;
        values += `$${columnName}`;
        parsedValues[`$${columnName}`] = row[columnName];
      }
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
      if (_.has(row, columnName)) {
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
      if (_.has(this.schema.columns, columnName)) {
        const schemaRow = this.schema.columns[columnName];
        if (!schemaRow) {
          throw new Error(`Column ${columnName} is not part of schema`);
        }
        if (schemaRow.primaryKey) {
          return columnName;
        }
      }
    }
  }
}
