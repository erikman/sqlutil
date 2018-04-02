import Promise from 'bluebird';
import _ from 'lodash';
import assert from 'assert';

import {SqlReadStream} from './sqlreadstream';
import {query} from './query';

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


class BuildQuery {
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

  /**
   * @brief Order result set by specified columns
   *
   * The syntax is:
   * orderBy([{field: criteria}, ...]);
   * where criteria can be asc, desc, ascending, descending, 1 or -1.
   * orderBy(['field']) is shorthand for orderBy([{'field': 1}])
   *
   * Example:
   * @code
   * query.orderBy(['key1', 'key2']);
   * query.orderBy([{key1: 1}, {key2: -1}]); // key1 ASC, key2 DESC
   * @endcode
   */
  orderBy(columns) {
    assert(_.isArray(columns));

    let parsedColumns = columns.map(column => {
      if (_.isString(column)) {
        return column;
      }
      else if (_.isObject(column)) {
        // There should only be one key in the column, and the value should be
        // +1 or -1 to indicate ascending or descending order respectively.
        let keys = Object.keys(column);
        if (keys.length !== 1) {
          throw new Error(`Invalid orderBy for column ${JSON.stringify(column)}`);
        }

        let direction;
        switch (column[keys[0]]) {
          case 'asc':
          case 'ascending':
          case 1:
            direction = 'ASC';
            break;

          case 'desc':
          case 'descending':
          case -1:
            direction = 'DESC';
            break;

          default:
            throw new Error(`Invalid orderBy direction: ${column[keys[0]]}`);
        }

        return `${keys[0]} ${direction}`;
      }

      throw new Error(`Invalid orderBy for column ${JSON.stringify(column)}`);
    });

    this.orderByColumns = this.orderByColumns.concat(parsedColumns);
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
        if (_.has(LOGICAL_OPERATORS, key)) {
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
        if (_.has(BINARY_OPERATORS, key)) {
          let op = BINARY_OPERATORS[key];
          if (_.has(rest, key)) {
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
      parameters
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
    const rawQuery = this._buildSelectQuery();
    return query(this.db, rawQuery.sql, rawQuery.params).all();
  }

  get() {
    const rawQuery = this._buildSelectQuery();
    return query(this.db, rawQuery.sql, rawQuery.params).get();
  }

  each(callback) {
    const rawQuery = this._buildSelectQuery();
    return query(this.db, rawQuery.sql, rawQuery.params).each(callback);
  }

  prepareSelect() {
    const rawQuery = this._buildSelectQuery();
    return query(this.db, rawQuery.sql, rawQuery.params).prepare();
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
        if (_.has(newValues, columnName)) {
          let parameterName = query.parameters.addParam(newValues[columnName]);
          setColumns.push(`${columnName} = ${parameterName}`);
        }
      }

      const sql = `UPDATE ${this.fromTable} SET ${setColumns.join(', ')} ${query.where}`;
      return this.db.run(sql, query.parameters.params);
    });
  }
}

/**
 * @deprecated Use #buildQuery function instead
 */
export const SqlQuery = BuildQuery;

export function buildQuery(db) {
  return new SqlQuery(db);
}
