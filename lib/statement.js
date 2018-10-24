import sqlite3 from 'sqlite3';
import Promise from 'bluebird';

import {SqlReadStream} from './sqlreadstream';

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

export function promisifyStatement() {
  promisifyFunctions(sqlite3.Statement, [
    'all', 'bind', 'get', 'getMultiple'
  ]);
}

promisifyStatement();

export class Statement {
  constructor(db, statement) {
    this.statement = statement;
  }

  all(...params) {
    return this.statement.allAsync(...params);
  }

  /**
   * It is important that we don't send any param values, even undefined, if not
   * used, as this would restart the statement.
   */
  get(...params) {
    return this.statement.getAsync(...params);
  }

  each(...params) {
    return this.statement.eachAsync(...params);
  }

  stream() {
    return new SqlReadStream(this.statement);
  }
}

export function statement(db, sql, params) {
  return db.prepare(sql, params).then(statement => {
    return new Statement(db, statement);
  });
}
