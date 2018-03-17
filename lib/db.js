import sqlite3 from 'sqlite3';
import Promise from 'bluebird';
import _ from 'lodash';

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

  close() {
    return Promise.fromCallback(cb => this.db.close(cb));
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

  exec(sql) {
    const self = this;
    return new Promise((resolve, reject) => {
      self.db.exec(sql, function (err, ...results) {
        if (err) {
          reject(new Error(`${err.message}, when running ${sql}`));
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

  enableForeignKeys(enable) {
    if (enable === undefined) {
      enable = 'ON';
    }
    else if (!enable) {
      enable = 'OFF';
    }

    return this.run(`PRAGMA foreign_keys = ${enable}`);
  }

  isForeignKeysEnabled() {
    return this.get('PRAGMA foreign_keys')
      .then(foreignKeysEnabled => Boolean(foreignKeysEnabled.foreign_keys));
  }
}
