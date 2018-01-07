import chai from 'chai';
import sqlite3 from 'sqlite3';

import stream from 'stream';
import * as sqlutil from '../lib';

let assert = chai.assert;

const TEST_ROW_COUNT = 50000;

describe('sqlite3', () => {
  let db;

  it('can create db in memory', done => {
    db = new sqlite3.Database(':memory:', done);
  });

  it('can create table with random data', done => {
    db.run('CREATE TABLE testtable (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL UNIQUE, value FLOAT)', err => {
      if (err) {
        done(err);
        return;
      }

      db.serialize(() => {
        db.run('BEGIN TRANSACTION');

        let stmt = db.prepare('INSERT INTO testtable (name, value) VALUES ($name, $value)');

        for (let i = 0; i < TEST_ROW_COUNT; i++) {
          stmt.run({
            $name: `name-${i}`,
            $value: Math.random()
          });
        }

        db.run('END TRANSACTION', done);
      });
    });
  });

  it('can create table with random data using streams', done => {
    class RandomData extends stream.Readable {
      constructor() {
        super({
          objectMode: true
        });

        this.id = 0;
      }

      _read() {
        try {
          while (this.id < TEST_ROW_COUNT) {
            let row = {
              name: `name-${this.id}`,
              value: Math.random()
            };
            this.id++;

            if (!this.push(row)) {
              return;
            }
          }

          this.push(null);
        }
        catch (err) {
          process.nextTick(() => this.emit('error', err));
        }
      }
    }

    let table = new sqlutil.Table(new sqlutil.Db(db), {
      name: 'testtable2',
      columns: {
        id: {type: 'INTEGER', primaryKey: true},
        name: {type: 'TEXT', unique: true},
        value: {type: 'REAL'}
      }
    });

    table.createTable().then(() => {
      let source = new RandomData();
      let writeStream = source.pipe(table.createWriteStream());

      writeStream.on('finish', done);
    });
  });

  it('can return data from table with db#all', done => {
    db.all('SELECT * from testtable', (err, rows) => {
      if (err) {
        done(err);
        return;
      }
      assert.equal(rows.length, TEST_ROW_COUNT);
      done();
    });
  });

  it('can return data from table with db#each', done => {
    let rowCount = 0;
    db.each('SELECT * from testtable', (/* row */) => {
      rowCount++;
    }, err => {
      if (err) {
        done(err);
        return;
      }
      assert.equal(rowCount, TEST_ROW_COUNT);
      done();
    });
  });

  it('can return data from table with statement#all', done => {
    let stmt;

    let cleanupAndDone = err => {
      stmt.finalize(done.bind(null, err));
    };

    stmt = db.prepare('SELECT * from testtable', err => {
      if (err) {
        cleanupAndDone(err);
        return;
      }

      stmt.all((err, rows) => {
        if (err) {
          done(err);
          return;
        }
        assert.equal(rows.length, TEST_ROW_COUNT);
        cleanupAndDone();
      });
    });
  });

  it('can return data from table with statement#each', done => {
    let stmt;

    let cleanupAndDone = err => {
      stmt.finalize(done.bind(null, err));
    };

    let rowCount = 0;
    stmt = db.prepare('SELECT * from testtable', err => {
      if (err) {
        cleanupAndDone(err);
        return;
      }

      stmt.each((/* row */) => {
        rowCount++;
      }, err => {
        if (err) {
          cleanupAndDone(err);
          return;
        }
        assert.equal(rowCount, TEST_ROW_COUNT);
        cleanupAndDone();
      });
    });
  });


  it('can return data from table with statement#get', done => {
    let stmt;

    let cleanupAndDone = err => {
      stmt.finalize(done.bind(null, err));
    };

    stmt = db.prepare('SELECT * from testtable', err => {
      if (err) {
        cleanupAndDone(err);
        return;
      }

      // Setup recursion
      let rowCount = 0;
      let recursiveGet;
      recursiveGet = (err, row) => {
        if (err) {
          cleanupAndDone(err);
          return;
        }

        if (!row) {
          assert.equal(rowCount, TEST_ROW_COUNT);
          cleanupAndDone();
          return;
        }
        rowCount++;

        stmt.get(recursiveGet);
      };

      // Start recursion
      stmt.get(recursiveGet);
    });
  });

  it('can implement each with statement#get', done => {
    /* eslint max-params: "off" */
    function asyncEach(db, sql, parameters, eachCb, doneCb) {
      let stmt;

      let cleanupAndDone = err => {
        stmt.finalize(doneCb.bind(null, err));
      };

      stmt = db.prepare(sql, parameters, err => {
        if (err) {
          return cleanupAndDone(err);
        }

        let recursiveGet;
        let next = err => {
          if (err) {
            return cleanupAndDone(err);
          }

          return stmt.get(recursiveGet);
        };

        // Setup recursion
        recursiveGet = (err, row) => {
          if (err) {
            return cleanupAndDone(err);
          }

          if (!row) {
            return cleanupAndDone(null);
          }

          // Call the each callback, get next entry when next is invoked
          return eachCb(row, next);
        };

        // Start recursion
        stmt.get(recursiveGet);
      });
    }

    let rowCount = 0;
    asyncEach(db, 'SELECT * from testtable', [], (row, next) => {
      assert.isObject(row);
      rowCount++;
      return next();
    }, err => {
      if (err) {
        return done(err);
      }
      assert.equal(rowCount, TEST_ROW_COUNT);
      done();
    });
  });

  it('can return data from table with statement#getMultiple', done => {
    let stmt;
    const CHUNK_SIZE = 50;

    let cleanupAndDone = err => {
      stmt.finalize(done.bind(null, err));
    };

    stmt = db.prepare('SELECT * from testtable', err => {
      if (err) {
        cleanupAndDone(err);
        return;
      }

      // Setup recursion
      let rowCount = 0;
      let recursiveGet = (err, rows) => {
        if (err) {
          cleanupAndDone(err);
          return;
        }

        rowCount += rows.length;
        if (rows.length < CHUNK_SIZE) {
          assert.equal(rowCount, TEST_ROW_COUNT);
          cleanupAndDone();
          return;
        }

        stmt.getMultiple(CHUNK_SIZE, recursiveGet);
      };

      // Start recursion
      stmt.getMultiple(CHUNK_SIZE, recursiveGet);
    });
  });

  it('can stream data with sqlstream', done => {
    let stmt;
    stmt = db.prepare('SELECT * from testtable', err => {
      if (err) {
        return done(err);
      }

      let stream = new sqlutil.SqlReadStream(stmt);

      let rowCount = 0;
      stream.on('data', () => {
        rowCount++;
      });

      stream.on('end', () => {
        assert.equal(rowCount, TEST_ROW_COUNT);
        done();
      });

      stream.on('error', done);
    });
  });
});
