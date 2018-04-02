import sqlite3 from 'sqlite3';
import stream from 'stream';
import Promise from 'bluebird';
import _ from 'lodash';
import assert from 'assert';

import {promisifyStatement} from './statement';

promisifyStatement();

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
