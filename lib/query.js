import _ from 'lodash';

import {SqlReadStream} from './sqlreadstream';

class Query {
  constructor(db, sql, params) {
    this.db = db;
    this.sql = sql;
    this.params = params;
  }

  all(params) {
    return this.db.all(this.sql, params || this.params);
  }

  get(params) {
    return this.db.get(this.sql, params || this.params);
  }

  each(params, callback) {
    if (_.isFunction(params)) {
      callback = params;
      params = undefined;
    }

    return this.db.each(this.sql, params || this.params, callback);
  }

  prepare(params) {
    return this.db.prepare(this.sql, params || this.params);
  }

  stream(params) {
    return new SqlReadStream(this.prepareSelect(params || this.params));
  }
}

export function query(db, sql, params) {
  return new Query(db, sql, params);
}
