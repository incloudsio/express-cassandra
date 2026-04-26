const debug = require('debug')('express-cassandra');

// Clients tagged with `__kind === 'opensearch'` are @opensearch-project/opensearch.
// That client's API is promise-based (no callbacks) and does not accept the
// deprecated per-document `type` parameter on putMapping.
//
// The legacy `elasticsearch` npm package uses `__kind === 'legacy'`. Its API is
// node-style callbacks and still accepts `type` on putMapping.
const isPromiseClient = client => !!client && client.__kind === 'opensearch';

// The opensearch-js client wraps every response in `{ body, statusCode, ... }`.
// Unwrap so downstream consumers keep seeing the raw payload they got from the
// legacy client.
const unwrap = res => {
  if (res && typeof res === 'object' && 'body' in res) return res.body;
  return res;
};

// Uniform invoker for either client shape. `fn` is the indices.* method;
// `thisArg` is the namespace it is bound to (e.g. client.indices).
const invoke = (client, thisArg, fn, params, callback) => {
  if (isPromiseClient(client)) {
    try {
      const p = fn.call(thisArg, params);
      if (p && typeof p.then === 'function') {
        p.then(res => callback(null, unwrap(res)), err => callback(err));
        return;
      }
      callback(null, unwrap(p));
    } catch (err) {
      callback(err);
    }
  } else {
    fn.call(thisArg, params, (err, res) => {
      if (err) {
        callback(err);
        return;
      }
      callback(null, unwrap(res));
    });
  }
};

// Elassandra table scoping still relies on typed mappings (`/<index>/_mapping/<table>`).
// The opensearch-js helper only exposes typeless putMapping(), so use transport
// directly when talking to Elassandra.
const invokeTransport = (client, params, callback) => {
  if (!client || !client.transport || typeof client.transport.request !== 'function') {
    callback(new Error('OpenSearch transport client unavailable'));
    return;
  }
  try {
    const p = client.transport.request(params);
    if (p && typeof p.then === 'function') {
      p.then(res => callback(null, unwrap(res)), err => callback(err));
      return;
    }
    callback(null, unwrap(p));
  } catch (err) {
    callback(err);
  }
};

const ElassandraBuilder = function f(client) {
  this._client = client;
};

ElassandraBuilder.prototype = {
  // `tableName` is optional for backward-compatibility with callers that create
  // keyspace-level indices. When provided, it is written as `index.table` in the
  // settings so Elassandra can bind a typeless OpenSearch mapping (default type
  // `_doc`) to the actual CQL table. Without it, Elassandra's IndexMetadata.table()
  // falls back to `_doc`, and CQL writes to the real table never reach ES.
  create_index(keyspaceName, indexName, tableName, callback) {
    if (typeof tableName === 'function') {
      callback = tableName;
      tableName = null;
    }
    debug('creating elassandra index: %s (keyspace=%s, table=%s)', indexName, keyspaceName, tableName || '<none>');
    const client = this._client;
    const settings = { keyspace: keyspaceName };
    if (tableName) {
      settings.table = tableName;
    }
    invoke(client, client.indices, client.indices.create, {
      index: indexName,
      body: { settings }
    }, err => {
      if (err) {
        callback(err);
        return;
      }
      callback();
    });
  },

  check_index_exist(indexName, callback) {
    debug('check for elassandra index: %s', indexName);
    const client = this._client;
    invoke(client, client.indices, client.indices.exists, { index: indexName }, (err, res) => {
      if (err) {
        callback(err);
        return;
      }
      // legacy returns a boolean directly; opensearch returns a boolean body.
      callback(null, !!res);
    });
  },

  assert_index(keyspaceName, indexName, tableName, callback) {
    if (typeof tableName === 'function') {
      callback = tableName;
      tableName = null;
    }
    this.check_index_exist(indexName, (err, exist) => {
      if (err) {
        callback(err);
        return;
      }
      if (!exist) {
        this.create_index(keyspaceName, indexName, tableName, callback);
        return;
      }
      callback();
    });
  },

  delete_index(indexName, callback) {
    debug('removing elassandra index: %s', indexName);
    const client = this._client;
    invoke(client, client.indices, client.indices.delete, { index: indexName }, err => {
      if (err) {
        callback(err);
        return;
      }
      callback();
    });
  },

  put_mapping(indexName, mappingName, mappingBody, callback) {
    debug('syncing elassandra mapping: %s', mappingName);
    const client = this._client;
    const params = {
      index: indexName,
      body: mappingBody
    };
    if (isPromiseClient(client)) {
      const typedPath = `/${encodeURIComponent(indexName)}/_mapping/${encodeURIComponent(mappingName)}`;
      invokeTransport(client, {
        method: 'PUT',
        path: typedPath,
        querystring: { include_type_name: 'true' },
        body: mappingBody
      }, typedErr => {
        if (!typedErr) {
          callback();
          return;
        }
        debug('typed mapping update failed for %s/%s, falling back to typeless putMapping: %s', indexName, mappingName, typedErr && typedErr.message ? typedErr.message : typedErr);
        invoke(client, client.indices, client.indices.putMapping, params, err => {
          if (err) {
            callback(err);
            return;
          }
          callback();
        });
      });
      return;
    }
    // Legacy ES/Elassandra convention: one "type" per table.
    params.type = mappingName;
    invoke(client, client.indices, client.indices.putMapping, params, err => {
      if (err) {
        callback(err);
        return;
      }
      callback();
    });
  }
};

module.exports = ElassandraBuilder;