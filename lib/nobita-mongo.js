const mongoose = require('mongoose');
const merge = require('lodash/merge');
let db = {};

module.exports = app => {
  const { mongo = {} } = app.config;
  let _db = {};
  if (mongo.clients) {
    for (let database in mongo.clients) {
      _db[database] = bind(mongo.clients[database]);
      _db[database]._add = (databaseName, tableName, schema) => {
        app.context.db[tableName] = create(databaseName, tableName, schema)[tableName];
      };
    }
  } else {
    _db = bind(mongo);
    _db._add = (databaseName, tableName, schema) => {
      app.context.db[tableName] = create(databaseName, tableName, schema)[tableName];
    };
  }

  app.context.db = _db;
  app.db = _db;
}

const bind = config => {
  let _f = {}
  const arr = config.url.split('/');
  const databaseName = arr[arr.length - 1];
  config.option = merge({
    useNewUrlParser: true,
    useCreateIndex: true
  }, config.option)
  db[databaseName] = mongoose.createConnection(config.url, config.option);
  // 连接失败
  db[databaseName].on("error", (err) => {
    console.error(`${databaseName}:数据库链接失败:` + err);
  });
  // 连接成功
  db[databaseName].on("open", () => {
    console.log(`${databaseName}:数据库链接成功`);
  });
  // 断开数据库
  db[databaseName].on("disconnected", () => {
    console.log(`${databaseName}:数据库断开`);
  });

  for (let table in config.tables) {
    _f = merge(_f, create(databaseName, table, config.tables[table]));
  }
  return _f;
}

const create = (databaseName, tableName, schema) => {
  const model = [];
  let _f = {};
  schema = Object.assign(schema, {
    createTime: {
      type: Date,
      default: Date.now,
      required: true
    },

    updateTime: {
      type: Date,
      default: Date.now,
      required: true
    }
  });

  let Schema = new mongoose.Schema(schema);
  model[tableName] = db[databaseName].model(tableName, Schema, tableName);

  _f[tableName] = {
    async find(sql, data = {}) {
      data.page = +(data.page || 0);
      data.limit = +(data.limit || 20);
      data.skip = data.page * data.limit || 0;
      try {
        const total = await model[tableName].countDocuments(sql);
        const res = await model[tableName].find(sql, data.filter, data);
        return {
          code: 200,
          data: {
            list: res,
            pages: {
              total,
              page: data.page,
              length: res.length
            }
          },
          msg: 'success'
        };
      } catch (error) {
        return {
          code: 201,
          data: null,
          msg: `fail: ${error}`
        }
      }
    },

    async findOne(sql, data = {}) {
      try {
        const res = await model[tableName].findOne(sql, null, data);
        return {
          code: 200,
          data: res.toObject(),
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: null,
          msg: `fail: ${error}`
        }
      }
    },

    async insertMany(sql) {
      for (let i in sql) {
        sql[i].createTime = sql[i].updateTime = +new Date();
      }

      try {
        const res = await model[tableName].insertMany(sql);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: null,
          msg: `fail: ${error}`
        }
      }
    },

    async insert(sql) {
      sql.createTime = sql.updateTime = +new Date();
      try {
        const res = await model[tableName].insertMany(sql);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: null,
          msg: `fail: ${error}`
        }
      }
    },

    async update(sql, newDate, params = {}) {
      newDate.updateTime = +new Date();
      if (params.upsert) {
        params.setDefaultsOnInsert = true;
      }

      try {
        const res = await model[tableName].updateMany(sql, newDate, params);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: null,
          msg: `fail: ${error}`
        }
      }

    },

    async updateOne(sql, newDate, params = {}) {
      newDate.updateTime = +new Date();
      if (params.upsert) {
        params.setDefaultsOnInsert = true;
      }

      try {
        const res = await model[tableName].updateOne(sql, newDate, params);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: null,
          msg: `fail: ${error}`
        }
      }
    },

    async remove(sql) {
      try {
        const res = await model[tableName].remove(sql);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: null,
          msg: `fail: ${error}`
        }
      }
    },

    async removeOne() {
      try {
        const res = await model[tableName].findOneAndRemove(sql);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: null,
          msg: `fail: ${error}`
        }
      }
    },

    async aggregate(sql) {
      try {
        const res = await model[tableName].aggregate(sql);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: null,
          msg: `fail: ${error}`
        }
      }
    }
  };
  return _f;
}