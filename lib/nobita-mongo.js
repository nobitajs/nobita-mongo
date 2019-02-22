const mongoose = require('mongoose');
const _ = require('lodash');
let db = {};

module.exports = app => {
  const { mongo = {} } = app.config;
  let _db = {};

  if (mongo.clients) {
    for (let database in mongo.clients) {
      _db[database] = bind(mongo.clients[database]);
      _db[database]._add = (databaseName, table, tables) => {
        app.context.db[table] = creat(databaseName, table, tables)[table];
      };
    }
  } else {
    _db = bind(mongo);
    _db._add = (databaseName, table, tables) => {
      app.context.db[table] = creat(databaseName, table, tables)[table];
    };
  }

  app.context.db = _db;
  app.db = _db;
}

const bind = config => {
  let _f = {}
  const arr = config.url.split('/');
  const databaseName = arr[arr.length - 1];
  config.option = _.merge({
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
    _f = _.merge(_f, creat(databaseName, table, config.tables[table]));
  }
  return _f;
}

const creat = (databaseName, table, tables) => {
  const model = [];
  let _f = {};
  tables = Object.assign(tables, {
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

  let Schema = new mongoose.Schema(tables);
  model[table] = db[databaseName].model(table, Schema, table);

  _f[table] = {
    async find(sql, data = {}) {
      data.page = +(data.page || 0);
      data.limit = +(data.limit || 20);
      data.skip = data.page * data.limit || 0;
      try {
        const total = await model[table].countDocuments(sql);
        const res = await model[table].find(sql, null, data);
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
          data: {},
          msg: `fail: ${error}`
        }
      }
    },

    async findOne(sql, data = {}) {
      try {
        const res = await model[table].findOne(sql, null, data);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: {},
          msg: `fail: ${error}`
        }
      }
    },

    async insertMany(sql) {
      for (let i in sql) {
        sql[i] = _.merge(sql[i], {
          updateTime: (+new Date()),
          createTime: (+new Date())
        })
      }

      try {
        const res = await model[table].insertMany(sql);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: {},
          msg: `fail: ${error}`
        }
      }
    },

    async insert(sql) {
      sql = _.merge(sql, {
        updateTime: (+new Date()),
        createTime: (+new Date())
      })
      try {
        const res = await model[table].insertMany(sql);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: {},
          msg: `fail: ${error}`
        }
      }
    },

    async update(sql, newDate, params) {
      const data = await model[table].find(sql);
      newDate = _.merge(newDate, { updateTime: (+new Date()) });
      if (!data.length) {
        newDate = _.merge(newDate, { createTime: (+new Date()) });
      }

      try {
        const res = await model[table].updateMany(sql, newDate, params);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: {},
          msg: `fail: ${error}`
        }
      }

    },

    async updateOne(sql, newDate, params) {
      const data = await model[table].find(sql);
      newDate = _.merge(newDate, { updateTime: (+new Date()) });
      if (!data.length) {
        newDate = _.merge(newDate, { createTime: (+new Date()) });
      }

      try {
        const res = await model[table].updateOne(sql, newDate, params);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: {},
          msg: `fail: ${error}`
        }
      }
    },

    async remove(sql) {
      try {
        const res = await model[table].remove(sql);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: {},
          msg: `fail: ${error}`
        }
      }
    },

    async removeOne() {
      try {
        const res = await model[table].findOneAndRemove(sql);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: {},
          msg: `fail: ${error}`
        }
      }
    },

    async aggregate(sql) {
      try {
        const res = await model[table].aggregate(sql);
        return {
          code: 200,
          data: res,
          msg: 'success'
        }
      } catch (error) {
        return {
          code: 201,
          data: {},
          msg: `fail: ${error}`
        }
      }
    }
  };
  return _f;
}