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
      _db[database].list = async () => {
        let cols = await db[database].db.collections();
        let colStrArr = [];
        for (let c of cols) {
          colStrArr.push(c.s.name);
        }
      }
    }
  } else {
    _db = bind(mongo);
    _db._add = (databaseName, tableName, schema) => {
      app.context.db[tableName] = create(databaseName, tableName, schema)[tableName];
    };
    _db.list = async () => {
      const arr = mongo.url.split('/');
      const databaseName = arr[arr.length - 1];
      let cols = await db[databaseName].db.collections();
      let tableNames = [];
      for (let c of cols) {
        tableNames.push(c.s.name);
      }
     return tableNames;
    }
  }

  return _db;
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
  db[databaseName].on("open", async () => {
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
        const res = await model[tableName].findOne(sql, data.filter, data);
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
          data: res[0],
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
        const res = await model[tableName].deleteMany(sql);
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

    async removeOne(sql) {
      try {
        const res = await model[tableName].deleteOne(sql);
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
    },

    async drop() {
      try {
        const res = await model[tableName].collection.drop();
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