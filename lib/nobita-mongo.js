const mongoose = require('mongoose');
const _ = require('lodash');
let db = {};

module.exports = app => {
  let _db = {};

  if (app.config.mongo.clients) {
    for (let database in app.config.mongo.clients) {
      _db[database] = bind(app.config.mongo.clients[database]);
      _db[database]._add = (databaseName, table, tables) => {
        app.context.db[table] = creat(databaseName, table, tables)[table]
      };
    }
  } else {
    _db = bind(app.config.mongo);
    _db._add = (databaseName, table, tables) => {
      app.context.db[table] = creat(databaseName, table, tables)[table]
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
  }, config.option);

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
    _f = creat(databaseName, table, config.tables[table])
  }
  return _f;
}

const creat = (databaseName, table, tables) => {
  const model = [];
  let _f = {};
  tables = Object.assign(tables, {
    createTime: {
      type: Date,
      default: Date.now
    },

    updateTime: {
      type: Date,
      default: Date.now
    }
  });

  let Schema = new mongoose.Schema(tables);
  model[table] = db[databaseName].model(table, Schema, table);

  _f[table] = {
    find(sql, data = {}) {
      data.page = +(data.page || 0);
      data.limit = +(data.limit || 20);
      data.skip = data.page * data.limit || 0;

      return new Promise((resolve, reject) => {
        model[table].countDocuments(sql, (err, total) => {

          model[table].find(sql, null, data, (err, res) => {
            if (err) {
              console.log(err);
              resolve({
                code: 201,
                data: {},
                msg: `fail: ${err}`
              });
            } else {
              resolve({
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
              });
            }
          });
        })

      });
    },

    findOne(sql, data = {}) {
      return new Promise((resolve, reject) => {
        model[table].countDocuments(sql, (err, total) => {

          model[table].findOne(sql, null, data, (err, res) => {
            if (err) {
              console.log(err);
              resolve({
                code: 201,
                data: {},
                msg: `fail: ${err}`
              });
            } else {
              resolve({
                code: 200,
                data: res,
                msg: 'success'
              });
            }
          });
        })

      });
    },

    insertMany(sql) {
      return new Promise((resolve, reject) => {
        for (let i in sql) {
          sql[i] = _.merge(sql[i], {
            updateTime: (+new Date()),
            createTime: (+new Date())
          })
        }
        model[table].insertMany(sql, function (err, res) {
          if (err) {
            console.log(err);
            resolve({
              code: 201,
              data: {},
              msg: `fail: ${err}`
            });
          }
          else {
            resolve({
              code: 200,
              data: res,
              msg: 'success'
            });
          }

        });
      });
    },

    insert(sql) {
      return new Promise((resolve, reject) => {
        sql = _.merge(sql, {
          updateTime: (+new Date()),
          createTime: (+new Date())
        })
        model[table].insertMany(sql, function (err, res) {
          if (err) {
            console.log(err);
            resolve({
              code: 201,
              data: {},
              msg: `fail: ${err}`
            });
          }
          else {
            resolve({
              code: 200,
              data: res,
              msg: 'success'
            });
          }

        });
      });

    },

    update(sql, newDate, params) {
      return new Promise((resolve, reject) => {
        newDate = _.merge(newDate, { updateTime: (+new Date()) })
        model[table].update(sql, newDate, params, (err, res) => {
          if (err) {
            console.log(err);
            resolve({
              code: 201,
              data: {},
              msg: `fail: ${err}`
            });
          } else {
            resolve({
              code: 200,
              data: res,
              msg: 'success'
            });
          }
        })
      });
    },

    remove(sql) {
      return new Promise((resolve, reject) => {
        model[table].remove(sql, (err, res) => {
          if (err) {
            console.log(err);
            resolve({
              code: 201,
              data: {},
              msg: `fail: ${err}`
            });
          } else {
            resolve({
              code: 200,
              data: res,
              msg: 'success'
            });
          }
        })
      });
    },

    removeOne() {
      return new Promise((resolve, reject) => {
        model[table].findOneAndRemove(sql, (err, res) => {
          if (err) {
            console.log(err);
            resolve({
              code: 201,
              data: {},
              msg: `fail: ${err}`
            });
          } else {
            resolve({
              code: 200,
              data: res,
              msg: 'success'
            });
          }
        })
      });
    },

    aggregate(sql) {
      return new Promise((resolve, reject) => {
        model[table].aggregate(sql, (err, res) => {
          if (err) {
            console.log(err);
            resolve({
              code: 201,
              data: {},
              msg: `fail: ${err}`
            });
          } else {
            resolve({
              code: 200,
              data: {
                list: res
              },
              msg: 'success'
            });
          }
        })
      });
    }
  };
  return _f;
}