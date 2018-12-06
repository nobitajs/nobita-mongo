const mongoose = require('mongoose');

module.exports = app => {
  mongoose.connect(app.config.mongo.url);
  // 连接失败
  mongoose.connection.on("error", function (err) {
    console.error("数据库链接失败:" + err);
  });
  // 连接成功
  mongoose.connection.on("open", function () {
    console.log("数据库链接成功");
  });
  // 断开数据库
  mongoose.connection.on("disconnected", function () {
    console.log("数据库断开");
  });

  const model = [];
  let _f = {}

  for (let table in app.config.mongo.tables) {
    let tables = Object.assign(app.config.mongo.tables[table], {
      createTime: {
        type: Date,
        default: Date.now
      },

      updateTime: {
        type: Date,
        default: Date.now
      }
    })
    let Schema = new mongoose.Schema(tables);
    model[table] = mongoose.model(table, Schema, table);
  }

  for (let table in app.config.mongo.tables) {
    _f[table] = {
      find(sql, data) {
        return new Promise((resolve, reject) => {
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
                  list: res
                },
                msg: 'success'
              });
            }
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
  }
  return _f;
}