module.exports = app => {
  const { mongo = {} } = app.config;
  if (mongo.url && mongo.tables || mongo && mongo.clients) {
    const mongo = require('./lib/nobita-mongo');
    mongo(app);
  }
}