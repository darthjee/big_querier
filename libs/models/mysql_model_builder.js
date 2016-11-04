(function(module){
  var _ = require("underscore"),
      dateFormat = require('dateformat'),
      fn = {};

  function MysqlModelBuilder(connection, table) {
    var Model = function(json) {
      this.json = json;
    };

    Model.table = table;
    Model.connection = connection;

    _.extend(Model, fn);

    return Model;
  }

  fn.getConnection = function() {
    if (this.connection.state == 'disconnected') {
      this.connection.connect();
    }
    return this.connection;
  }

  fn.newerThan = function(date, callback) {
    var createdAt = [
      date.getUTCFullYear(),
      ('0' + (date.getUTCMonth()+1)).slice(-2),
      ('0' + date.getUTCDate()).slice(-2)
    ].join('-') + ' ' + [
      ('0' + date.getUTCHours()).slice(-2),
      ('0' + date.getUTCMinutes()).slice(-2),
      ('0' + date.getUTCSeconds()).slice(-2)
    ].join(':');
    this.query("created_at > '" + createdAt + "'", {
      success: callback,
      error: function() {
        console.error(arguments);
      }
    });
  }

  fn.fetch = function(callback) {
    this.query('1=1', {
      success: callback,
      error: function() {
        console.error(arguments);
      }
    });
  }

  fn.query = function(where, options) {
    var table = this.table,
        that = this,
        query = 'SELECT * FROM ' + table + ' WHERE ' + where + ' LIMIT 10';
        console.info(query);

    this.getConnection().query(query, function(err, rows, fields) {
      if(err) {
        options.error.call(that, err);
      } else {
        options.success.call(that, rows, fields);
      }
    });
  };

  module.exports = MysqlModelBuilder;
})(module);

