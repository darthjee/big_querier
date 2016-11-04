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

  fn.newerThan = function(createdAt, callback) {
    createdAt = dateFormat(createdAt, "yyyy-mm-dd h:MM:ss")
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

