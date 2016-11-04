(function(module){
  var _ = require("underscore"),
      fn = {};

  function MysqlModelBuilder(connection, table) {
    var Model = function(json) {
    };

    Model.table = table;
    Model.connection = connection;
    console.info(connection);

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
    var table = this.table;
    this.query('created_at > ' + createdAt, {
      success: callback
    });
  }

  fn.query = function(where, options) {
    var table = this.table,
        that = this,
        query = 'SELECT * FROM ' + table + ' WHERE ' + where;

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

