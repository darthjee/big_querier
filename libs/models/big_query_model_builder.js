(function(module){
  var _ = require("underscore"),
      BigQueryApi = require('big_wrapper'),
      fn = {},
      fnClass = {};

  function BigQueryModelBuilder(datasetName, tableName) {
    var Model = function(json) {
      this.json = json;
    };

    Model.datasetName = datasetName;
    Model.tableName = tableName;
    _.extend(Model.prototype, fn);
    _.extend(Model, fnClass);

    return Model;
  }

  fn.enrich = function() {
    var json = this.json;

    return json;
  };

  fnClass.insertBatch = function(rows, callback) {
    this.getTable().insert(rows, {
      success: callback,
      error: function(err, insertErrors) {
        console.error(insertErrors[0]);
      }
    });
  };

  fnClass.getTable = function() {
    if(this.table) {
      return this.table
    } else {
      var datasetName = this.datasetName,
          tableName = this.tableName;
      return this.table = BigQueryApi.default.dataset(datasetName).table(tableName);
    }
  }

  fnClass.lastCreation = function(callback) {
    this.getTable().select('max(created_at)', {
      success: function(response) {
        var value = null;
        if (response[0]) {
          value = response[0].created_at;
        }
        callback(value);
      }
    });
  };

  module.exports = BigQueryModelBuilder;
})(module);

