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
    return this.json;
  };

  fnClass.insertBatch = function(rows) {
    console.info('get table');
    this.getTable().insert(rows);
  };

  fnClass.getTable = function() {
    if(this.table) {
      return this.table
    } else {
      var datasetName = this.datasetName,
          tableName = this.tableName;
      console.info('names', datasetName, tableName);
      return this.table = BigQueryApi.default.dataset(datasetName).table(tableName);
    }
  }

  module.exports = BigQueryModelBuilder;
})(module);

