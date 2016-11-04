(function(module) {
  var BigQueryApi = require('big_wrapper'),
      Mysql = require('mysql'),
      _ = require('underscore'),
      BigQueryModelBuilder = require('./libs/models/big_query_model_builder');
      MysqlModelBuilder = require('./libs/models/mysql_model_builder');

  module.exports = {
    handler: function() {
      const projectFile = './config/auth.json',
            databaseFile = './config/database.json',
            querierFile = './config/querier.json',
            projectData = require(projectFile),
            databaseConfig = require(databaseFile),
            querierConfig = require(querierFile),
            bigQueryConfig = {
              projectId: projectData['project_id'],
              keyFilename: projectFile
            };

      BigQueryApi.default = new BigQueryApi(bigQueryConfig).connect();
      var conn = Mysql.createConnection(databaseConfig),
          Origin = MysqlModelBuilder(conn, querierConfig.origin.table),
          Destiny = BigQueryModelBuilder(querierConfig.destiny.dataset, querierConfig.destiny.table);

      Destiny.lastCreation(function(last){
        if (last) {
          Origin.newerThan(last, function(rows) {
            rows = _.map(rows, function(e) {
              return new Destiny(e).enrich();
            });
            Destiny.insertBatch(rows, function() {
              console.info('success');
            });
          });
        } else {
          Origin.fetch(function(rows) {
            rows = _.map(rows, function(e) {
              return new Destiny(e).enrich();
            });
            Destiny.insertBatch(rows, function() {
              console.info('success');
            });
          });
        }
      });
    }
  };
})(module);
