(function(module) {
  var BigQueryApi = require('big_wrapper'),
      Mysql = require('mysql'),
      _ = require('underscore'),
      BigQueryModelBuilder = require('./libs/models/big_query_model_builder');
      MysqlModelBuilder = require('./libs/models/mysql_model_builder');
  const projectFile = './config/auth.json',
        querierFile = './config/querier.json',
        databaseFile = './config/database.json'

  module.exports = {
    initialize: function() {
      if (BigQueryApi.default) {
        return;
      }

      var projectData = require(projectFile),
          bigQueryConfig = {
            projectId: projectData['project_id'],
            keyFilename: projectFile
          };

      BigQueryApi.default = new BigQueryApi(bigQueryConfig).connect();
    },
    getQuerierConfig: function() {
      if (this.querierConfig) {
        return querierConfig;
      }
      return this.querierConfig = require(querierFile);
    },
    getDatabaseConfig: function() {
      if (this.databaseConfig) {
        return this.databaseConfig;
      }

      return this.databaseConfig = require(databaseFile),
    },
    getOrigin: function() {
      if (this.Origin) {
        return this.Origin;
      }

      var conn = Mysql.createConnection(this.databaseConfig),
          Origin = MysqlModelBuilder(conn, this.querierConfig.origin.table),
      return this.Origin = Origin;
    },
    getDestiny: function() {
      if (this.Destiny) {
        return this.Destiny;
      }

      var Destiny = BigQueryModelBuilder(this.querierConfig.destiny.dataset, this.querierConfig.destiny.table);

      return this.Destiny = Destiny;
    },
    insert: function() {
    },
    handler: function() {
      this.initialize();

      var Destiny = this.getDestiny(),
          Origin = this.getOrigin();

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
