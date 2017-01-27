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
      const projectData = require(projectFile),
            bigQueryConfig = {
              projectId: projectData['project_id'],
              keyFilename: projectFile
            };

      BigQueryApi.default = new BigQueryApi(bigQueryConfig).connect();
      this.querierConfig = querierConfig;
    },
    getQuerierConfig: function() {
      if (this.querierConfig) {
        return querierConfig;
      }
      var querierConfig = require(querierFile);

      this.querierConfig = querierConfig;
    },
    getDatabaseConfig: function() {
      if (this.databaseConfig) {
        return this.databaseConfig;
      }

      var databaseConfig = require(databaseFile),
      this.databaseConfig = databaseConfig;
    },
    getOrigin: function() {
      if (this.Origin) {
        return this.Origin;
      }

      var conn = Mysql.createConnection(this.databaseConfig),
          Origin = MysqlModelBuilder(conn, this.querierConfig.origin.table),
      this.Origin = Origin;
      return Origin;
    },
    getDestiny: function() {
      if (this.Destiny) {
        return this.Destiny;
      }

      var Destiny = BigQueryModelBuilder(this.querierConfig.destiny.dataset, this.querierConfig.destiny.table);

      this.Destiny = Destiny;
      return Destiny;
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
