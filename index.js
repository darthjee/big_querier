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

      var databaseConfig = this.getDatabaseConfig(),
          conn = Mysql.createConnection(databaseConfig),
          querierConfig = this.getQuerierConfig(),
          Origin = MysqlModelBuilder(conn, querierConfig.origin.table),
      return this.Origin = Origin;
    },
    getDestiny: function() {
      if (this.Destiny) {
        return this.Destiny;
      }

      var querierConfig = this.getQuerierConfig(),
          Destiny = BigQueryModelBuilder(querierConfig.destiny.dataset, querierConfig.destiny.table);

      return this.Destiny = Destiny;
    },
    insert: function(data) {
      var Destiny = this.getDestiny(),
          rows = data.constructor == Array ? data : [ data ];

      Destiny.insertBatch(rows, function() {
        console.info('success');
      });
    },
    enrich: function(rows) {
      var Destiny = this.getDestiny();
      return _.map(rows, function(e) {
        return new Destiny(e).enrich();
      });
    },
    handler: function() {
      var that = this;
      this.initialize();

      var Destiny = this.getDestiny(),
          Origin = this.getOrigin();

      Destiny.lastCreation(function(last){
        if (last) {
          Origin.newerThan(last, function(rows) {
            rows = that.enrich(rows);
            that.insert(rows);
          });
        } else {
          Origin.fetch(function(rows) {
            rows = that.enrich(rows);
            that.insert(rows);
          });
        }
      });
    }
  };
})(module);
