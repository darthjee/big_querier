(function(module) {
  var BigQueryApi = require('big_wrapper'),
      Mysql = require('mysql'),
      _ = require('underscore'),
      BigQueryModelBuilder = require('./models/big_query_model_builder');
      MysqlModelBuilder = require('./models/mysql_model_builder');

  function BigQuerier(projectFile, querierFile, databaseFile) {
    var projectData = require(projectFile),
        bigQueryConfig = {
          projectId: projectData['project_id'],
          keyFilename: projectFile
        };

    this.projectFile = projectFile;
    this.querierFile = querierFile;
    this.databaseFile = databaseFile;

    BigQueryApi.default = new BigQueryApi(bigQueryConfig).connect();
  }

  var fn = BigQuerier.prototype;

  fn.getQuerierConfig = function() {
    if (this.querierConfig) {
      return querierConfig;
    }
    return this.querierConfig = require(this.querierFile);
  };

  fn.getDatabaseConfig = function() {
    if (this.databaseConfig) {
      return this.databaseConfig;
    }

    return this.databaseConfig = require(this.databaseFile);
  };

  fn.getOrigin = function() {
    if (this.Origin) {
      return this.Origin;
    }

    var databaseConfig = this.getDatabaseConfig(),
        conn = Mysql.createConnection(databaseConfig),
        querierConfig = this.getQuerierConfig(),
        Origin = MysqlModelBuilder(conn, querierConfig.origin.table)
    return this.Origin = Origin;
  };
  
  fn.getDestiny = function() {
    if (this.Destiny) {
      return this.Destiny;
    }

    var querierConfig = this.getQuerierConfig(),
        Destiny = BigQueryModelBuilder(querierConfig.destiny.dataset, querierConfig.destiny.table);

    return this.Destiny = Destiny;
  };

  fn.insert = function(data) {
    var Destiny = this.getDestiny(),
        rows = data.constructor == Array ? data : [ data ];

    Destiny.insertBatch(rows, function() {
      console.info('success');
    });
  };

  fn.enrich = function(rows) {
    var Destiny = this.getDestiny();
    return _.map(rows, function(e) {
      return new Destiny(e).enrich();
    });
  };

  fn.import = function() {
    var that = this;

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
  };
  module.exports = BigQuerier;
})(module);
