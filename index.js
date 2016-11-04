(function(module) {
  var BigQueryApi = require('big_wrapper'),
      Mysql = require('mysql');

  module.exports = {
    handler: function() {
      const projectFile = './config/auth.json',
            databaseFile = './config/database.json',
            projectData = require(projectFile),
            databaseConfig = require(databaseFile),
            bigQueryConfig = {
              projectId: projectData['project_id'],
              keyFilename: projectFile
            };

      BigQueryApi.default = new BigQueryApi(bigQueryConfig).connect();
      return Mysql.createConnection(databaseConfig);
    }
  };
})(module);
