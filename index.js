(function(module) {
  var Cloudflare = require('./libs/services/cloudflare'),
      BigQueryApi = require('./libs/services/big_query/api');

  module.exports = {
    handler: function() {
      const projectFile = './config/auth.json',
            projectData = require(projectFile),
            bigQueryConfig = {
              projectId: projectData['project_id'],
              keyFilename: projectFile
            };

      BigQueryApi.default = new BigQueryApi(bigQueryConfig).connect();

    }
  };
})(module);
