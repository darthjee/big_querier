(function(module) {
  var BigQuerier = require('./libs/big_querier');
  const projectFile = '../config/auth.json',
        querierFile = '../config/querier.json',
        databaseFile = '../config/database.json'

  module.exports = {
    handler: function() {
      new BigQuerier(projectFile, querierFile, databaseFile).import();
    }
  };
})(module);
