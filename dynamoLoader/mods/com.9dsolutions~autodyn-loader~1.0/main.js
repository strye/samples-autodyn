var container = require("vertx/container");
var console = require('vertx/console');


container.deployVerticle("DynamoTable.java", function(err, deployID) {
	if (!err) {
		console.log("The DynamoTable has been deployed, deployment ID is " + deployID);
		container.deployVerticle("AutoDynManager.java", null);
	} else {
		console.log("Deployment failed! " + err.getMessage());
	}
});

