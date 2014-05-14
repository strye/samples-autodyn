
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.platform.Verticle;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.json.JsonArray;


import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.util.Tables;


public class DynamoTable extends Verticle {
	static AmazonDynamoDBClient dynamoDB;

	/**
	 * The only information needed to create a client are security credentials
	 * consisting of the AWS Access Key ID and Secret Access Key. All other
	 * configuration, such as the service endpoints, are performed
	 * automatically. Client parameters, such as proxies, can be specified in an
	 * optional ClientConfiguration object when constructing a client.
	 *
	 * @see com.amazonaws.auth.BasicAWSCredentials
	 * @see com.amazonaws.auth.PropertiesCredentials
	 * @see com.amazonaws.ClientConfiguration
	 */
	private static void init() {
    	/*
		 * This credentials provider implementation loads your AWS credentials
		 * from a properties file at the root of your classpath.
		 */
		dynamoDB = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider());
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		dynamoDB.setRegion(usWest2);
	}


	public void start() {
		init();

		vertx.eventBus().registerHandler("autodyn.table.create", new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> message) {
				System.out.println("I received a create message");

				boolean created = CreateTable(message.body());
				if (created) {
					message.reply("true");
				} else {
					message.reply("false");
				}
			}
		});


		vertx.eventBus().registerHandler("autodyn.table.delete", new Handler<Message<String>>() {
			@Override
			public void handle(Message<String> message) {
				System.out.println("I received a delete message");

				boolean deleted = DeleteTable(message.body());
				if (deleted) {
					message.reply("true");
				} else {
					message.reply("false");
				}
			}
		});




		vertx.eventBus().registerHandler("autodyn.table.upsert", new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> message) {
				System.out.println("I received an upsert message");

				boolean upserted = UpsertTableItem(message.body());
				if (upserted) {
					message.reply("true");
				} else {
					message.reply("false");
				}
			}
		});


		container.logger().info("DynamoTable listener started");
	}

	private boolean CreateTable(JsonObject tableSpec) {
		boolean res = false;
		try {

			// Variables used to genrate the table
			String tableName = tableSpec.getString("name");
			JsonArray keys = tableSpec.getArray("keys");
			System.out.println(tableName);


			// Create table if it does not exist yet
			if (Tables.doesTableExist(dynamoDB, tableName)) {
				System.out.println("Table " + tableName + " is already ACTIVE");
			} else {
				System.out.println("Table " + tableName + " does not exist... Creating");

				List<KeySchemaElement> keyElements = new ArrayList<>();
				List<AttributeDefinition> attrElements = new ArrayList<>();
				for (Object elem : keys) {
					JsonObject key = (JsonObject) elem;
					String name = key.getString("name");
					String keyType = key.getString("keyType");
					String attrType = key.getString("attrType");

					keyElements.add(GenerateKey(name, keyType));
					attrElements.add(GenerateAttribute(name, attrType));
				}


				CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
						.withKeySchema(keyElements)
						.withAttributeDefinitions(attrElements)
						.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
				TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
				System.out.println("Created Table: " + createdTableDescription);


				// Wait for it to become active
				System.out.println("Waiting for " + tableName + " to become ACTIVE...");
				Tables.waitForTableToBecomeActive(dynamoDB, tableName);
			}

			DescribeTable(tableName);

			res = true;
		} catch (AmazonServiceException ase) {
			AmzServiceExptn(ase);
		} catch (AmazonClientException ace) {
			AmzClientExptn(ace);
		}

		return res;
	}


	private boolean DeleteTable(String tableName) {
		boolean res = false;
		try {

			// Delete table if it exists
			if (Tables.doesTableExist(dynamoDB, tableName)) {
				System.out.println("Table " + tableName + " is ACTIVE...Preparign to delete");


				DeleteTableRequest deleteTableRequest = new DeleteTableRequest().withTableName(tableName);
				TableDescription deletedTableDescription = dynamoDB.deleteTable(deleteTableRequest).getTableDescription();
				System.out.println("Deleting Table: " + deletedTableDescription);
			} else {
				System.out.println("Table " + tableName + " does not exist...can not delete");
			}

			res = true;
		} catch (AmazonServiceException ase) {
			AmzServiceExptn(ase);
		} catch (AmazonClientException ace) {
			AmzClientExptn(ace);
		}

		return res;
	}



	private boolean UpsertTableItem(JsonObject request) {
		boolean res = false;


		try {
			String tableName = request.getString("tableName");
			JsonObject data = request.getObject("data");
			JsonArray map = request.getArray("map");

			if (Tables.doesTableExist(dynamoDB, tableName)) {
				Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();


				for (Object elem : map) {
					JsonObject key = (JsonObject) elem;
					String field = key.getString("field");
					String type = key.getString("type");
					String value = data.getString(field);

					if (value != null && value.length() > 0) {
						item.put(field, prepareValue(data.getString(field), type));
					}
				}


				PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
				PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
				System.out.println("Result: " + putItemResult);
			} else {
				System.out.println("Table " + tableName + " does not exist...can not load");
			}

			res = true;
		} catch (AmazonServiceException ase) {
			AmzServiceExptn(ase);
		} catch (AmazonClientException ace) {
			AmzClientExptn(ace);
		}

		return res;
	}

	private AttributeValue prepareValue(String value, String type) {
		AttributeValue res = new AttributeValue();

		// TODO: Expand to handle more types
		switch (type) {
			case "S":
				res.setS(value);
				break;
			case "N":
				res.setN(value);
				break;
		}

		return res;
	}



	private KeySchemaElement GenerateKey(String name, String keyType) {
		// Set key Type
		//KeyType type = KeyType.fromValue(keyType);

		return new KeySchemaElement().withAttributeName(name).withKeyType(keyType);
	}


	private AttributeDefinition GenerateAttribute(String name, String attrType) {
		// Set Attr Type
		//ScalarAttributeType type = ScalarAttributeType.fromValue(attrType);

		return new AttributeDefinition().withAttributeName(name).withAttributeType(attrType);
	}

	private LocalSecondaryIndex GenerateSecondaryIndex(String name, JsonArray keys, String projectionType) {
		// This method shows how to generate a secondary index
		// We also show loading enumerators by thier string value
		//   instead of converting with the fromValue method
		//   This can simplify the code and remove some of the helper methods contained here-in

		List<KeySchemaElement> keyElements = new ArrayList<>();
		for (Object elem : keys) {
			JsonObject key = (JsonObject) elem;
			String keyName = key.getString("name");
			String keyType = key.getString("keyType");

			keyElements.add(new KeySchemaElement().withAttributeName(keyName).withKeyType(keyType));
		}

		return new LocalSecondaryIndex().withIndexName(name)
			.withKeySchema(keyElements)
			.withProjection( new Projection().withProjectionType(projectionType));
	}



	/*** COMMON METHODS ***/
	private void DescribeTable(String tableName) {
		// Describe our new table
		DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
		TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
		System.out.println("Table Description: " + tableDescription);

	}

	private void AmzServiceExptn (AmazonServiceException ase) {
		System.out.println("Caught an AmazonServiceException, which means your request made it "
				+ "to AWS, but was rejected with an error response for some reason.");
		System.out.println("Error Message:    " + ase.getMessage());
		System.out.println("HTTP Status Code: " + ase.getStatusCode());
		System.out.println("AWS Error Code:   " + ase.getErrorCode());
		System.out.println("Error Type:       " + ase.getErrorType());
		System.out.println("Request ID:       " + ase.getRequestId());
	}

	private void AmzClientExptn (AmazonClientException ace) {
		System.out.println("Caught an AmazonClientException, which means the client encountered "
				+ "a serious internal problem while trying to communicate with AWS, "
				+ "such as not being able to access the network.");
		System.out.println("Error Message: " + ace.getMessage());
	}


}