import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.platform.Verticle;
import org.vertx.java.core.json.JsonObject;

import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import java.io.FileReader;
import java.io.BufferedReader;

import java.util.HashMap;
import java.util.Map;

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
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.util.Tables;




public class LoadSampleData extends Verticle {
	static AmazonDynamoDBClient dynamoDB;

	static String goalTable = "khtzGoals";




	Handler<Message> myHandler = new Handler<Message>() {
		public void handle(Message message) {

			System.out.println("Generating Goals..." + message.body);
			LoadGoals();
		}
	};

	eb.registerHandler("kehootz.load.goals", myHandler);





	/*** START GOAL METHODS ***/
	private void LoadGoals() {
		// Create table if it does not exist yet
		if (Tables.doesTableExist(dynamoDB, goalTable)) {
			System.out.println("Table " + goalTable + " is already ACTIVE");
		} else {
			System.out.println("Table " + goalTable + " does not exist... Creating");


			// Create a table with a primary hash key named 'name', which holds a string
			CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(goalTable)
					.withKeySchema(new KeySchemaElement().withAttributeName("conspiracy").withKeyType(KeyType.HASH), new KeySchemaElement().withAttributeName("dossierId.id").withKeyType(KeyType.RANGE))
					.withAttributeDefinitions(new AttributeDefinition().withAttributeName("conspiracy").withAttributeType(ScalarAttributeType.S), new AttributeDefinition().withAttributeName("dossierId.id").withAttributeType(ScalarAttributeType.S))
					.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

			TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
			System.out.println("Created Table: " + createdTableDescription);

			// Wait for it to become active
			System.out.println("Waiting for " + goalTable + " to become ACTIVE...");
			Tables.waitForTableToBecomeActive(dynamoDB, goalTable);
		}

		DescribeTable(goalTable);
		AddRecords();
	}

	private void AddRecords() {

		BufferedReader reader = new BufferedReader(new FileReader("data/goals.json"));
		String line = null;

		while ((line = reader.readLine()) != null) {
			// Convert to json object
			if (line.length() > 0) {
				System.out.println(line);
				JsonObject item = new JsonObject(line);

				String conId = item.getString("conspiracy");
				String id = item.getString("dossier") + "." + item.getString("id");
				String title = item.getString("title");
				String description = item.getString("description");
				String lastActivity = item.getString("lastActivity");
				String effort = item.getString("effort");
				String value = item.getString("value");

				upsertRecord(conId, id, title, description, lastActivity, effort, value, "1");
				//upsertRecord("conspiracy", "dossierId.id", "title", "description", "lastActivity", effort, value, 1);

			}
		}

	}

	private void upsertRecord(String conspiracy, String id, String title, String description, String lastActivity, String effort, String value, String status) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("conspiracy", new AttributeValue(conspiracy));
		item.put("dossierId.id", new AttributeValue(id));
		item.put("title", new AttributeValue(title));
		item.put("description", new AttributeValue(description));
		item.put("lastActivity", new AttributeValue(lastActivity));
		item.put("effort", new AttributeValue().withN(effort) );
		item.put("value", new AttributeValue().withN(value) );
		item.put("status", new AttributeValue().withN(status) );

		addRecordToDynamo(item);
	}
	/*** END GOAL METHODS ***/



	/*** COMMON METHODS ***/
	private void DescribeTable(String tableName) {
		// Describe our new table
		DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
		TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
		System.out.println("Table Description: " + tableDescription);

	}
	private void addRecordToDynamo(Map<String, AttributeValue> data) {
		PutItemRequest putItemRequest = new PutItemRequest(tableName, data);
		PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
		System.out.println("Result: " + putItemResult);
	}

}