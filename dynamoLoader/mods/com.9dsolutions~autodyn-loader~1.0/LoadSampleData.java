
//import org.vertx.java.core.Handler;
//import org.vertx.java.core.eventbus.Message;
import org.vertx.java.platform.Verticle;
import org.vertx.java.core.json.JsonObject;


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




/**
 * Created for kehootz
 * User: wstry1
 * Date: 4/26/14
 * Time: 3:15 PM
 */
public class LoadSampleData extends Verticle {
	static AmazonDynamoDBClient dynamoDB;


	static String conspiraciesTable = "khtzConspiracies";

	static String dossierTable = "khtzDossiers";
	static String goalTable = "khtzGoals";
	static String goalItemTable = "khtzGoalItems";

	// for debugging
	static boolean print = false;

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


		try {

			LoadConspiracies();

			LoadDossiers();

			LoadGoals();

			LoadGoalItems();

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to AWS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with AWS, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}



	}



	/*** START CONSPIRACY METHODS ***/
	private void LoadConspiracies() {
		// Create table if it does not exist yet
		if (Tables.doesTableExist(dynamoDB, conspiraciesTable)) {
			System.out.println("Table " + conspiraciesTable + " is already ACTIVE");
		} else {
			System.out.println("Table " + conspiraciesTable + " does not exist... Creating");

			// Create a table with a primary hash key named 'name', which holds a string
			CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(conspiraciesTable)
					.withKeySchema(new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH))
					.withAttributeDefinitions(new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.S))
					.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
			TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
			System.out.println("Created Table: " + createdTableDescription);

			// Wait for it to become active
			System.out.println("Waiting for " + conspiraciesTable + " to become ACTIVE...");
			Tables.waitForTableToBecomeActive(dynamoDB, conspiraciesTable);
		}

		DescribeTable(conspiraciesTable);
		AddConspiracies();
	}

	private void AddConspiracies() {
		upsertConspiracy("504becc3848f127768000001", "Captains Log", "Personal Goals and Tasks", "1347153091496");
		upsertConspiracy("504c00bd848f127768000005", "Kehootz", "This is the conspiracy for the Kehootz web site.", "1347158205224");
		upsertConspiracy("504bedba848f127768000003", "Book Co-Op (Boo Coo)", "This is the project site for the book coop and sharing site", "1347838984266");
		upsertConspiracy("505e1c97848f127768000016", "Nike", "General catch-all for Nike tasks", "1348344983734");
	}

	private void upsertConspiracy(String id, String title, String description, String lastActivity) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("id", new AttributeValue(id));
		item.put("title", new AttributeValue(title));
		item.put("description", new AttributeValue(description));
		item.put("lastActivity", new AttributeValue(lastActivity));

		addRecordToDynamo(conspiraciesTable, item);
	}
	/*** END CONSPIRACY METHODS ***/


	/*** START DOSSIER METHODS ***/
	private void LoadDossiers() {
		// Create table if it does not exist yet
		if (Tables.doesTableExist(dynamoDB, dossierTable)) {
			System.out.println("Table " + dossierTable + " is already ACTIVE");
		} else {
			System.out.println("Table " + dossierTable + " does not exist... Creating");

			// Create secondary Index
			LocalSecondaryIndex secondIdx = new LocalSecondaryIndex().withIndexName("statusIndex")
					.withKeySchema(new KeySchemaElement().withAttributeName("conspiracy").withKeyType(KeyType.HASH), new KeySchemaElement().withAttributeName("status").withKeyType(KeyType.RANGE))
					.withProjection( new Projection().withProjectionType(ProjectionType.KEYS_ONLY ));

			// Create a table with a primary hash key named 'name', which holds a string
			CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(dossierTable)
					.withKeySchema(new KeySchemaElement().withAttributeName("conspiracy").withKeyType(KeyType.HASH), new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.RANGE))
					.withAttributeDefinitions(new AttributeDefinition().withAttributeName("conspiracy").withAttributeType(ScalarAttributeType.S), new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.S))
					.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

			TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
			System.out.println("Created Table: " + createdTableDescription);

			// Wait for it to become active
			System.out.println("Waiting for " + dossierTable + " to become ACTIVE...");
			Tables.waitForTableToBecomeActive(dynamoDB, dossierTable);
		}

		DescribeTable(dossierTable);
		AddDossiers();
	}

	private void AddDossiers() {
		upsertDossiers("504becc3848f127768000001", "51f5753c69c878ce222fb280", "General TODOs", "General TODOs", "1347153091496", 1);
		upsertDossiers("504c00bd848f127768000005", "51f5753c69c878ce222fb281", "Root Dossier", "Root Dossier", "1347158205224", 1);
		upsertDossiers("504bedba848f127768000003", "51f5753c69c878ce222fb282", "Root Dossier", "Root Dossier", "1347838984266", 1);
		upsertDossiers("505e1c97848f127768000016", "51f5753c69c878ce222fb283", "General TODOs", "General TODOs", "1348344983734", 1);
	}

	private void upsertDossiers(String conspiracy, String id, String title, String description, String lastActivity, int status) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("conspiracy", new AttributeValue(conspiracy));
		item.put("id", new AttributeValue(id));
		item.put("title", new AttributeValue(title));
		item.put("description", new AttributeValue(description));
		item.put("lastActivity", new AttributeValue(lastActivity));
		item.put("status", new AttributeValue().withN(Integer.toString(status)) );

		addRecordToDynamo(dossierTable, item);
	}
	/*** END DOSSIER METHODS ***/




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
		AddGoals();
	}

	private void AddGoals() {

		try {
			BufferedReader reader = new BufferedReader(new FileReader("/Users/willstrye/Documents/Source/kehootz/kehootz/dynamoLoader/mods/com.kehootz~dynamo-loader~1.0/data/goals.json"));

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

					upsertGoal(conId, id, title, description, lastActivity, effort, value, "1");
					//upsertRecord("conspiracy", "dossierId.id", "title", "description", "lastActivity", effort, value, 1);

				}
			}
		} catch (FileNotFoundException er) {
			System.out.println(er);
		} catch (IOException er) {
			System.out.println(er);
		}

	}

	private void upsertGoal(String conspiracy, String id, String title, String description, String lastActivity, String effort, String value, String status) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("conspiracy", new AttributeValue(conspiracy));
		item.put("dossierId.id", new AttributeValue(id));
		item.put("title", new AttributeValue(title));
		item.put("description", new AttributeValue(description));
		item.put("lastActivity", new AttributeValue(lastActivity));
		item.put("effort", new AttributeValue().withN(effort) );
		item.put("value", new AttributeValue().withN(value) );
		item.put("status", new AttributeValue().withN(status) );

		addRecordToDynamo(goalTable, item);
	}
	/*** END GOAL METHODS ***/





	/*** START GOAL ITEM METHODS ***/
	private void LoadGoalItems() {
		// Create table if it does not exist yet
		if (Tables.doesTableExist(dynamoDB, goalItemTable)) {
			System.out.println("Table " + goalItemTable + " is already ACTIVE");
		} else {
			System.out.println("Table " + goalItemTable + " does not exist... Creating");


			// Create a table with a primary hash key named 'name', which holds a string
			CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(goalItemTable)
					.withKeySchema(new KeySchemaElement().withAttributeName("conspiracy").withKeyType(KeyType.HASH), new KeySchemaElement().withAttributeName("dossierId.goalId.id").withKeyType(KeyType.RANGE))
					.withAttributeDefinitions(new AttributeDefinition().withAttributeName("conspiracy").withAttributeType(ScalarAttributeType.S), new AttributeDefinition().withAttributeName("dossierId.goalId.id").withAttributeType(ScalarAttributeType.S))
					.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

			TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
			System.out.println("Created Table: " + createdTableDescription);

			// Wait for it to become active
			System.out.println("Waiting for " + goalItemTable + " to become ACTIVE...");
			Tables.waitForTableToBecomeActive(dynamoDB, goalItemTable);
		}

		DescribeTable(goalItemTable);
		AddGoalItems();
	}

	private void AddGoalItems() {

		try {
			BufferedReader reader = new BufferedReader(new FileReader("/Users/willstrye/Documents/Source/kehootz/kehootz/dynamoLoader/mods/com.kehootz~dynamo-loader~1.0/data/goalItems.json"));

			String line = null;

			while ((line = reader.readLine()) != null) {
				// Convert to json object
				if (line.length() > 0) {
					System.out.println(line);
					JsonObject item = new JsonObject(line);

					String conId = item.getString("conspiracy");
					String id = item.getString("dossier") + "." + item.getString("goal") + "." + item.getString("id");
					String title = item.getString("title");
					String description = item.getString("description");
					String lastActivity = item.getString("lastActivity");
					String points = item.getString("points");
					String type = item.getString("type");

					upsertGoalItem(conId, id, title, description, lastActivity, points, type, "1");
					//upsertRecord("conspiracy", "dossierId.id", "title", "description", "lastActivity", effort, value, 1);

				}
			}
		} catch (FileNotFoundException er) {
			System.out.println(er);
		} catch (IOException er) {
			System.out.println(er);
		}

	}

	private void upsertGoalItem(String conspiracy, String id, String title, String description, String lastActivity, String points, String type, String status) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("conspiracy", new AttributeValue(conspiracy));
		item.put("dossierId.goalId.id", new AttributeValue(id));

		item.put("title", new AttributeValue(title));
		if (description != null && description.length() > 0) {
			item.put("description", new AttributeValue(description));
		}
		item.put("lastActivity", new AttributeValue(lastActivity));
		item.put("points", new AttributeValue().withN(points) );
		item.put("type", new AttributeValue(type) );
		item.put("status", new AttributeValue().withN(status) );

		addRecordToDynamo(goalItemTable, item);
	}
	/*** END GOAL ITEM METHODS ***/




	/*
	private void SearchItems(String tableName) {

		// Scan items for movies with a year attribute greater than 1985
		HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
		Condition condition = new Condition()
				.withComparisonOperator(ComparisonOperator.GT.toString())
				.withAttributeValueList(new AttributeValue().withN("1985"));
		scanFilter.put("year", condition);
		ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
		ScanResult scanResult = dynamoDB.scan(scanRequest);
		System.out.println("Result: " + scanResult);

	}
	*/

	/*** COMMON METHODS ***/
	private void DescribeTable(String tableName) {
		// Describe our new table
		DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
		TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
		System.out.println("Table Description: " + tableDescription);

	}
	private void addRecordToDynamo(String tableName, Map<String, AttributeValue> data) {
		PutItemRequest putItemRequest = new PutItemRequest(tableName, data);
		PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
		System.out.println("Result: " + putItemResult);
	}

}
