
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
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

public class AutoDynManager extends Verticle {
	static EventBus eb; // = vertx.eventBus();
	static String tableSpecPath = "/Users/willstrye/Documents/Source/Samples/autodyn/data/tableSpecs.json";
	static String loadMapPath = "/Users/willstrye/Documents/Source/Samples/autodyn/data/loadMap.json";


	public void start() {
		eb = vertx.eventBus();

		teardownTables();

		//createTables();

		//loadTables();
	}


	private void createTables(){
		try {
			BufferedReader reader = new BufferedReader(new FileReader(tableSpecPath));

			String line = null;

			while ((line = reader.readLine()) != null) {
				// Convert to json object
				if (line.length() > 0) {
					//System.out.println(line);
					JsonObject tableSpec = new JsonObject(line);

					// Send message to create table
					eb.send("autodyn.table.create", tableSpec, new Handler<Message<String>>() {
						public void handle(Message<String> message) {
							System.out.println("I received a reply " + message.body());
							/*if (message.body == "true") {
								// Load table
							}*/
						}
					});


				}
			}
		} catch (FileNotFoundException er) {
			System.out.println(er);
		} catch (IOException er) {
			System.out.println(er);
		}

	}

	private void teardownTables(){
		try {
			BufferedReader reader = new BufferedReader(new FileReader(tableSpecPath));

			String line = null;

			while ((line = reader.readLine()) != null) {
				// Convert to json object
				if (line.length() > 0) {
					//System.out.println(line);
					JsonObject tableSpec = new JsonObject(line);

					// Send message to delete table
					eb.send("autodyn.table.delete", tableSpec.getString("name"), new Handler<Message<String>>() {
						public void handle(Message<String> message) {
							System.out.println("I received a reply " + message.body());
							/*if (message.body == "true") {
								// Load table
							}*/
						}
					});


				}
			}
		} catch (FileNotFoundException er) {
			System.out.println(er);
		} catch (IOException er) {
			System.out.println(er);
		}
	}


	private void loadTables(){
		try {
			BufferedReader reader = new BufferedReader(new FileReader(loadMapPath));

			String line = null;

			while ((line = reader.readLine()) != null) {
				// Convert to json object
				if (line.length() > 0) {
					//System.out.println(line);
					JsonObject loadSpec = new JsonObject(line);

					loadTable(loadSpec.getString("tableName"), loadSpec.getArray("map"), loadSpec.getString("filePath"));

				}
			}
		} catch (FileNotFoundException er) {
			System.out.println(er);
		} catch (IOException er) {
			System.out.println(er);
		}

	}


	private void loadTable(String tableName, JsonArray map, String filePath){
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));

			String line = null;

			while ((line = reader.readLine()) != null) {
				// Convert to json object
				if (line.length() > 0) {
					//System.out.println(line);
					JsonObject record = new JsonObject(line);

					JsonObject upsertRequest = new JsonObject();
					upsertRequest.putString("tableName", tableName);
					upsertRequest.putObject("data", record);
					upsertRequest.putArray("map", map);

					// Send message to delete table
					eb.send("autodyn.table.upsert", upsertRequest, new Handler<Message<String>>() {
						public void handle(Message<String> message) {
							System.out.println("I received a reply " + message.body());
							/*if (message.body == "true") {
								// Load table
							}*/
						}
					});


				}
			}
		} catch (FileNotFoundException er) {
			System.out.println(er);
		} catch (IOException er) {
			System.out.println(er);
		}

	}

}
