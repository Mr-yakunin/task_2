import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.time.Instant;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;


public class JavaAggregator {
	// these values used to setup the RPC connection with flume agent
	// i must send them output information from application
	// flume save this in hdfs storage
	public static final String LOCAL_HOST = "localhost";
	public static final int PORT = 44444;

	// describe Metric class for application
	// input info such: id, timestamp, value
	// we work only with id and value infomation
	public static class Metric implements Serializable {
		private int id;
		private int value;

		// get id method
		public int getId() {
		  return id;
		}

		// set id method
		public void setId(int id) {
		  this.id = id;
		}

		// get value method
		public int getValue() {
			return value;
		}

		// set value method
		public void setValue(int value) {
			this.value = value;
		}
	}

	// desribe RpcClient class
	// it sends data to flume 
	public static class MyRpcClientFacade {
		private RpcClient client;
		private String hostname;
		private int port;

		// Setup the RPC connection
		public void init(String hostname, int port) {
			this.hostname = hostname;
			this.port = port;
			this.client = RpcClientFactory.getDefaultInstance(hostname, port);
		}

		// Create a Flume Event object that encapsulates the sample data
		public void sendDataToFlume(String data) {
			Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
			// Send the event
			try {
			  client.append(event);
			} catch (EventDeliveryException e) {
			  // clean up and recreate the client
			  client.close();
			  client = null;
			  client = RpcClientFactory.getDefaultInstance(hostname, port);
			}
		}

		// Close the RPC connection
		public void cleanUp() {
			client.close();
		}
	}

	// main program
	public static void main(String[] args) throws Exception {
		// check arguments, it must be only two
		if ((args.length != 2) || (!(args[1].equals("avg") || args[1].equals("min") || args[1].equals("max")))) {
			System.err.println("Usage: JavaAggregator <input file> <scale>\n" +
			"Files must be into HDFS\n" +
			"Scale must be only:\n" +
			"- avg - average metric\n" +
			"- min - minimum metric\n" +
			"- max - maximum metric");
			System.exit(1);
		}

		// create a basic SparkSession
		SparkSession spark = SparkSession
			.builder()
			.appName("Java Spark SQL basic example")
			.config("spark.some.config.option", "some-value")
			.getOrCreate();

		// Create an RDD of Metric objects from a text file 
		// /app/test file in HDFS
		JavaRDD<Metric> metricRDD = spark.read()
			.textFile(args[0])
			.javaRDD()
			.map(line -> {
				String[] parts = line.split(",");
				Metric metric = new Metric();
				metric.setId(Integer.parseInt(parts[0].trim()));
				metric.setValue(Integer.parseInt(parts[2].trim()));
				return metric;
			});

		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		Dataset<Row> metricDF = spark.createDataFrame(metricRDD, Metric.class);
		// Register the DataFrame as a temporary view
		metricDF.createOrReplaceTempView("metric");
		// prepare dataset variable
		Dataset<String> sendDataSet = spark.createDataset(new ArrayList<String>(),Encoders.STRING());

		switch(args[1]) {
			case "avg": sendDataSet = metricDF.groupBy("id").agg(functions.round(functions.avg("value").as("average"),2)).
								    orderBy(functions.asc("id")).map(
										(MapFunction<Row, String>)
										row -> row.get(0) + ", " + Long.toString(Instant.now().getEpochSecond()) + ", avg, " + row.get(1),
										Encoders.STRING());
										break;
			case "min": sendDataSet = metricDF.groupBy("id").agg(functions.min("value").as("minimum")).
										orderBy(functions.asc("id")).map(
										(MapFunction<Row, String>)
										row -> row.get(0) + ", " + Long.toString(Instant.now().getEpochSecond()) + ", min, " + row.get(1),
										Encoders.STRING());
										break;
			case "max": sendDataSet = metricDF.groupBy("id").agg(functions.max("value").as("maximum")).
										orderBy(functions.asc("id")).map(
										(MapFunction<Row, String>)
										row -> row.get(0) + ", " + Long.toString(Instant.now().getEpochSecond()) + ", max, " + row.get(1),
										Encoders.STRING());
										break;
										//orderBy(function.desc())
		}
		// make list form dataset
		List<String> sendDataPrepare = sendDataSet.as(Encoders.STRING()).collectAsList();
		// prepare send data information
		String sendData = String.join("\n", sendDataPrepare);
		
		MyRpcClientFacade client = new MyRpcClientFacade();
		// Initialize client with the remote Flume agent's host and port
		client.init(LOCAL_HOST, PORT);
		/*for (String message : sendDataPrepare) {
			client.sendDataToFlume(message);
		}*/
		// send data to flume agent
		client.sendDataToFlume(sendData);
		// Close the RPC connection
		client.cleanUp();
		// stop spark session application
		spark.stop();
	}
}