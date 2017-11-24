import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

public class JavaTests implements Serializable {
	
	private transient SparkSession spark;
	private transient JavaSparkContext jsc;

	@Before
	// set sparksession, javasparkcontext etc for testing application
	public void setUp() {
		spark = SparkSession.builder()
		.master("local[*]")
		.appName("testing")
		.config("spark.driver.memory", "1073741824") // i have too little memory ...
		.config("spark.testing.memory", "1073741824")
		.getOrCreate();
		jsc = new JavaSparkContext(spark.sparkContext());
	}

	@After
	// stop sparksession after executed testing
	public void tearDown() {
		spark.stop();
		spark = null;
	}

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

	
	@Test
	// test created schema, dataframe
	public void applySchema() {
		// create some tests values
		List<Metric> metricList = new ArrayList<>(2);
		Metric metric1 = new Metric();
		metric1.setId(1);
		metric1.setValue(30);
		metricList.add(metric1);
		Metric metric2 = new Metric();
		metric2.setId(1);
		metric2.setValue(50);
		metricList.add(metric2);

		JavaRDD<Row> rowRDD = jsc.parallelize(metricList).map(
        metric -> RowFactory.create(metric.getId(), metric.getValue()));
		// create struct of schema
		List<StructField> fields = new ArrayList<>(2);
		fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
		fields.add(DataTypes.createStructField("value", DataTypes.IntegerType, false));
		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
		df.createOrReplaceTempView("metric");
		List<Row> actual = spark.sql("SELECT * FROM metric").collectAsList();
		// create expected values
		List<Row> expected = new ArrayList<>(2);
		expected.add(RowFactory.create(1, 30));
		expected.add(RowFactory.create(1, 50));
		// test if objects are equal
		Assert.assertEquals(expected, actual);
	}

	@Test
	// test average operation with values
	public void dataFrameAverageOperation() {
		// create some tests values
		List<Metric> metricList = new ArrayList<>(4);
		Metric metric1 = new Metric();
		metric1.setId(1);
		metric1.setValue(30);
		metricList.add(metric1);
		Metric metric2 = new Metric();
		metric2.setId(1);
		metric2.setValue(50);
		metricList.add(metric2);
		Metric metric3 = new Metric();
		metric3.setId(2);
		metric3.setValue(20);
		metricList.add(metric3);
		Metric metric4 = new Metric();
		metric4.setId(2);
		metric4.setValue(70);
		metricList.add(metric4);

		JavaRDD<Row> rowRDD = jsc.parallelize(metricList).map(
        metric -> RowFactory.create(metric.getId(), metric.getValue()));

		List<StructField> fields = new ArrayList<>(2);
		fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
		fields.add(DataTypes.createStructField("value", DataTypes.IntegerType, false));
		StructType schema = DataTypes.createStructType(fields);
		// perform average sql operation
		Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
		df.createOrReplaceTempView("metric");
		Dataset<String> ds = df.groupBy("id").agg(functions.avg("value").as("average")).map(
									(MapFunction<Row, String>)
									row -> row.get(0) + ", timestamp, avg, " + row.get(1),
									Encoders.STRING());
		List<String> actual = ds.as(Encoders.STRING()).collectAsList();
		// create expected values
		List<String> expected = new ArrayList<String>(2);
		expected.add("1, timestamp, avg, 40.0");
		expected.add("2, timestamp, avg, 45.0");
		// test if objects are equal
		Assert.assertEquals(expected, actual);
	}

	@Test
	// test minimum operation with values
	public void dataFrameMinimumOperation() {
		// create some tests values
		List<Metric> metricList = new ArrayList<>(4);
		Metric metric1 = new Metric();
		metric1.setId(1);
		metric1.setValue(30);
		metricList.add(metric1);
		Metric metric2 = new Metric();
		metric2.setId(1);
		metric2.setValue(50);
		metricList.add(metric2);
		Metric metric3 = new Metric();
		metric3.setId(2);
		metric3.setValue(20);
		metricList.add(metric3);
		Metric metric4 = new Metric();
		metric4.setId(2);
		metric4.setValue(70);
		metricList.add(metric4);

		JavaRDD<Row> rowRDD = jsc.parallelize(metricList).map(
        metric -> RowFactory.create(metric.getId(), metric.getValue()));

		List<StructField> fields = new ArrayList<>(2);
		fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
		fields.add(DataTypes.createStructField("value", DataTypes.IntegerType, false));
		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
		df.createOrReplaceTempView("metric");
		// perform min sql operation
		Dataset<String> ds = df.groupBy("id").agg(functions.min("value").as("minimum")).map(
									(MapFunction<Row, String>)
									row -> row.get(0) + ", timestamp, min, " + row.get(1),
									Encoders.STRING());
		// collect dataset as list of strings
		List<String> actual = ds.as(Encoders.STRING()).collectAsList();
		// create expected values
		List<String> expected = new ArrayList<String>(2);
		expected.add("1, timestamp, min, 30");
		expected.add("2, timestamp, min, 20");
		// test if objects are equal
		Assert.assertEquals(expected, actual);
	}

	@Test
	// test maximum operation with values
	public void dataFrameMaximumOperation() {
		// create some tests values
		List<Metric> metricList = new ArrayList<>(4);
		Metric metric1 = new Metric();
		metric1.setId(1);
		metric1.setValue(330);
		metricList.add(metric1);
		Metric metric2 = new Metric();
		metric2.setId(1);
		metric2.setValue(53);
		metricList.add(metric2);
		Metric metric3 = new Metric();
		metric3.setId(2);
		metric3.setValue(24);
		metricList.add(metric3);
		Metric metric4 = new Metric();
		metric4.setId(2);
		metric4.setValue(32);
		metricList.add(metric4);

		JavaRDD<Row> rowRDD = jsc.parallelize(metricList).map(
        metric -> RowFactory.create(metric.getId(), metric.getValue()));

		List<StructField> fields = new ArrayList<>(2);
		fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
		fields.add(DataTypes.createStructField("value", DataTypes.IntegerType, false));
		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
		df.createOrReplaceTempView("metric");
		// perform max sql operation
		Dataset<String> ds = df.groupBy("id").agg(functions.max("value").as("maximum")).map(
									(MapFunction<Row, String>)
									row -> row.get(0) + ", timestamp, max, " + row.get(1),
									Encoders.STRING());
		// collect dataset as list of strings
		List<String> actual = ds.as(Encoders.STRING()).collectAsList();
		// create expected values
		List<String> expected = new ArrayList<String>(2);
		expected.add("1, timestamp, max, 330");
		expected.add("2, timestamp, max, 32");
		// test if objects are equal
		Assert.assertEquals(expected, actual);
	}

	@Test
	// test reading from file operation
	public void dataFrameReadFromFile() {
		// create RDD from file src/test/resources/testinput.txt 
		JavaRDD<Metric> metricRDD = spark.read()
			.textFile("src/test/resources/testinput.txt")
			.javaRDD()
			.map(line -> {
				String[] parts = line.split(",");
				Metric metric = new Metric();
				metric.setId(Integer.parseInt(parts[0].trim()));
				metric.setValue(Integer.parseInt(parts[2].trim()));
				return metric;
		});
		// test Metric class as using as schema
		Dataset<Row> metricDF = spark.createDataFrame(metricRDD, Metric.class);
		metricDF.createOrReplaceTempView("metric");
		// collect dataset as list of strings
		List<Row> actual = spark.sql("SELECT * FROM metric").collectAsList();
		// create expected values
		List<Row> expected = new ArrayList<>(5);
		expected.add(RowFactory.create(1, 10));
		expected.add(RowFactory.create(1, 50));
		expected.add(RowFactory.create(1, 50));
		expected.add(RowFactory.create(2, 20));
		expected.add(RowFactory.create(1, 10));
		// test if objects are equal
		Assert.assertEquals(expected, actual);
	}
}
