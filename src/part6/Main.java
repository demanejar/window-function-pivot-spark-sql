package part6;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Bai_6").master("local").getOrCreate();
		Dataset<Row> data = spark.read().option("header", true).option("inferSchema", true).csv("src/part6/input.csv");
		
		WindowSpec wins = Window.partitionBy("department").orderBy("id")
				.rowsBetween(Window.unboundedPreceding(), Window.unboundedFollowing());
		Dataset<Row> result_1 = data.withColumn("diff_max", functions.max("salary").over(wins));
		
		result_1.createOrReplaceTempView("data_1");
		Dataset<Row> result = spark.sql("select id, name, department, salary, diff_max - salary as diff from data_1");
		
		result.show();
	}
}

