import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;

public class ReadTextFile implements Serializable {
    static {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.ERROR);
    }
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext("local[*]", "read text file");

        // create an RDD with some values
        JavaRDD<String> rdd = sparkContext.textFile("src/main/resources/README.md");

        rdd.foreach(line-> System.out.println(line));
        // rdd.foreach(System.out::println) won't work

        SparkSession spark = SparkSession.builder().config(sparkContext.getConf()).getOrCreate();
        Dataset<Row> customers = spark.read().
                option("header","true").
                option("inferSchema","true").
                options(new HashMap<>()).
                csv("src/main/resources/data/cmdata/customer.csv");

        customers.show();

        customers.filter(customers.col("contactFirstName").startsWith("S")).show();

        customers.printSchema();

        sparkContext.stop();
    }
}


