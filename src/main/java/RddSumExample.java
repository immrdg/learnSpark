import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class RddSumExample {
    static {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.ERROR);
    }
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext("local[*]", "RddSumExample");

        // create an RDD with some values
        JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // find the sum of values in the RDD
        int sum = rdd.reduce(Integer::sum);

        // print the sum
        System.out.println("Sum: " + sum);

        sparkContext.stop();
    }
}
