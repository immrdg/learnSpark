import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.stream.StreamSupport;

public class groupByExample {
    static {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.ERROR);
    }
    public static void main(String[] args) {
        String masterUrl="local[*]";
        String appName="GroupBy Example";
        try (JavaSparkContext sc = new JavaSparkContext(masterUrl, appName)){
            JavaRDD<String> orderDetailRDD =
                    sc.textFile("src/main/resources/data/cmdata/orderdetail.csv");
            System.out.println(orderDetailRDD.take(2)); // display first num records

            // as you can see First record is headers of orderdetail.csv file . let's remove it out from rdd

            String header = orderDetailRDD.first();

            orderDetailRDD = orderDetailRDD.filter(line -> !line.equals(header));

            System.out.println();
            System.out.println(header);
            orderDetailRDD.take(2).forEach(System.out::println);


            // check orderDetail.java file

            JavaPairRDD<String, Double> orderPrice = orderDetailRDD.mapToPair(line -> {
                String[] parts = line.split(",");
                return new Tuple2<>(parts[1], Integer.parseInt(parts[2]) * Double.parseDouble(parts[3]));
            });

            // we have used reduceByKey earlier

            // let's see how groupByKey works


            //  productCode    orderPrice
            //  p1               1000
            //  p2               200
            //  p1               900
            //  p1               2000
            //  p2               1000


            // when you perform groupByKey

            // p1 -> Iterable<> [1000, 900, 2000]
            // p2 -> Iterable<> [200, 1000]

            // look at the console it'll display string and Iterable
            orderPrice.groupByKey().take(2).forEach(System.out::println);


            // now that we have Iterable we need to find sum.
            // I wanted to use streams, so I came up with this .
            // you can use your own logic

            orderPrice.groupByKey().mapValues(x-> {
                double sum = StreamSupport.stream(x.spliterator(), false).
                        mapToDouble(Double::doubleValue).sum();
                sum = Math.round(sum*100.0)/100.0;
                return sum;}
            ).take(10).forEach(System.out::println);

            // if you need you can sort
        }

    }
}
