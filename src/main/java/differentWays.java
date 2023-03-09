import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.stream.StreamSupport;

public class differentWays {
    static {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.ERROR);
    }
    public static void main(String[] args) {
        String masterUrl = "local[*]";
        String appName = "GroupByKey,reduceByKey,aggregateByKey";
        try (JavaSparkContext sc = new JavaSparkContext(masterUrl, appName)) {
            JavaRDD < String > orderDetailRDD =
                    sc.textFile("src/main/resources/data/cmdata/orderdetail.csv");

            String header = orderDetailRDD.first();
            orderDetailRDD = orderDetailRDD.filter(line -> !line.equals(header));

            // let's calculate total purchase amount
            // using reduceByKey method

            JavaPairRDD < String, Double > odRddReduceByKey =
                    orderDetailRDD.
                            mapToPair(line -> {
                                String[] parts = line.split(",");
                                return new Tuple2 < > (parts[1], Integer.parseInt(parts[2]) * Double.parseDouble(parts[3]));
                            }).
                            reduceByKey(Double::sum).
                            mapValues(x -> (double) Math.round(x * 100.0) / 100.0);

            System.out.println("Reduce By key : \n");
            odRddReduceByKey.take(20).forEach(System.out::println);

            // using groupByKey method
            JavaPairRDD < String, Double > odRddGroupByKey = orderDetailRDD.
                    mapToPair(line -> {
                        String[] parts = line.split(",");
                        return new Tuple2 < > (parts[1], Integer.parseInt(parts[2]) * Double.parseDouble(parts[3]));
                    }).
                    groupByKey().
                    mapValues(iterable -> {
                        double sum = StreamSupport.stream(iterable.spliterator(), false).
                                mapToDouble(Double::doubleValue).sum();
                        sum = Math.round(sum * 100.0) / 100.0;
                        return sum;
                    });

            System.out.println("\nGroup By Key: \n");
            odRddGroupByKey.take(10).forEach(System.out::println);

            // method 3 - aggregateByKey

            // let me explain how this works

//            assume this is what the data we need
//            +--------+----------+----------+------+
//            | orderID| productID|  quantity| price|
//            +--------+----------+----------+------+
//            | 1      | A        | 2        | 10.0 |
//            | 1      | B        | 1        | 20.0 |
//            | 2      | A        | 3        | 15.0 |
//            | 2      | B        | 4        | 25.0 |
//            +--------+----------+----------+------+

//             and then we transformed that into key value pairs ( rdd pairs/tuples )
               // key -> productID
               // value -> totalCost
//              this is what you get after map transformation

//            +----------+------------+
//            | productID| totalCost  |
//            +----------+------------+
//            | A        | 20.0       |
//            | B        | 20.0       |
//            | A        | 45.0       |
//            | B        | 100.0      |
//            +----------+------------+


            // the first argument to aggregateByKey is like initial value set before computation

            // when you apply aggregateByKey, the data is partitioned based on keys

//            partition1 -> acc=0.0             partition2 -> acc=0.0
//            +----------+------------+         +----------+------------+
//            | productID| totalCost  |         | productID| totalCost  |
//            +----------+------------+         +----------+------------+
//            | A        | 20.0       |         | B        | 20.0       |
//            | A        | 45.0       |         | B        | 100.0      |
//            +----------+------------+         +----------+------------+
//                                          ┃
//                                          ┃  (acc,val) -> acc+val
//                                          ▼
//                                 ┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉
//                                 ┋ acc -> acc+value ┋
//                                 ┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉┉
//                          acc = 0.0 (first arg to aggregateByKey method
//              acc = 0.0 + 20.0 -> 20.0    〢      acc = 0.0 + 20.0 -> 20.0
//              acc = 20.0 + 45.0 -> 65.0   〢      acc = 20.0 + 100.0 -> 120.0
//              result-> ( A, 65.0 )        〢      result-> ( B, 120.0 )
//                                          ┃
//                                          ┃  (acc1,acc2)-> acc1+acc2
//                                          ▼
//              final out come -> [ ( A, 65.0 ), ( B, 120.0 ) ]

//            +----------+------------+
//            | productID| totalCost  |
//            +----------+------------+
//            | A        | 65.0       |
//            | B        | 120.0      |
//            +----------+------------+

            // first argument is initial value of accumulator
            // for every key the value of the accumulator is set
            //

            JavaPairRDD<String, Double> stringDoubleJavaPairRDD = orderDetailRDD.mapToPair(line -> {
                String[] parts = line.split(",");
                return new Tuple2<>(parts[1], Integer.parseInt(parts[2]) * Double.parseDouble(parts[3]));
            }).aggregateByKey(0.0, (acc, val) -> acc + val, (acc1, acc2) -> acc1 + acc2).
            mapValues(x -> (double) Math.round(x * 100.0) / 100.0);;


            System.out.println("\n Aggregate By Key : \n");
            stringDoubleJavaPairRDD.take(10).forEach(System.out::println);


        } catch (Exception w) {
            System.out.println(w.getMessage());
        }
    }
}