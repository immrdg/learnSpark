import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class orderDetail {
    static {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.ERROR);
    }
    public static void main(String[] args) {

        String masterUrl="local[*]";
        String appName="Working with orderDetail.csv";
        try (JavaSparkContext sc = new JavaSparkContext(masterUrl, appName)){
            JavaRDD<String> orderDetailRDD = sc.textFile("src/main/resources/data/cmdata/orderdetail.csv");

            System.out.println(orderDetailRDD.take(2)); // display first num records

            // as you can see First record is headers of orderdetail.csv file . let's remove it out from rdd

            String header = orderDetailRDD.first();

            orderDetailRDD = orderDetailRDD.filter(line -> !line.equals(header));

            System.out.println(orderDetailRDD.take(2));

            // let's find out total purchase amount for each product

            // let's display header now

            System.out.println(header);

            // output -> orderNumber,productCode,quantityOrdered,priceEach,orderLineNumber
            // now for each product code we need to find out total purchase
            // first we need to compute total amount for each order which is quantityOrdered * priceEach
            //orderNumber productCode  quantityOrdered    priceEach
            // 1            p1               10               100
            // 2            p2               20                10
            // 3            p1               9                100
            // 4            p1               20               100
            // 5            p2               100               10

            // sample data
            // first we need to compute productCode * quantity as total price of an order

            //orderNumber productCode    orderPrice
            // 1            p1               1000
            // 2            p2               200
            // 3            p1               900
            // 4            p1               2000
            // 5            p2               1000

            // now let's perform this step
            // headers -> orderNumber,productCode,quantityOrdered,priceEach,orderLineNumber
            // only productCode,quantityOrdered,priceEach are needed..1,2,3 indexed columns
            JavaPairRDD<String, Double> orderPrice = orderDetailRDD.mapToPair(line -> {
                String[] parts = line.split(",");
                return new Tuple2<>(parts[1], Integer.parseInt(parts[2]) * Double.parseDouble(parts[3]));
            });
            System.out.println(orderPrice.collect());


            // now that we have order price for each order
            // lets find out total purchase amount for each product

            //  productCode    orderPrice
            //  p1               1000
            //  p2               200
            //  p1               900
            //  p1               2000
            //  p2               1000

            // to

            // productCode    totalPurchaseAmount
            // p1              3900 [ 1000+900+2000]
            // p2              1200 [ 200 + 1000 ]

            JavaPairRDD<String, Double> productTotalPurchaseAmount = orderPrice.reduceByKey(Double::sum);
            productTotalPurchaseAmount.take(10).forEach(System.out::println);

            // now that you have performed this step look at those values the look something like
            // 54024.87000000001 there are 14 digits to the right of the decimal point,
            // which means that the precision count is 14
            // lets round it off

            System.out.println("-------");

            productTotalPurchaseAmount = productTotalPurchaseAmount.mapValues(x-> (double) Math.round(x*100.0)/100.0);
            productTotalPurchaseAmount.take(10).forEach(System.out::println);



            // so if you want to find top 5 products we can use sortByKey,
            // but we need to swap and re-swap things

            productTotalPurchaseAmount = productTotalPurchaseAmount.
                    mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)).
                    sortByKey(false).
                    mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

            System.out.println("------ Top 10 ----------");
            productTotalPurchaseAmount.take(10).forEach(System.out::println);

        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
