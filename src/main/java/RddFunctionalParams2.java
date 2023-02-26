import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RddFunctionalParams2 {
    static {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.ERROR);
    }
    public static void main(String[] args) {
        String masterUrl="local[*]";
        String appName="RddFunctionalParams";
        try (JavaSparkContext sc = new JavaSparkContext(masterUrl, appName)) {
            JavaRDD<Integer> rdd = sc.parallelize(
                    IntStream.range(1,41).
                            boxed().collect(Collectors.toList()));


            JavaRDD<Integer> odd= rdd.filter(number -> number % 2 == 1);

            System.out.println();

            // let's find sum of those nums
            // way one
            Integer sumByPassingFunction = odd.reduce(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });

            System.out.println(sumByPassingFunction);

            // working of reduce
            // (1 2) 3 4 5
            // (3 3) 4 5
            // (6 4) 5
            // (10 5)
            // 15



            // way two
            Function2<Integer,Integer,Integer> reduceToSum=new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1+v2;
                }
            };

            System.out.println(odd.reduce(reduceToSum));


            // way 1 and 2 are same not much a difference .
            // we just introduced a variable nothing else


            // way 3 -> using lambda
            Function2<Integer,Integer,Integer> reduceToSumLambda = (first,second)->first+second;
            System.out.println(odd.reduce(reduceToSumLambda));

            // way 4 -> Method Reference
            Function2<Integer,Integer,Integer> reduceToSumMethodRef = Integer::sum;
            System.out.println(odd.reduce(reduceToSumMethodRef));



        }
        catch (Exception e){
            System.out.println("Error: "+e.getMessage());
        }
    }
}
