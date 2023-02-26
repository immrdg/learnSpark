import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RddFunctionalParams1 {
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

            Function<Integer,Boolean> checkEvenPredicateFunction=new Function<Integer, Boolean>() {
                @Override
                public Boolean call(Integer v1) throws Exception {
                    return v1%2==0;
                }
            };


            Function<Integer,Boolean> checkEvenLambdaFunction=v1->v1%2==0;

            // Both checkEvenPredicateFunction & checkEvenLambdaFunction are same



            System.out.println(rdd.filter(checkEvenPredicateFunction).collect());

            System.out.println(rdd.filter(checkEvenLambdaFunction).collect());


            // this also do the same
            System.out.println(rdd.filter(number->number%2==0).collect());

        }
        catch (Exception e){
            System.out.println("Error: "+e.getMessage());
        }
    }
}
