import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.LoggerFactory;

public class RddUnion {
    static {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.ERROR);
    }
    public static void main(String[] args) {
        String masterUrl="local[*]";
        String appName="Rdd Union";
        try (JavaSparkContext sc = new JavaSparkContext(masterUrl, appName)){
            JavaRDD<String> logs = sc.textFile("src/main/resources/app.log");

            JavaRDD<String> debugLogs=logs.filter(x->x.contains("[DEBUG]"));

            System.out.println("----[Debugs]----");
            debugLogs.foreach( line -> System.out.println(line));


            JavaRDD<String> errorLogs=logs.filter(x->x.contains("[ERROR]"));

            System.out.println("----[Error]----");
            errorLogs.foreach(new VoidFunction<String>() {
                @Override
                public void call(String s) throws Exception {
                    System.out.println(s);
                }
            });

            JavaRDD<String> debugAndErrors = debugLogs.union(errorLogs);

            System.out.println("----[Debug and error]----");
            debugAndErrors.foreach( line -> System.out.println(line));

        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
