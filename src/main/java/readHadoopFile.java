import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.LoggerFactory;

public class readHadoopFile {
    static {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.ERROR);
    }
    public static void main(String[] args) {

        String masterUrl = "local[*]";
        String appName = "GroupByKey,reduceByKey,aggregateByKey";
        try (JavaSparkContext sc = new JavaSparkContext(masterUrl, appName)){
            JavaRDD<String> rdd= sc.textFile("hdfs://localhost:8020/example.txt");
            rdd=rdd.filter(line->!(line.isEmpty() || line.isBlank()));
            //rdd.saveAsTextFile("hdfs://localhost:8020/output");

            rdd.coalesce(1).saveAsTextFile("hdfs://localhost:8020/plOut");
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }


    }
}
