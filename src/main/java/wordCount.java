import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class wordCount {
    static {
        // set log level to Error
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.ERROR);
    }
    public static void main(String[] args) {
        // spark setup
        String sparkMasterUrl = "local[*]";
        SparkConf conf = new SparkConf().setAppName("SparkJavaExample").setMaster(sparkMasterUrl);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        try (JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {

            // read file
            JavaRDD<String> prideAndPrejudiceRDD=
                    sc.textFile("src/main/resources/data/ebook/PrideAndPrejudice.txt");

            // trim out each line && split into words
            prideAndPrejudiceRDD =
                    prideAndPrejudiceRDD.map(String::trim).  // remove trailing and leading spaces
                                        filter(line->!line.isBlank()). // removes empty lines
                                        flatMap(line->
                                                Arrays.asList(line.split(" ")).iterator());
                                        // flat Map  -> [[1,2,3],[2,4,5]] -> [1,2,3,2,4,5]

            System.out.println(prideAndPrejudiceRDD.take(20)); // Take the first num elements of the RDD.

            // remove punctuations and other special characters
            prideAndPrejudiceRDD =
                    prideAndPrejudiceRDD.map(x->x.replaceAll("[\\p{Punct}—”“’‘]+","")).
                                        filter(x->!x.isBlank()).
                                        filter(x->
                                                !EnglishAnalyzer.ENGLISH_STOP_WORDS_SET.contains(x)
                                                        &&
                                                !StringUtils.isNumeric(x));

            System.out.println(prideAndPrejudiceRDD.take(20));


            // word to k,v pair -> [This, is, my, code]->[(this,1),(is,1),(my,1),(code,1)]
            // Note performing lower as well This -> this
            JavaPairRDD<String,Integer> wordOnePair=
                                        prideAndPrejudiceRDD.mapToPair(x->new Tuple2<>(x.toLowerCase(),1));


            // word count ==> [(this,1),(is,1),(my,1),(code,1),(i,1),(love,1),(to,1),(code,1)]

            // reduce by key .. [(this,1),(is,1),(my,1),(code,2),(i,1),(love,1),(to,1)]

            JavaPairRDD<String,Integer> wordCount = wordOnePair.reduceByKey(Integer::sum);

            // I need to sortByValue but there is no method to sortByValue but there is sortByKey
            System.out.println(wordCount.collect());


            // [(this,1),(is,1),(my,1),(code,2),(i,1),(love,1),(to,1)]
            //          -> [(1,this),(1,is),(1,my),(2,code),(1,i),(1,love),(1,to)]  { swap }
            //          -> [(2,code),(1,this),(1,is),(1,my),(1,i),(1,love),(1,to)]  { sortByKey }
            //          -> [(2,code),(1,this),(1,is),(1,my),(1,i),(1,love),(1,to)]  {swappedBack }
            JavaPairRDD<String,Integer> wordCountSortedByValue =
                    wordCount.mapToPair(Tuple2::swap).// swap key value pairs
                                sortByKey(false). // now value is key as we swapped. we are sorting by key. ascending set to false
                                mapToPair(Tuple2::swap); // we are now swapping back

            System.out.println("\n-----------Top n Records-----------\n");
            wordCountSortedByValue.take(30).forEach(System.out::println);
            System.out.println("\n-----------Sample Records-----------\n");
            wordCountSortedByValue.takeSample(false,30).forEach(System.out::println);


            // total words

            System.out.println("Total Words: "+wordCountSortedByValue.map(Tuple2::_2).reduce(Integer::sum));












        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }

    }
}
