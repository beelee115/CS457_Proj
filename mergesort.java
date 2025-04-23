//distributed merge sort
// Spark core classes
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

// Java utility classes
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

// Spark Pair class
import scala.Tuple2;

public class mergeSort {
    public static void main(String[] args) throws Exception{

        //initialize spark
        SparkConf conf = new SparkConf().setAppName("DistributedMergeSort");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        //data from file instead of random data
        String inputFile = "file:///Users/michellelu/mergeSort/src/main/java/unsortedNum.txt";

        //RDD
        //read file
        JavaRDD<String> stringData = sc.textFile(inputFile);
        JavaRDD<Integer> data = stringData.map(line -> Integer.parseInt(line.trim()));

        // partitioning, sorting, and merging
        int numPartitions = 8;
        long startTime = System.currentTimeMillis();
        data = data.repartition(numPartitions);
        JavaRDD<Integer> combineSortedPartitions = data.sortBy(x -> x, true, numPartitions);
        long endTime = System.currentTimeMillis();
    
        // print result
        List<Integer> sorted = combineSortedPartitions.take(50);
        System.out.println("First 50 sorted numbers:");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sorted.size(); i++) {
            sb.append(sorted.get(i));
            if (i < sorted.size() - 1) {
                sb.append(", ");
            }
        }
        System.out.println(sb.toString());
        
        System.out.println("Total runtime: " + (endTime - startTime) + " ms");

        //done
        sc.stop();
    }
}
