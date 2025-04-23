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
        // if (args.length < 1) {
        //     System.err.println("Usage: DistributedMergeSort <file>");
        //     System.exit(1);
        // }

        //initialize spark
        SparkConf conf = new SparkConf().setAppName("DistributedMergeSort");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long startTime = System.currentTimeMillis(); 
        
        // //create random data
        // List<Integer> randomNumbers = newArrayList<>();
        // Random rand = new Random();
        // int numItems = 100; 
        // for (int i = 0; i < numItems; i++){
        //     randomNumbers.add(rand.nextInt(1_000_000));
        // }

        //data from file instead of random data
        String inputFile = "file:///Users/michellelu/mergeSort/src/main/java/unsortedNum.txt";

        //RDD
        
        //JavaRDD<String> data = sc.parallelize(randomNumbers, numPartitions);
        //read file
        JavaRDD<String> stringData = sc.textFile(inputFile);
        JavaRDD<Integer> data = stringData.map(line -> Integer.parseInt(line.trim()));

        int numPartitions = 8;
        data = data.repartition(numPartitions);

        // combining results from all partitions
        long combiningPartitionStartTime = System.currentTimeMillis();
        JavaRDD<Integer> combineSortedPartitions = data.sortBy(x -> x, true, numPartitions);
        long combiningPartitionEndTime = System.currentTimeMillis();
        System.out.println("time to combine all partitions: " + (combiningPartitionEndTime - combiningPartitionStartTime) + " ms");

        // Collect and print the first 50 sorted numbers
        List<Integer> sorted = combineSortedPartitions.take(50);
        System.out.println("First 50 sorted numbers:");
        for (int i = 0; i < sorted.size(); i++) {
            System.out.println(sorted.get(i));
        }
        
        long endTime = System.currentTimeMillis();
        System.out.println("Total runtime: " + (endTime - startTime) + " ms");


        //done
        sc.stop();
    }
}
