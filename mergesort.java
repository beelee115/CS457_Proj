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

public static void main(String[] args) throws Exception{
    // if (args.length < 1) {
    //     System.err.println("Usage: DistributedMergeSort <file>");
    //     System.exit(1);
    // }

    //initialize spark
    SparkConf conf = new SparkConf().setAppName("DistributedMergeSort");
    JavaSparkContext sc = new JavaSparkContext(conf);

    //create random data
    List<Integer> randomNumbers = newArrayList<>();
    Random rand = new Random();
    int numItems = 100; 
    for (int i = 0; i < numItems; i++){
        randomNumbers.add(rand.nextInt(1_000_000));
    }
    //RDD
    int numPartitions = 8;
    JavaRDD<String> data = sc.parallelize(randomNumbers, numPartitions);

    
    //pass eahc element through a function func map(func) ?? maybe not
        
        //data partitionned and sorted by different nodes  map.Partitions????
        //slpit data into chunks to be sorted in parallel
            //how to split data
        //data randomly created ? not sure

    //merge results

    // sort within each partition 
    JavaRDD<Integer> sortedInPartition = rdd.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) iter -> {
            List<Integer> list = new ArrayList<>();
            iter.forEachRemaining(e -> list.add(e));
            Collections.sort(list);
            return list.iterator(); 
        });

     // combining results from all partitions
        JavaRDD<Integer> combineSortedPartitions = sortedInPartition.sortBy(x -> x, true, numPartitions);

    //done
    sc.stop();
}


