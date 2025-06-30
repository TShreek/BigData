package oddEven;

import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Reduceer
  extends MapReduceBase
  implements Reducer <Text, IntWritable, Text, IntWritable>{
    public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output, Reporter r){
      int sum=0,count=0;
      while(value.hasNext()){
        sum+= value.next().get();
        count++;
      }
      output.collect(new Text("The sum of all "+key+"numbers is "), new IntWritable(sum));
      output.collect(new Text("number of " + key + " elements is "), new IntWritable(count)); 
    }
  }
