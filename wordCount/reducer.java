package wordcount;

import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer< Text, IntWritable, Text, IntWritable>{
  public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException{
    int sum=0;
    while(values.hasNext()){
      sum += values.next().get();
    }
    output.collect(key, new IntWritable(sum));
  }
}
