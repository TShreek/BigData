package insurance;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Reducer extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable>{
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException {
      int sum=0;
      while(values.hasNext()){
          sum+=values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }
