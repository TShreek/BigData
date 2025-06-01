package wordcount;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException, InterruptedException {
    for(String line : value.toString().split(" "){
      output.collect(new Text(line), new IntWritable(1));
    }
  }
}
