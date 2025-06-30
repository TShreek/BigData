package insurance;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Mapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable>{
    public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter r) throws IOException {
      String[] line = value.toString().split(",");
      String name = line[2];
      output.collect(new Text(name), new IntWritable(1));
    }
  }
