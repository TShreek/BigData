package employee;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Mapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable>{
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter r){
      String[] line = value.toString().split("\\t");
      double salary = Double.parseDouble(line[8]);
      output.collect(new Text(line[3], new DoubleWritable(salary));
    }
  }
