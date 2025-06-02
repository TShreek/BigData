package Sales;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException {
    String[] line = value.toString().split(",");
    
    int price = Integer.parseInt(line[2]);
    String cardtype = line[3];
    String country = line[7];

    output.collect(new Text(country), new IntWritable(price));   // Total sales per country
    output.collect(new Text(cardtype), new IntWritable(1));      // Count of card types used
  }
}
