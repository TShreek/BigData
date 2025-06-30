pacakge oddEven

import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper 
  extends MapReduceBase 
  implements Mapper 
  <LongWritable, 
  Text, 
  Text, 
  IntWritable> {
  public void map(
    LongWritable key, 
    Text value, 
    OutputCollector<Text, IntWritable> output, 
    Reporter r){
    
    String[] line = value.toString().split(" ");
    for(String str : line){
      int num = Interger.parseInt(str);
      if(num % 2 == 0){
        output.collect(new Text("even"),new IntWritable(num));
      }
      else{
        output.collect(new Text("odd"),new IntWritable(num));
      }
    }
    
  }
}
