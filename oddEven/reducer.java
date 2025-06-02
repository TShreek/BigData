package oddEven;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
  public void reduce(Text key, Iterator<IntWritable> value, outputCollector<Text, IntWritable> output,Reporter r ) throws IOException{
    int sum=0,count=0;
    while(value.hasNext()){
      sum+=value.next().get();
      count++;
    }
    output.collect(new Text("sum of "+key+ "numbers :"), new IntWritable(sum)));
    output.collect(new Text("count of " + key + " numbers : "), new IntWritable(count))); 
  }
}
