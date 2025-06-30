package weather;

import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Reducer extends MapReduceBase
  implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(
      Text key,
      Iterator<DoubleWritable> values,
      OutputCollector<Text, DoubleWritable> output,
      Reporter r
    ) throws IOException {
      double max=-9999.0,min=9999.0;
      while(values.hasNext()){
        double temp = values.next().get();
        max = Math.max(temp, max);
        min = Math.min(temp, min);
      }
      output.collect(new Text ("Min temp at " + key), new DoubleWritable(min));
      output.collect(new Text("max temp at " + key), new DoubleWritable(max));
    }
  }
