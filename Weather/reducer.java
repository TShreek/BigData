package Weather;

import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter r) throws IOException {
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;

    while (values.hasNext()) {
      double temp = values.next().get();
      min = Math.min(min, temp);
      max = Math.max(max, temp);
    }

    output.collect(new Text("Max temp at " + key.toString()), new DoubleWritable(max));
    output.collect(new Text("Min temp at " + key.toString()), new DoubleWritable(min));
  }
}
