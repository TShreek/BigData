package Weather;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;

public class driver{
  public static void main(String args[]) throws Exception{
    JobConf conf = new JobConf(driver.class);

    conf.setMapperClass(mapper.class);
    conf.setReducerClass(reducer.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.addOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
