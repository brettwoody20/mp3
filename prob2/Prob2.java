package prob2;

import prob2.MyInputFormat;
import prob2.MyRecordReader;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import java.util.Locale;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Prob2 {
    public static class arrayMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);


	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	    	String line = value.toString();
	    	String[] lines = line.split("\n");

			//context.write(new Text(String.valueOf(lines.length)), new IntWritable(1));
			// if (lines.length == 4) {
			// 	String wi = "-" + lines[3].charAt(0) + "-";
			// 	context.write(new Text(lines[1]), new IntWritable(1));
			// }
			// if (lines[2].charAt(0) == 'W') {
			// 	context.write(new Text("1"), new IntWritable(1));
			// } else {
			// 	context.write(new Text("0"), new IntWritable(1));
			// }

	    	
            if (lines.length == 4 && lines[3].toLowerCase().contains("sleep")) {
                try {
                    String timestampString = lines[1].substring(1, lines[1].length());
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(dateFormat.parse(timestampString));
                    int hour = cal.get(Calendar.HOUR_OF_DAY);

                    context.write(new Text(String.valueOf(hour)), new IntWritable(1));
                } catch (ParseException e) {

                }
            }
	      
	    }
	  }

	  public static class arrayReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

	        int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
		  
	    }
	  }

	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Prob 2");
		
	    // set jar by finding where the given class name came from 
	    job.setJarByClass(Prob2.class);
		
		//set custom reader here
	    job.setInputFormatClass(MyInputFormat.class);

	    //set the mapper, reducer, and the combiner 
	    //with the above classes we defined
	    job.setMapperClass(arrayMapper.class);
	    job.setReducerClass(arrayReducer.class);

	    //set the data type for the key in the output
	    job.setOutputKeyClass(Text.class);

	    //set the data type for the value in the output
	    job.setOutputValueClass(IntWritable.class);

	    //provide the location of the input and output directory 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
    
}
