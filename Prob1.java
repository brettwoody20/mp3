//package problem.one;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Prob1 {
    public static class TweetMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
        private static final Log log = LogFactory.getLog(TweetMapper.class);
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.length() > 0 && line.charAt(0) == 'T') {
                try {
                    String timestampString = line.substring(1, line.length());
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(dateFormat.parse(timestampString));
                    int hour = cal.get(Calendar.HOUR_OF_DAY);

                    context.write(new Text(String.valueOf(hour)), new IntWritable(1));
                } catch (ParseException e) {

                }
            }
        }
    }

    public static class TweetReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Time of Day Most Often Tweets");
        job.setJarByClass(Prob1.class);
        job.setMapperClass(TweetMapper.class);
        job.setReducerClass(TweetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
