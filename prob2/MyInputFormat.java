package prob2;

import prob2.MyRecordReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.split.*;
import java.io.IOException;

public class MyInputFormat extends TextInputFormat{


    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
            TaskAttemptContext context) {
        return new MyRecordReader();
    }
}
