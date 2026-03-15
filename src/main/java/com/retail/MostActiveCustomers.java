package com.retail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostActiveCustomers {

    public static class CustomerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text customerId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Skip header
            if (line.startsWith("InvoiceNo")) return;

            // Split CSV handling commas within quotes
            String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (tokens.length >= 8) {
                String customerIdStr = tokens[6].trim();
                
                // Exclude empty and unspecified customer IDs
                if (customerIdStr.length() > 0) {
                    customerId.set(customerIdStr);
                    context.write(customerId, one);
                }
            }
        }
    }

    public static class CustomerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Force Hadoop to use the local filesystem instead of HDFS
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        
        Job job = Job.getInstance(conf, "Most Active Customers");
        job.setJarByClass(MostActiveCustomers.class);
        job.setMapperClass(CustomerMapper.class);
        job.setCombinerClass(CustomerReducer.class);
        job.setReducerClass(CustomerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Hardcode local paths
        FileInputFormat.addInputPath(job, new Path("OnlineRetail.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output_customers"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
