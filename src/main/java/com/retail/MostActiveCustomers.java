package com.retail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostActiveCustomers {

   
    // JOB 1 - MAPPER: reads CSV, emits (customerID, 1)
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

  
    // JOB 1 - REDUCER: counts total transactions per customer
    public static class CustomerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    
    // JOB 2 - MAPPER: flips (customerID, count) → (count, customerID)
    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Job 1 output format is: "CustomerID\tCount"
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                try {
                    String customer = parts[0].trim();
                    int count = Integer.parseInt(parts[1].trim());
                    // Emit (count, customerID) so sorting happens on count
                    context.write(new IntWritable(count), new Text(customer));
                } catch (NumberFormatException e) {
                    // Ignore malformed lines
                }
            }
        }
    }

   
    // JOB 2 - REDUCER: flips back to (customerID, count) for output
    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Multiple customers can have the same transaction count
            for (Text customer : values) {
                context.write(customer, key);
            }
        }
    }

    // DESCENDING SORT COMPARATOR
    // By default Hadoop sorts keys ascending — this reverses it
    public static class DescendingComparator extends WritableComparator {
        public DescendingComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            // Reverse the natural order: higher count comes first
            return -super.compare(a, b);
        }
    }

   
    // MAIN: runs Job 1 then Job 2
    public static void main(String[] args) throws Exception {

        // ── JOB 1: Count total transactions per customer 
        Configuration conf1 = new Configuration();
        conf1.set("fs.defaultFS", "file:///");
        conf1.set("mapreduce.framework.name", "local");

        // OnlineRetail.csv has 541909 data rows
        // Divide by 3 mappers: ceil(541909 / 3) = 180637 lines per mapper
        // Mapper 1 → lines 1–180637
        // Mapper 2 → lines 180638–361274
        // Mapper 3 → lines 361275–541909
        conf1.setInt(NLineInputFormat.LINES_PER_MAP, 180637);

        Job job1 = Job.getInstance(conf1, "Most Active Customers - Aggregate");
        job1.setJarByClass(MostActiveCustomers.class);
        job1.setMapperClass(CustomerMapper.class);
        job1.setCombinerClass(CustomerReducer.class);
        job1.setReducerClass(CustomerReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // NLineInputFormat splits file by line count → 3 mappers
        job1.setInputFormatClass(NLineInputFormat.class);

        FileInputFormat.addInputPath(job1, new Path("OnlineRetail.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("output_customers_temp"));

        // Wait for Job 1 to finish before starting Job 2
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // ── JOB 2: Sort by transaction count descending 
        Configuration conf2 = new Configuration();
        conf2.set("fs.defaultFS", "file:///");
        conf2.set("mapreduce.framework.name", "local");

        Job job2 = Job.getInstance(conf2, "Most Active Customers - Sort");
        job2.setJarByClass(MostActiveCustomers.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);

        // Tell Hadoop to use descending comparator when sorting keys (counts)
        job2.setSortComparatorClass(DescendingComparator.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // Job 2 reads from Job 1's output
        FileInputFormat.addInputPath(job2, new Path("output_customers_temp"));
        FileOutputFormat.setOutputPath(job2, new Path("output_customers"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
