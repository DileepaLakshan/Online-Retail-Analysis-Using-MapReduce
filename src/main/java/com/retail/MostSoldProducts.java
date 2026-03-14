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

public class MostSoldProducts {

    public static class ProductMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text productDesc = new Text();
        private IntWritable quantity = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Skip header
            if (line.startsWith("InvoiceNo")) return;

            // Split CSV handling commas within quotes
            String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (tokens.length >= 8) {
                try {
                    String descStr = tokens[2].trim();
                    // Remove enclosing quotes if present
                    if (descStr.startsWith("\"") && descStr.endsWith("\"") && descStr.length() > 1) {
                        descStr = descStr.substring(1, descStr.length() - 1);
                    }
                    
                    int qty = Integer.parseInt(tokens[3].trim());
                    
                    // Only consider positive quantities
                    if (descStr.length() > 0 && qty > 0) {
                        productDesc.set(descStr);
                        quantity.set(qty);
                        context.write(productDesc, quantity);
                    }
                } catch (NumberFormatException e) {
                     // Ignore malformed rows
                }
            }
        }
    }

    public static class ProductReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        if (args.length != 2) {
            System.err.println("Usage: MostSoldProducts <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Sold Products");
        job.setJarByClass(MostSoldProducts.class);
        job.setMapperClass(ProductMapper.class);
        job.setCombinerClass(ProductReducer.class);
        job.setReducerClass(ProductReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
