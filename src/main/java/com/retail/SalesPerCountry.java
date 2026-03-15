package com.retail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesPerCountry {

    public static class SalesMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text country = new Text();
        private DoubleWritable revenue = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Skip header
            if (line.startsWith("InvoiceNo")) {
                return;
            }

            // Split CSV handling commas within quotes
            String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (tokens.length >= 8) {
                try {
                    String countryStr = tokens[7].trim();
                    double quantity = Double.parseDouble(tokens[3].trim());
                    double unitPrice = Double.parseDouble(tokens[5].trim());

                    if (countryStr.length() > 0) {
                        country.set(countryStr);
                        revenue.set(quantity * unitPrice);
                        context.write(country, revenue);
                    }
                } catch (NumberFormatException e) {
                    // Ignore malformed rows (e.g., empty quantity/price)
                }
            }
        }
    }

    public static class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
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

        // OnlineRetail.csv has 541909 data rows + 1 header = 541910 total lines
        // Divide by 3 mappers: ceil(541909 / 3) = 180637 lines per mapper
        // Mapper 1 → lines 1–180637
        // Mapper 2 → lines 180638–361274
        // Mapper 3 → lines 361275–541909
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 180637);

        Job job = Job.getInstance(conf, "Sales Per Country");
        job.setJarByClass(SalesPerCountry.class);
        job.setMapperClass(SalesMapper.class);
        job.setCombinerClass(SalesReducer.class);
        job.setReducerClass(SalesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Use NLineInputFormat to split by line count instead of byte size
        job.setInputFormatClass(NLineInputFormat.class);

        FileInputFormat.addInputPath(job, new Path("OnlineRetail.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output_sales"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}