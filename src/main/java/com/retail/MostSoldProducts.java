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

public class MostSoldProducts {

    // -------------------------------------------------------
    // JOB 1 - MAPPER: reads CSV, emits (product, quantity)
    // -------------------------------------------------------
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

                    // Only consider positive quantities (ignoring returns/cancellations)
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

    // -------------------------------------------------------
    // JOB 1 - REDUCER: sums quantities per product
    // -------------------------------------------------------
    public static class ProductReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    // -------------------------------------------------------
    // JOB 2 - MAPPER: flips (product, quantity) → (quantity, product)
    // so Hadoop can sort by quantity
    // -------------------------------------------------------
    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Job 1 output format is: "ProductName\tQuantity"
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                try {
                    String product = parts[0].trim();
                    int qty = Integer.parseInt(parts[1].trim());
                    // Emit (quantity, product) so sorting happens on quantity
                    context.write(new IntWritable(qty), new Text(product));
                } catch (NumberFormatException e) {
                    // Ignore malformed lines
                }
            }
        }
    }

    // -------------------------------------------------------
    // JOB 2 - REDUCER: flips back to (product, quantity) for output
    // -------------------------------------------------------
    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Multiple products can have the same quantity
            for (Text product : values) {
                context.write(product, key);
            }
        }
    }

    // -------------------------------------------------------
    // DESCENDING SORT COMPARATOR
    // By default Hadoop sorts keys ascending — this reverses it
    // -------------------------------------------------------
    public static class DescendingComparator extends WritableComparator {
        public DescendingComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            // Reverse the natural order: higher quantity comes first
            return -super.compare(a, b);
        }
    }

    // -------------------------------------------------------
    // MAIN: runs Job 1 then Job 2
    // -------------------------------------------------------
    public static void main(String[] args) throws Exception {

        // ── JOB 1: Aggregate total quantity per product ──────────────
        Configuration conf1 = new Configuration();
        conf1.set("fs.defaultFS", "file:///");
        conf1.set("mapreduce.framework.name", "local");

        // OnlineRetail.csv has 541909 data rows
        // Divide by 3 mappers: ceil(541909 / 3) = 180637 lines per mapper
        // Mapper 1 → lines 1–180637
        // Mapper 2 → lines 180638–361274
        // Mapper 3 → lines 361275–541909
        conf1.setInt(NLineInputFormat.LINES_PER_MAP, 180637);

        Job job1 = Job.getInstance(conf1, "Most Sold Products - Aggregate");
        job1.setJarByClass(MostSoldProducts.class);
        job1.setMapperClass(ProductMapper.class);
        job1.setCombinerClass(ProductReducer.class);
        job1.setReducerClass(ProductReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // NLineInputFormat splits file by line count → 3 mappers
        job1.setInputFormatClass(NLineInputFormat.class);

        FileInputFormat.addInputPath(job1, new Path("OnlineRetail.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("output_products_temp"));

        // Wait for Job 1 to finish before starting Job 2
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // ── JOB 2: Sort by quantity descending ───────────────────────
        Configuration conf2 = new Configuration();
        conf2.set("fs.defaultFS", "file:///");
        conf2.set("mapreduce.framework.name", "local");

        Job job2 = Job.getInstance(conf2, "Most Sold Products - Sort");
        job2.setJarByClass(MostSoldProducts.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);

        // Tell Hadoop to use descending comparator when sorting keys (quantities)
        job2.setSortComparatorClass(DescendingComparator.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // Job 2 reads from Job 1's output
        FileInputFormat.addInputPath(job2, new Path("output_products_temp"));
        FileOutputFormat.setOutputPath(job2, new Path("output_products"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
