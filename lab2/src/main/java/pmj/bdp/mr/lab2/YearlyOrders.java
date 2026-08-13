package pmj.bdp.mr.lab2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Exercise 1.3 — Year wise order count and sum of order amount.
 * Year is extracted from OrderDate. Amount = Order Quantity * Unit Price.
 * Output: &lt;Year, OrderCount, SumAmount&gt;
 */
public class YearlyOrders {

    public static class YearMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final Text year = new Text();
        private final Text countAndAmount = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            OrderRecord order = OrderRecord.parse(value.toString());
            if (order == null) {
                return;
            }
            year.set(order.year());
            countAndAmount.set("1," + order.orderAmount());
            context.write(year, countAndAmount);
        }
    }

    public static class YearReducer
            extends Reducer<Text, Text, Text, Text> {

        private final Text out = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long count = 0;
            double amount = 0.0;

            for (Text v : values) {
                String[] parts = v.toString().split(",", 2);
                count += Long.parseLong(parts[0]);
                amount += Double.parseDouble(parts[1]);
            }

            out.set(count + "\t" + String.format("%.2f", amount));
            context.write(key, out);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: YearlyOrders <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // Local mode by default (no HDFS/YARN required). Override via -D or cluster conf if needed.
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf, "Yearly Order Count and Amount");
        job.setJarByClass(YearlyOrders.class);
        job.setMapperClass(YearMapper.class);
        job.setReducerClass(YearReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
