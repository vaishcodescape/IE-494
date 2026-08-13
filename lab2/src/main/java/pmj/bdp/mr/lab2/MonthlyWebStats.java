package pmj.bdp.mr.lab2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * Exercise 2.4 — Monthly request count and download size (MB).
 * Output: &lt;Year-Month, NumberOfRequests, DownloadSizeMB&gt;
 * e.g. Dec-2015, Jan-2016, ...
 */
public class MonthlyWebStats {

    // Apache Combined / Common Log Format timestamp + status + size
    // [12/Dec/2015:18:25:11 +0100] "GET /path HTTP/1.1" 200 4263
    private static final Pattern LOG_PATTERN = Pattern.compile(
            "\\[\\d{2}/(\\w{3})/(\\d{4}):[^\\]]+\\]\\s+\"[^\"]*\"\\s+\\d{3}\\s+(\\S+)"
    );

    public static class MonthlyMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final Text monthKey = new Text();
        private final Text countAndBytes = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Matcher m = LOG_PATTERN.matcher(value.toString());
            if (!m.find()) {
                return;
            }

            String month = m.group(1); // Dec
            String year = m.group(2);  // 2015
            String sizeTok = m.group(3);

            long bytes = 0L;
            if (!"-".equals(sizeTok)) {
                try {
                    bytes = Long.parseLong(sizeTok);
                } catch (NumberFormatException e) {
                    bytes = 0L;
                }
            }

            monthKey.set(month + "-" + year);
            // value: requestCount,bytes
            countAndBytes.set("1," + bytes);
            context.write(monthKey, countAndBytes);
        }
    }

    public static class MonthlyReducer
            extends Reducer<Text, Text, Text, Text> {

        private final Text out = new Text();
        private static final double BYTES_PER_MB = 1024.0 * 1024.0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long requests = 0;
            long bytes = 0;

            for (Text v : values) {
                String[] parts = v.toString().split(",", 2);
                requests += Long.parseLong(parts[0]);
                bytes += Long.parseLong(parts[1]);
            }

            double mb = bytes / BYTES_PER_MB;
            out.set(requests + "\t" + String.format("%.2f", mb));
            context.write(key, out);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MonthlyWebStats <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // Local mode by default (no HDFS/YARN required). Override via -D or cluster conf if needed.
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf, "Monthly Web Request Stats");
        job.setJarByClass(MonthlyWebStats.class);
        job.setMapperClass(MonthlyMapper.class);
        job.setReducerClass(MonthlyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
