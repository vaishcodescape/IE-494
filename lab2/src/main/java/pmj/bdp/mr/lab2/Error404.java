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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Exercise 2.5 — List Timestamp and URL for HTTP 404 responses.
 * Map-only job (no reducer).
 * Output: &lt;Timestamp, URL&gt;
 */
public class Error404 {

    // [12/Dec/2015:19:02:36 +0100] "GET /path HTTP/1.1" 404 239
    private static final Pattern LOG_PATTERN = Pattern.compile(
            "\\[([^\\]]+)\\]\\s+\"(\\S+)\\s+(\\S+)\\s+[^\"]*\"\\s+(404)\\s+\\S+"
    );

    public static class Error404Mapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final Text timestamp = new Text();
        private final Text url = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Matcher m = LOG_PATTERN.matcher(value.toString());
            if (!m.find()) {
                return;
            }

            timestamp.set(m.group(1)); // full timestamp incl. timezone
            url.set(m.group(3));       // requested URL/path
            context.write(timestamp, url);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Error404 <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // Local mode by default (no HDFS/YARN required). Override via -D or cluster conf if needed.
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf, "HTTP 404 Timestamp and URL");
        job.setJarByClass(Error404.class);
        job.setMapperClass(Error404Mapper.class);
        job.setNumReduceTasks(0); // map-only
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
