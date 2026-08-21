package pmj.bdp.mr.lab3;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

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
 * Exercise 1 — Monthly moving average of website visitors
 * (First-Time and Returning), following Parsian, Data Algorithms, Ch. 6
 * Example 2 (URL / visit time series).
 *
 * Daily counts are summed into calendar months, then a simple moving
 * average of window size W (default 3) is applied to the monthly series.
 *
 * Output: series \t YYYY-MM \t monthlyTotal \t movingAverage
 */
public class MonthlyMovingAverage {

    public static final String WINDOW_CONF = "moving.average.window.size";

    public static class VisitMapper
            extends Mapper<LongWritable, Text, Text, TimeSeriesData> {

        private final Text series = new Text();
        private final TimeSeriesData point = new TimeSeriesData();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            VisitorRecord rec = VisitorRecord.parse(value.toString());
            if (rec == null) {
                return;
            }

            long ym = rec.yearMonth();

            series.set("FirstTime");
            point.set(ym, rec.firstTimeVisits);
            context.write(series, point);

            series.set("Returning");
            point.set(ym, rec.returningVisits);
            context.write(series, point);
        }
    }

    public static class MovingAverageReducer
            extends Reducer<Text, TimeSeriesData, Text, Text> {

        private int windowSize = 3;
        private final Text out = new Text();

        @Override
        protected void setup(Context context) {
            windowSize = context.getConfiguration().getInt(WINDOW_CONF, 3);
        }

        @Override
        protected void reduce(Text key, Iterable<TimeSeriesData> values,
                              Context context)
                throws IOException, InterruptedException {

            // Sum daily points that fall in the same YYYYMM.
            // Copy each Writable — Hadoop reuses the same object.
            Map<Long, Double> monthly = new TreeMap<>();
            for (TimeSeriesData ts : values) {
                TimeSeriesData copy = TimeSeriesData.copy(ts);
                monthly.merge(copy.getTimestamp(), copy.getValue(), Double::sum);
            }

            long[] months = new long[monthly.size()];
            double[] totals = new double[monthly.size()];
            int n = 0;
            for (Map.Entry<Long, Double> e : monthly.entrySet()) {
                months[n] = e.getKey();
                totals[n] = e.getValue();
                n++;
            }

            if (n < windowSize) {
                return;
            }

            double sum = 0.0;
            for (int i = 0; i < windowSize - 1; i++) {
                sum += totals[i];
            }

            for (int i = windowSize - 1; i < n; i++) {
                sum += totals[i];
                double sma = sum / windowSize;
                out.set(formatYearMonth(months[i])
                        + "\t" + String.format("%.0f", totals[i])
                        + "\t" + String.format("%.2f", sma));
                context.write(key, out);
                sum -= totals[i - windowSize + 1];
            }
        }

        static String formatYearMonth(long yyyymm) {
            long year = yyyymm / 100;
            long month = yyyymm % 100;
            return String.format("%04d-%02d", year, month);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MonthlyMovingAverage <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        if (conf.get(WINDOW_CONF) == null) {
            conf.setInt(WINDOW_CONF, 3);
        }

        Job job = Job.getInstance(conf, "Monthly Moving Average of Website Visitors");
        job.setJarByClass(MonthlyMovingAverage.class);
        job.setMapperClass(VisitMapper.class);
        job.setReducerClass(MovingAverageReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TimeSeriesData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
