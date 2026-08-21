package pmj.bdp.mr.lab3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Exercise 2 — MR-Apriori frequent-itemset mining
 * (Lin, "MR-Apriori: Association Rules Algorithm Based on MapReduce", 2014).
 *
 * One MapReduce job per itemset size k:
 *   k = 1  count items; keep those with support &gt;= minsup
 *   k &gt;= 2  Apriori-gen C_k from L_{k-1}, count candidates, keep frequent
 *
 * Singleton itemsets are computed but not written to the final listing.
 *
 * Usage: MRApriori &lt;transactions&gt; &lt;outputDir&gt; &lt;minSupport&gt;
 */
public class MRApriori {

    public static final String CANDIDATES_PATH = "apriori.candidates.path";
    public static final String MIN_SUPPORT = "apriori.minsupport";
    public static final String ITEMSET_K = "apriori.k";

    public static class CandidateCountMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text outKey = new Text();
        private int k;
        private List<int[]> candidates = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            k = conf.getInt(ITEMSET_K, 1);
            if (k >= 2) {
                String path = conf.get(CANDIDATES_PATH);
                candidates = readItemsets(path);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            Set<Integer> txn = parseTransaction(line);
            if (txn.isEmpty()) {
                return;
            }

            if (k == 1) {
                for (int item : txn) {
                    outKey.set(Integer.toString(item));
                    context.write(outKey, ONE);
                }
                return;
            }

            for (int[] cand : candidates) {
                if (Itemset.containsAll(txn, cand)) {
                    outKey.set(Itemset.toKey(cand));
                    context.write(outKey, ONE);
                }
            }
        }

        static Set<Integer> parseTransaction(String line) {
            Set<Integer> items = new HashSet<>();
            for (String tok : line.split("\\s+")) {
                if (tok.isEmpty()) {
                    continue;
                }
                items.add(Integer.parseInt(tok));
            }
            return items;
        }
    }

    /** Local sum — same types as the reducer, no minsup filter. */
    public static class SumCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable out = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            out.set(sum);
            context.write(key, out);
        }
    }

    public static class SupportReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int minSupport;
        private final IntWritable out = new IntWritable();

        @Override
        protected void setup(Context context) {
            minSupport = context.getConfiguration().getInt(MIN_SUPPORT, 1);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            if (sum >= minSupport) {
                out.set(sum);
                context.write(key, out);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MRApriori <transactions> <outputDir> <minSupport>");
            System.exit(-1);
        }

        Path transactions = new Path(args[0]);
        File outRoot = new File(args[1]);
        int minSupport = Integer.parseInt(args[2]);

        if (outRoot.exists()) {
            System.err.println("Output directory already exists: " + outRoot);
            System.exit(-1);
        }
        if (!outRoot.mkdirs()) {
            System.err.println("Could not create " + outRoot);
            System.exit(-1);
        }

        File work = new File(outRoot, "_work");
        work.mkdirs();

        List<int[]> previous = new ArrayList<>();
        File combined = new File(outRoot, "frequent-itemsets.txt");

        try (BufferedWriter listing = new BufferedWriter(new FileWriter(combined))) {
            listing.write("# Frequent itemsets (size >= 2), minSupport=" + minSupport);
            listing.newLine();

            for (int k = 1; ; k++) {
                File candidatesFile = new File(work, "C" + k + ".txt");
                if (k >= 2) {
                    List<int[]> ck = Itemset.generateCandidates(previous);
                    System.out.println("k=" + k + " |L" + (k - 1) + "|=" + previous.size()
                            + " |C" + k + "|=" + ck.size());
                    if (ck.isEmpty()) {
                        break;
                    }
                    writeItemsets(candidatesFile, ck);
                }

                Path jobOut = new Path(new File(outRoot, "L" + k).getAbsolutePath());
                boolean ok = runCountJob(transactions, jobOut, k, minSupport,
                        k >= 2 ? candidatesFile.getAbsolutePath() : null);
                if (!ok) {
                    System.err.println("Job for k=" + k + " failed");
                    System.exit(1);
                }

                List<int[]> lk = readFrequentFromJob(new File(outRoot, "L" + k));
                System.out.println("k=" + k + " |L" + k + "|=" + lk.size());
                if (lk.isEmpty()) {
                    break;
                }

                if (k >= 2) {
                    listing.write("# size " + k);
                    listing.newLine();
                    appendJobOutput(new File(outRoot, "L" + k), listing);
                    listing.newLine();
                }

                previous = lk;
            }
        }

        System.out.println("Frequent itemsets (k>=2) written to " + combined.getAbsolutePath());
    }

    private static boolean runCountJob(Path input, Path output, int k, int minSupport,
                                       String candidatesPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt(MIN_SUPPORT, minSupport);
        conf.setInt(ITEMSET_K, k);
        if (candidatesPath != null) {
            conf.set(CANDIDATES_PATH, candidatesPath);
        }

        Job job = Job.getInstance(conf, "MR-Apriori k=" + k);
        job.setJarByClass(MRApriori.class);
        job.setMapperClass(CandidateCountMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(SupportReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job.waitForCompletion(true);
    }

    static List<int[]> readItemsets(String path) throws IOException {
        List<int[]> list = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                list.add(Itemset.parse(line));
            }
        }
        return list;
    }

    static void writeItemsets(File file, List<int[]> itemsets) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            for (int[] items : itemsets) {
                bw.write(Itemset.toKey(items));
                bw.newLine();
            }
        }
    }

    static List<int[]> readFrequentFromJob(File jobOutDir) throws IOException {
        List<int[]> list = new ArrayList<>();
        File[] parts = jobOutDir.listFiles((dir, name) -> name.startsWith("part-"));
        if (parts == null) {
            return list;
        }
        for (File part : parts) {
            try (BufferedReader br = new BufferedReader(new FileReader(part))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] kv = line.split("\\t", 2);
                    if (kv.length == 0 || kv[0].isBlank()) {
                        continue;
                    }
                    list.add(Itemset.parse(kv[0]));
                }
            }
        }
        return list;
    }

    static void appendJobOutput(File jobOutDir, BufferedWriter dest) throws IOException {
        File[] parts = jobOutDir.listFiles((dir, name) -> name.startsWith("part-"));
        if (parts == null) {
            return;
        }
        for (File part : parts) {
            dest.write(Files.readString(part.toPath()));
        }
    }
}
