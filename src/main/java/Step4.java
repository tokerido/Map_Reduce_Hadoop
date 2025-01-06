import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;

public class Step4 {

    public static class ProbabilityKey implements WritableComparable<ProbabilityKey> {
        private final Text w1w2;
        private final DoubleWritable probability;

        public ProbabilityKey() {
            this.w1w2 = new Text();
            this.probability = new DoubleWritable();
        }

        public void set(String w1w2, double probability) {
            this.w1w2.set(w1w2);
            this.probability.set(probability);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            w1w2.write(out);
            probability.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            w1w2.readFields(in);
            probability.readFields(in);
        }

        @Override
        public int compareTo(ProbabilityKey other) {
            int w1w2Compare = this.w1w2.compareTo(other.w1w2);
            if (w1w2Compare != 0) {
                return w1w2Compare;
            }
            return -Double.compare(this.probability.get(), other.probability.get());
        }

        public Text getW1w2() {
            return w1w2;
        }

        public DoubleWritable getProbability() {
            return probability;
        }
    }

    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(ProbabilityKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ProbabilityKey key1 = (ProbabilityKey) a;
            ProbabilityKey key2 = (ProbabilityKey) b;
            // Group only by w1w2
            return key1.getW1w2().compareTo(key2.getW1w2());
        }
    }

    public static class MapperClass extends Mapper<LongWritable, Text, ProbabilityKey, Text> {
        private ProbabilityKey ProbabilityKey = new ProbabilityKey();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            if (fields.length != 4) {
                return;
            }
            String w1w2 = fields[0] + " " + fields[1];
            String w3 = fields[2];
            double probability = Double.parseDouble(fields[3]);

            ProbabilityKey.set(w1w2, probability);
            context.write(ProbabilityKey, new Text(w3));
        }
    }

    public static class ReducerClass extends Reducer<ProbabilityKey, Text, Text, Text> {
        @Override
        protected void reduce(ProbabilityKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text w3 : values) {
                String[] w1w2Parts = key.getW1w2().toString().split(" ");
                String outputKey = String.format("%s %s %s", w1w2Parts[0], w1w2Parts[1], w3.toString());
                context.write(new Text(outputKey), new Text(String.format("%.5f", key.getProbability().get())));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<ProbabilityKey, Text> {
        @Override
        public int getPartition(ProbabilityKey key, Text value, int numPartitions) {
            return Math.abs(key.getW1w2().hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");

//        String jarBucketName = "jarbucket1012";
        String jarBucketName = "hadoop-map-reduce-bucket";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step4");

        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setMapOutputKeyClass(ProbabilityKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("s3://" + jarBucketName + "/step3_output/"));
        FileOutputFormat.setOutputPath(job, new Path("s3://" + jarBucketName + "/step4_output/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
