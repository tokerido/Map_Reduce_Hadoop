import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;

public class Step2
{
    public static class TaggedValue implements Writable {
        private final Text tag;    // e.g. "1GRAM", "2GRAM", "3GRAM", "C0"
        private final LongWritable count;  // the numeric count
        private final Text nGram;  // e.g. the original n-gram

        public TaggedValue() {
            this.tag = new Text();
            this.count = new LongWritable(0);
            this.nGram = new Text();
        }

        public TaggedValue(String tag, long count, String extra) {
            this.tag = new Text(tag);
            this.count = new LongWritable(count);
            this.nGram = new Text(extra);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            tag.write(out);
            count.write(out);
            nGram.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            tag.readFields(in);
            count.readFields(in);
            nGram.readFields(in);
        }

        public Text getTag() { return tag; }
        public LongWritable getCount() { return count; }
        public Text getNGram() { return nGram; }
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, TaggedValue> {
        private boolean is1GramFile = false;
        private boolean is2GramFile = false;
        private boolean is3GramFile = false;
        private final String NULL_CHARACTER = "\u0000";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            if (filename.contains("1gram")) {
                is1GramFile = true;
            } else if (filename.contains("2gram")) {
                is2GramFile = true;
            } else if (filename.contains("3gram")) {
                is3GramFile = true;
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            if (is3GramFile) {
                // Format: w1 w2 w3 N3
                if (fields.length == 4) {
                    String w1 = fields[0];
                    String w2 = fields[1];
                    String w3 = fields[2];
                    long n3 = Long.parseLong(fields[3]);

                    // Key = w2
//                    Text outKey2 = new Text(w2);
                    // Value = (tag="3GRAM", count=n3, nGram="")
                    TaggedValue outVal2 = new TaggedValue("3GRAM-2", n3, String.format("%s %s %s", w1, w2, w3));
                    context.write(new Text(String.format("%s %s %s", w2, w1, w3)), outVal2);

                    // Key = w3
//                    Text outKey3 = new Text(w3);
                    // Value = (tag="3GRAM", count=n3, nGram="")
                    TaggedValue outVal3 = new TaggedValue("3GRAM-3", n3, String.format("%s %s %s", w1, w2, w3)); // CHANGE IN THE WORD ORDER!
                    context.write(new Text(String.format("%s %s %s", w3, w2, w1)), outVal3);
                }
            }
            else if (is2GramFile) {
                // Format: wA wB N2
                if (fields.length == 3) {
                    String w1 = fields[0];
                    String w2 = fields[1];
                    long count = Long.parseLong(fields[2]);

//                    Text outKey = new Text(w2);
                    TaggedValue outVal = new TaggedValue("2GRAM", count, String.format("%s %s", w1, w2));
                    context.write(new Text(String.format("%s %s %s",w2 ,w1, NULL_CHARACTER)), outVal);
                }
            }
            else if (is1GramFile) {
                if (fields.length == 2) {
                    if (!fields[0].equals("C0")) {

                        String w1 = fields[0];
                        long n1 = Long.parseLong(fields[1]);

//                        Text outKey = new Text(w1);
                        context.write(new Text(String.format("%s %s %s", w1, NULL_CHARACTER, NULL_CHARACTER)), new TaggedValue("1GRAM", n1, String.format("%s", w1) ));

                    }
                }
            }
        }
    }

//    public static class CustomValueComparator implements Comparator<TaggedValue> {
//        @Override
//        public int compare(TaggedValue tv1, TaggedValue tv2) {
//            // Extract tags and nGrams
//            String tag1 = tv1.getTag().toString();
//            String tag2 = tv2.getTag().toString();
//            String nGram1 = tv1.getNGram().toString();
//            String nGram2 = tv2.getNGram().toString();
//
//            // Rule 1: `1GRAM` always comes first
//            if (tag1.equals("1GRAM") && !tag2.equals("1GRAM")) {
//                return -1; // tv1 (1GRAM) comes first
//            }
//            if (!tag1.equals("1GRAM") && tag2.equals("1GRAM")) {
//                return 1; // tv2 (1GRAM) comes first
//            }
//
//            // Rule 2: For other tags, sort alphabetically by nGram
//            return nGram1.compareTo(nGram2);
//        }
//    }

    public static class ReducerClass extends Reducer<Text, TaggedValue, Text, Text> {

        public long C0;
        // For 1-grams:
        public long NorC1 = 0;
        // For 2-grams:
        public long NorC2 = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            C0 = Long.parseLong(context.getConfiguration().get("C0"));
        }

        @Override
        protected void reduce(Text key, Iterable<TaggedValue> values, Context context)
                throws IOException, InterruptedException {

//            context.write(new Text("\nStarted Reducer"), new Text(""));
//            context.write(new Text(String.format("Key: %s\n", key.toString())), new Text(""));

            for (TaggedValue tv : values) {

                String tag = tv.getTag().toString();
                long count = tv.getCount().get();
                String nGram = tv.getNGram().toString();

//                context.write(new Text(String.format("tag: %s, count: %d, nGram: %s", tag, count, nGram)), new Text(""));

                switch (tag) {
                    case "3GRAM-2" : // W2 case
//                        context.write(new Text("Entered 3GRAM-2"), new Text(""));

                        context.write(new Text(nGram), new Text(String.format("C0:%d C1:%d C2:%d N3:%d", C0, NorC1, NorC2, count)));

                        break;

                    case "3GRAM-3": // W3 case
//                        context.write(new Text("Entered 3GRAM-3"), new Text(""));

//                        String[] words = nGram.split(" ");
//                        context.write(new Text(String.format("%s %s %s", words[2], words[0], words[1])), new Text(String.format("C0:%s N1:%d N2:%d N3:%d", C0, NorC1, NorC2, count)));
                        context.write(new Text(nGram), new Text(String.format("C0:%d N1:%d N2:%d N3:%d", C0, NorC1, NorC2, count)));
                        break;

                    case "2GRAM" :
//                        context.write(new Text("Entered 2GRAM"), new Text(""));
                        NorC2 = count;
                        break;

                    case "1GRAM" :
//                        context.write(new Text("Entered 1GRAM"), new Text(""));
                        NorC1 = count;
                        break;

                }
            }
        }


    }

    public static class PartitionerClass extends Partitioner<Text, TaggedValue> {
        @Override
        public int getPartition(Text key, TaggedValue value, int numPartitions) {
            // Let's define "canonical" w1, w2, w3 for partitioning:
            // if w1 != "", use w1; otherwise use w2; if w2 != "", etc...
            // That can get complicated. A simpler approach might just do:

//            String p1 = key.getW1().equals("") ? "" : key.getW1();
//            String p2 = key.getW2().equals("*") ? "*" : key.getW2();
//            String p3 = key.getW3().equals("*") ? "" : key.getW3();

//            String combined = p1 + "#" + p2 + "#" + p3;
            return Math.abs(key.toString().hashCode() % numPartitions);
        }
    }

//    public static void calculateProbabilities(double N1, double N2, double N3, double C0, double C1,double C2) {
//
//        double k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 2) + 2);
//        double k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 2) + 2);
//
//        double prob = k3 * (N3/C2) + (1 - k3)*k2*(N2/N1);
//
//    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
//        System.out.println(args.length > 0 ? args[0] : "no args");

//        String jarBucketName = "jarbucket1012";
        String jarBucketName = "hadoop-map-reduce-bucket";

        String s3InputPath = "s3a://" + jarBucketName + "/step1_output/part-r-00000";
        FileSystem fs = FileSystem.get(URI.create(s3InputPath), new Configuration());
        String c0Value = null;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(s3InputPath))))) {
            // Read the first line only
            String line = reader.readLine();
            if (line != null && line.startsWith("C0")) {
                c0Value = line.split(" ")[1]; // Extract the value
            }
        }

        if (c0Value == null) {
            throw new RuntimeException("Counter value for C0 not found in the file!");
        }

        System.out.println("C0 =" + c0Value);

        Configuration conf = new Configuration();
        conf.set("C0", c0Value);
        // Add the C0 file to the distributed cache


//        String c0 = "";
//        Path input_1gram = new Path("s3://" + jarBucketName + "/input-samples/output_test_1gram");  // Path to the file from Step1
//        FileSystem fs = FileSystem.get(conf);
//        FSDataInputStream in = fs.open(input_1gram);
//        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
//        String line;
//        while ((line = reader.readLine()) != null) {
//            if (line.startsWith("C0")) {
//                c0 = line.split("\t")[1];
//                break;
//            }
//        }
//        reader.close();

//        conf.set("C0:", c0);

        Job job = Job.getInstance(conf, "Step2");

        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
//        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(CombinerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TaggedValue.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        job.setInputFormatClass(SequenceFileInputFormat.class);  // For S3 N-gram data
        job.setInputFormatClass(TextInputFormat.class);

//        job.setNumReduceTasks(4);

//        FileInputFormat.addInputPath(job,
//                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
//        FileInputFormat.addInputPath(job, new Path("s3://bucketforjars/input/3gram_sample_100"));
//        FileOutputFormat.setOutputPath(job, new Path("s3://bucketforjars/output_word_count"));

//        conf.set("mapreduce.input.fileinputformat.split.maxsize", "128000000");

//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileInputFormat.addInputPath(job, input_1gram);
//        FileInputFormat.addInputPath(job, new Path("/user/local/input/output_test_2gram"));
//        FileInputFormat.addInputPath(job, new Path("/user/local/input/output_test_3gram"));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileInputFormat.addInputPath(job, new Path("s3://" + jarBucketName + "/step1_output/"));
//        FileInputFormat.addInputPath(job, new Path("/user/local/step1_output/"));
        FileOutputFormat.setOutputPath(job, new Path("s3://" + jarBucketName + "/step2_output/"));
//        FileOutputFormat.setOutputPath(job, new Path("/user/local/step2_output/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}