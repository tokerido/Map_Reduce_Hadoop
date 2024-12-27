import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class Step2
{
    // ---------------------------------------------------
    // 1) A composite key that holds (w1, w2, w3)
    // ---------------------------------------------------
    public static class TripletKey implements WritableComparable<TripletKey> {
        private String w1;
        private String w2;
        private String w3;

        public TripletKey() {}
        public TripletKey(String w1, String w2, String w3) {
            this.w1 = w1;
            this.w2 = w2;
            this.w3 = w3;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(w1);
            out.writeUTF(w2);
            out.writeUTF(w3);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            w1 = in.readUTF();
            w2 = in.readUTF();
            w3 = in.readUTF();
        }

        @Override
        public int compareTo(TripletKey other) {
            // Standard lexicographic compare by w1, then w2, then w3
            int cmp = w1.compareTo(other.w1);
            if (cmp != 0) return cmp;
            cmp = w2.compareTo(other.w2);
            if (cmp != 0) return cmp;
            return w3.compareTo(other.w3);
        }

        // getters & setters ...
        public String getW1() { return w1; }
        public String getW2() { return w2; }
        public String getW3() { return w3; }
    }

    // ---------------------------------------------------
    // 2) The "tagged value" that the mapper emits
    // ---------------------------------------------------
    public static class TaggedValue implements Writable {
        private Text tag;    // e.g. "1GRAM", "2GRAM", "3GRAM", "C0"
        private LongWritable count;  // the numeric count
        // Possibly store which position(s) this belongs to (for 2-gram or 1-gram).
        // But let's keep it simple and store the original token(s) as needed.
        private Text extra;  // e.g. store "w1_w2" or "w" if needed

        public TaggedValue() {
            this.tag = new Text();
            this.count = new LongWritable(0);
            this.extra = new Text();
        }

        public TaggedValue(String tag, long count, String extra) {
            this.tag = new Text(tag);
            this.count = new LongWritable(count);
            this.extra = new Text(extra);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            tag.write(out);
            count.write(out);
            extra.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            tag.readFields(in);
            count.readFields(in);
            extra.readFields(in);
        }

        public Text getTag() { return tag; }
        public LongWritable getCount() { return count; }
        public Text getExtra() { return extra; }
    }

    public static class MapperClass extends Mapper<LongWritable, Text, TripletKey, TaggedValue> {
        private boolean is1GramFile = false;
        private boolean is2GramFile = false;
        private boolean is3GramFile = false;

        // One approach: use different input paths or
        // set a config property telling us which file we're mapping
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // In a real job, you might pass a parameter or
            // check the input split path. For example:
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

                    // Key = (w1, w2, w3)
                    TripletKey outKey = new TripletKey(w1, w2, w3);
                    // Value = (tag="3GRAM", count=n3, extra="")
                    TaggedValue outVal = new TaggedValue("3GRAM", n3, "");
                    context.write(outKey, outVal);
                }
            }
            else if (is2GramFile) {
                // Format: wA wB N2
                if (fields.length == 3) {
                    String wA = fields[0];
                    String wB = fields[1];
                    long n2 = Long.parseLong(fields[2]);

                    // We need to contribute to BOTH:
                    //   - (wA, wB, ANY) => this is the 3-gram key w1=wA, w2=wB, w3=?
                    //   - (ANY, wA, wB) => this might be the 3-gram key w1=?, w2=wA, w3=wB
                    //
                    // In principle, we might replicate this record for *every possible w3*
                    // or *every possible w1*, but that can be huge. We'll show a smaller approach:
                    // We'll produce partial keys with asterisks to rely on a custom grouping comparator.
                    // For example:
                    TripletKey outKey1 = new TripletKey(wA, wB, "*");
                    TaggedValue outVal1 = new TaggedValue("2GRAM", n2, "POS12");
                    context.write(outKey1, outVal1);

                    TripletKey outKey2 = new TripletKey("*", wA, wB);
                    TaggedValue outVal2 = new TaggedValue("2GRAM", n2, "POS23");
                    context.write(outKey2, outVal2);
                }
            }
            else if (is1GramFile) {
                // Format: wC N1 OR "C0 totalCount"
                if (fields.length == 2) {
                    if (fields[0].equals("C0")) {
                        long c0 = Long.parseLong(fields[1]);
                        // We'll broadcast this to a wildcard key
                        TripletKey outKey = new TripletKey("*", "*", "*");
                        TaggedValue outVal = new TaggedValue("C0", c0, "");
                        context.write(outKey, outVal);
                    } else {
                        String wC = fields[0];
                        long n1 = Long.parseLong(fields[1]);

                        // For a single word wC, it might appear as w1, w2, or w3
                        // We'll replicate:

                        TripletKey k2 = new TripletKey("*", wC, "*");  // w2 = wC
                        context.write(k2, new TaggedValue("1GRAM", n1, "POS2"));

                        TripletKey k3 = new TripletKey("*", "*", wC);  // w3 = wC
                        context.write(k3, new TaggedValue("1GRAM", n1, "POS3"));
                    }
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<TripletKey, TaggedValue, Text, NullWritable> {

        @Override
        protected void reduce(TripletKey key, Iterable<TaggedValue> values, Context context)
                throws IOException, InterruptedException {

            // We'll gather partial data here:
            // We might have multiple 1-gram counts for w1, w2, w3, multiple 2-gram counts, etc.
            long n3 = 0;
            long c0 = 0;

            // For 1-grams:
            long n1_w1 = 0;
            long n1_w2 = 0;
            long n1_w3 = 0;

            // For 2-grams:
            long n2_w1w2 = 0;
            long n2_w2w3 = 0;

            // We only want to output a final line if this key is a "real" 3-gram
            // i.e. key != (*, something, something) etc.  We'll check later.
            boolean has3Gram = false;

            for (TaggedValue tv : values) {
                String tag = tv.getTag().toString();
                long count = tv.getCount().get();
                String extra = tv.getExtra().toString();

                if (tag.equals("3GRAM")) {
                    n3 = count;
                    has3Gram = true;
                }
                else if (tag.equals("2GRAM")) {
                    // "POS12" => means this is for (w1,w2,*)
                    // "POS23" => means this is for (*,w2,w3)
                    if (extra.equals("POS12")) {
                        // (key.w1, key.w2, "*")
                        n2_w1w2 = count;
                    } else if (extra.equals("POS23")) {
                        // ( "*", key.w2, key.w3)
                        n2_w2w3 = count;
                    }
                }
                else if (tag.equals("1GRAM")) {
                    // "POS1" => (w1, *, *) => n1 for w1
                    // "POS2" => (*, w2, *) => n1 for w2
                    // "POS3" => (*, *, w3) => n1 for w3
                    if (extra.equals("POS1")) {
                        n1_w1 = count;
                    } else if (extra.equals("POS2")) {
                        n1_w2 = count;
                    } else if (extra.equals("POS3")) {
                        n1_w3 = count;
                    }
                }
                else if (tag.equals("C0")) {
                    c0 = count;
                }
            } // end for

            // Now decide if we want to emit a final record. Typically,
            // we only have a "real" 3-gram if w1 != "*" and w2 != "*" and w3 != "*"
            // and has3Gram == true.
            String w1 = key.getW1();
            String w2 = key.getW2();
            String w3 = key.getW3();

            if (has3Gram && !w1.equals("*") && !w2.equals("*") && !w3.equals("*")) {
                double k2 = (Math.log(n2_w2w3 + 1) + 1) / (Math.log(n2_w2w3 + 1) + 2);
                double k3 = (Math.log(n3 + 1) + 1) / (Math.log(n3 + 1) + 2);
                double prob = k3 * (n3/n2_w1w2) + (1 - k3)*k2*(n2_w2w3/n1_w2) + (1 - k3)*(1 - k2)*(n1_w3/c0);


//                context.write(new Text(String.format("%s %s %s", w1, w2, w3)), new Text(prob.toString()));
            }
        }
    }

//    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        //        public static int C0 = -1;
//
//        @Override
//        protected void setup(Context context) {
//        }
//
//        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String line = value.toString();
//            String[] parts = line.split("\t");
//            if (parts.length != 2) return;
//
//            String nGram = parts[0].trim(); // e.g., "* w *" or "w q s"
//            String countInfo = parts[1].trim(); // Count value
//
//            String[] words = nGram.split(" ");
//
//            if (nGram.equals("* C0 *")) {
//                // Corpus size
//                context.write(new Text("BROADCAST"), new Text("C0:" + countInfo));
//            } else if (nGram.startsWith("*") && nGram.endsWith("*")) {
//                // Unigram count for w
//                context.write(new Text(words[1] + ":UNIGRAM"), new Text("C1:" + countInfo));
//            } else if (nGram.endsWith("*")) {
//                // Bigram count <w, q>
//                context.write(new Text(words[0] + "," + words[1] + ":BIGRAM"), new Text("C2:" + countInfo));
//            } else if (nGram.startsWith("*")) {
//                // Bigram count <q, s>
//                context.write(new Text(words[1] + "," + words[2] + ":BIGRAM"), new Text("N2:" + countInfo));
//            } else if (words.length == 3) {
//                // Trigram count <w, q, s>
//                context.write(new Text(words[0] + "," + words[1] + "," + words[2]), new Text("N3:" + countInfo));
//            }
//        }
//    }
//
//    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
//
//        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            int C0 = 0, C1 = 0, C2 = 0, N1 = 0, N2 = 0, N3 = 0;
//
//            for (Text value : values) {
//                String[] parts = value.toString().split(":");
//                String countType = parts[0];
//                int count = Integer.parseInt(parts[1]);
//
//                switch (countType) {
//                    case "C0":
//                        C0 = count;
//                        break;
//                    case "C1":
//                        C1 = count;
//                        break;
//                    case "C2":
//                        C2 = count;
//                        break;
//                    case "N1":
//                        N1 = count;
//                        break;
//                    case "N2":
//                        N2 = count;
//                        break;
//                    case "N3":
//                        N3 = count;
//                        break;
//                }
//            }
//
//            // Emit trigram and its associated counts
//            context.write(key, new Text("C0:" + C0 + " C1:" + C1 + " C2:" + C2 + " N1:" + N1 + " N2:" + N2 + " N3:" + N3));
//        }
//    }
//
////    public static class CombinerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
//
////        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
////            int sum = 0;
////            for (IntWritable value : values) {
////                sum += value.get();
////            }
////            context.write(key, new IntWritable(sum));
////
////        }
////    }
//
//    public static class PartitionerClass extends Partitioner<Text, Text> {
//        @Override
//        public int getPartition(Text key, Text value, int numPartitions) {
//            String baseKey;
//
//            // Handle special cases
//            if (key.toString().startsWith("BROADCAST")) {
//                baseKey = "BROADCAST";
//            } else {
//                // Use only the base trigram key (strip type suffixes like :UNIGRAM, :BIGRAM)
//                baseKey = key.toString().split(":")[0];
//            }
//
//            // Hash the base key and ensure it maps to one of the reducers
//            return (baseKey.hashCode() & Integer.MAX_VALUE) % numPartitions;
//        }
//
    public static void calculateProbabilities(double N1, double N2, double N3, double C0, double C1,double C2) {

        double k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 2) + 2);
        double k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 2) + 2);

        double prob = k3 * (N3/C2) + (1 - k3)*k2*(N2/N1);

    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2");

        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
//        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(CombinerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

//        FileInputFormat.addInputPath(job,
//                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
//        FileInputFormat.addInputPath(job, new Path("s3://bucketforjars/input/3gram_sample_100"));
//        FileOutputFormat.setOutputPath(job, new Path("s3://bucketforjars/output_word_count"));

//        conf.set("mapreduce.input.fileinputformat.split.maxsize", "128000000");

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);

        // if (args.length != 2) {
        // System.err.println("Usage: WordCount <input path> <output path>");
        // System.exit(-1);
        // }

        // Configuration conf = new Configuration();
        // Job job = Job.getInstance(conf, "Local WordCount");

        // job.setJarByClass(WordCount.class);
        // job.setMapperClass(MapperClass.class);
        // job.setReducerClass(ReducerClass.class);
        // job.setCombinerClass(CombinerClass.class);
        // job.setPartitionerClass(PartitionerClass.class);

        // // job.setInputFormatClass(TextInputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class);

        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        // job.setOutputFormatClass(TextOutputFormat.class);

        // // Set the input and output paths from command line arguments
        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
