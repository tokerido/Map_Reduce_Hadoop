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
    ///
    /// Custom writable value to store all the data necessary
    ///
    public static class TaggedValue implements Writable {
        private final Text tag;    // For example: "1GRAM", "2GRAM", "3GRAM", "C0"
        private final LongWritable count;  // The numeric count
        private final Text nGram;  // The original n-gram

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

                    TaggedValue outVal2 = new TaggedValue("3GRAM-2", n3, String.format("%s %s %s", w1, w2, w3));
                    context.write(new Text(String.format("%s %s %s", w2, w1, w3)), outVal2);

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

                    TaggedValue outVal = new TaggedValue("2GRAM", count, String.format("%s %s", w1, w2));
                    context.write(new Text(String.format("%s %s %s",w2 ,w1, NULL_CHARACTER)), outVal);
                }
            }
            else if (is1GramFile) {
                if (fields.length == 2) {
                    if (!fields[0].equals("C0")) {

                        String w1 = fields[0];
                        long n1 = Long.parseLong(fields[1]);

                        context.write(new Text(String.format("%s %s %s", w1, NULL_CHARACTER, NULL_CHARACTER)), new TaggedValue("1GRAM", n1, String.format("%s", w1) ));

                    }
                }
            }
        }
    }

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

            for (TaggedValue tv : values) {

                String tag = tv.getTag().toString();
                long count = tv.getCount().get();
                String nGram = tv.getNGram().toString();

                switch (tag) {
                    case "3GRAM-2" : // W2 case
                        context.write(new Text(nGram), new Text(String.format("C0:%d C1:%d C2:%d N3:%d", C0, NorC1, NorC2, count)));
                        break;

                    case "3GRAM-3": // W3 case
                        context.write(new Text(nGram), new Text(String.format("C0:%d N1:%d N2:%d N3:%d", C0, NorC1, NorC2, count)));
                        break;

                    case "2GRAM" :
                        NorC2 = count;
                        break;

                    case "1GRAM" :
                        NorC1 = count;
                        break;

                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");

//        String jarBucketName = "jarbucket1012";
        String jarBucketName = "hadoop-map-reduce-bucket";

        String s3InputPath = "s3a://" + jarBucketName + "/step1_output_small/part-r-00000";
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

        Configuration conf = new Configuration();
        conf.set("C0", c0Value);


        Job job = Job.getInstance(conf, "Step2");

        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TaggedValue.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path("s3://" + jarBucketName + "/step1_output_small/"));
        FileOutputFormat.setOutputPath(job, new Path("s3://" + jarBucketName + "/step2_output_small/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}