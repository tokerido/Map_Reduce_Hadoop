
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.*;
import java.util.Collections;
import java.util.HashSet;

import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;

public class Step_1_2_Combined {

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
        public final HashSet<String> STOPWORDS = new HashSet<>();
        private boolean is1GramFile = false;
        private boolean is2GramFile = false;
        private boolean is3GramFile = false;
        private final String NULL_CHARACTER = "\u0000";

        @Override
        protected void setup(Context context) {
            Collections.addAll(STOPWORDS,
                    "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה", "מ",
                    "למה", "לכל", "לי", "לו",
                    "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר", "יד", "י", "זה", "ז",
                    "ועל", "ומי", "ולא", "וכן", "וכל", "והיא",
                    "והוא", "ואם", "ו", "הרבה", "הנה", "היו", "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל",
                    "בו", "בה", "בא", "את", "אשר", "אם",
                    "אלה", "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1",
                    ".", "-", "*", "\"", "!", "שלשה", "בעל",
                    "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל", "יודע", "זאת",
                    "השמים", "הזאת", "הדברים", "הדבר", "הבית",
                    "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק", "שני", "שכל",
                    "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו",
                    "מלא", "מזה", "ם", "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן", "היתה",
                    "הא", "ה", "בל", "בין", "בזה", "ב", "אף",
                    "אי", "אותה", "או", "אבל", "א", "");

//            FileSplit fileSplit = (FileSplit) context.getInputSplit();
//            String currentFileName = fileSplit.getPath().getName();
//            System.out.println("Processing file: " + currentFileName);
//            if (currentFileName.contains("1gram")) {
//                is1GramFile = true;
//            } else if (currentFileName.contains("2gram")) {
//                is2GramFile = true;
//            } else if (currentFileName.contains("3gram")) {
//                is3GramFile = true;
//            }

        }

        ///
        /// Filter out n-grams that aren't valuable data like commonly used words or signs
        ///
        protected boolean isValuable(String[] words, int nGramSize, Context context) {

            if (words.length != nGramSize) {
                return false;
            }

            for (int i = 0; i < words.length; i++) {
                if (words[i] == null) {
                    return false;
                }
                if (words[i].startsWith("\"") && words[i].length() > 1) {
                    words[i] = words[i].substring(1);
                }
                if (STOPWORDS.contains(words[i])) {
                    return false;
                }
            }
            return true;
        }

        ///
        /// Counts all the valuable data in each line
        ///
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {

            String[] fields = line.toString().split("\t");

            if (fields.length > 0) {
                String[] words = fields[0].split(" ");

                if (isValuable(words, words.length, context)) {
                    // Format: w1 w2 w3 N3
                    if (words.length == 3) {
                        String w1 = words[0];
                        String w2 = words[1];
                        String w3 = words[2];
                        long n3 = Long.parseLong(fields[2]);

                        TaggedValue outVal2 = new TaggedValue("3GRAM-2", n3, String.format("%s %s %s", w1, w2, w3));
                        context.write(new Text(String.format("%s %s %s", w2, w1, w3)), outVal2);// CHANGE IN THE WORD ORDER!

                        TaggedValue outVal3 = new TaggedValue("3GRAM-3", n3, String.format("%s %s %s", w1, w2, w3));
                        context.write(new Text(String.format("%s %s %s", w3, w2, w1)), outVal3);// CHANGE IN THE WORD ORDER!
                    }

                    // Format: wA wB N2
                    else if (words.length == 2) {
                        String w1 = words[0];
                        String w2 = words[1];
                        long count = Long.parseLong(fields[2]);

                        TaggedValue outVal = new TaggedValue("2GRAM", count, String.format("%s %s", w1, w2));
                        context.write(new Text(String.format("%s %s %s",w2 ,w1, NULL_CHARACTER)), outVal);
                    }


                    else if (words.length == 1) {
                        String w1 = words[0];
                        long count = Long.parseLong(fields[2]);

                        TaggedValue outVal = new TaggedValue("1GRAM", count, String.format("%s", w1));
                        context.write(new Text(String.format("%s %s %s", w1, NULL_CHARACTER, NULL_CHARACTER)), outVal);
                        // Add to the total word count in the corpus - C0
//                            context.getCounter("CustomGroup", "C0").increment(count);
                        context.write(new Text(String.format("%s %s %s", NULL_CHARACTER, NULL_CHARACTER, "C0")),
                                        new TaggedValue("C0", count, ""));
                    }
                }
            }
            else {
                System.out.println("fields length is 0");
            }
        }

//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//
////            context.getConfiguration().set("C0" ,Long.toString(context.getCounter("CustomGroup", "C0").getValue()));
//            long c0 = context.getCounter("CustomGroup", "C0").getValue();
//            context.write(new Text("C0_TAG"), new TaggedValue("C0", c0, ""));
//        }
    }

    ///
    /// Combine all the counts of the mapper into single values
    /// For example:
    /// [(Danny,1),(Danny,1),(Danny,1),(Tammy,1),(Tammy,1)] -> [(Danny,3),(Tammy,2)]
    ///
    public static class ReducerClass extends Reducer<Text, TaggedValue, Text, Text> {

        public long C0;
        // For 1-grams:
        public long NorC1 = 0;
        // For 2-grams:
        public long NorC2 = 0;
//        private MultipleOutputs<Text, Text> multipleOutputs;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            multipleOutputs = new MultipleOutputs<>(context);
//            C0 = Long.parseLong(context.getConfiguration().get("C0"));
        }

        @Override
        public void reduce(Text key, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {

            TaggedValue lastVal = null;
            long sum = 0;

            boolean lastRoundUpdate = false;

            for (TaggedValue value : values) {

//                /*Debug*/context.write(new Text(String.format("Gram %s", value.getNGram())), new Text(""));

                if (lastVal == null){ // Set the first value
                    lastVal = value;
                    sum = value.getCount().get();
                    continue;
                }

                if (value.getNGram().equals(lastVal.getNGram())) {
                    sum += value.getCount().get();
                    lastRoundUpdate = false;

//                    /*Debug*/context.write(new Text(String.format("In gram %s, changed sum to: %d", lastVal.getNGram(), sum)), new Text(""));
                }
                else {

                    handleSum(lastVal, sum, context);

//                    /*Debug*/context.write(new Text(String.format("Moving from value %s to value %s", lastVal.getNGram(), value.getNGram())), new Text(""));
                    lastRoundUpdate = true;
                    lastVal = value;
                    sum = value.getCount().get();
                }
            }

            if (lastVal != null && !lastRoundUpdate)
                handleSum(lastVal,sum,context);
        }

        private void handleSum(TaggedValue val, long sum, Context context) throws IOException, InterruptedException{
            String tag = val.getTag().toString();
            String nGram = val.getNGram().toString();

            switch (tag) {
                case "3GRAM-2" : // W2 case
                    context.write(new Text(nGram), new Text(String.format("C0:%d C1:%d C2:%d N3:%d", C0, NorC1, NorC2, sum)));
                    break;

                case "3GRAM-3": // W3 case
                    context.write(new Text(nGram), new Text(String.format("C0:%d N1:%d N2:%d N3:%d", C0, NorC1, NorC2, sum)));
                    break;

                case "2GRAM" :
                    NorC2 = sum;
                    break;

                case "1GRAM" :
                    NorC1 = sum;
                    break;

                case "C0":
                    C0 = sum;
                    break;
            }
        }
    }

    ///
    /// Combine all the counts of the mapper into single values
    /// For example:
    /// [(Danny,1),(Danny,1),(Danny,1),(Tammy,1),(Tammy,1)] -> [(Danny,3),(Tammy,2)]
    ///
    public static class CombinerClass extends Reducer<Text, TaggedValue, Text, TaggedValue> {
        @Override
        public void reduce(Text key, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {

            TaggedValue lastVal = null;
            long sum = 0;

            boolean lastRoundUpdate = false;

            for (TaggedValue value : values) {

//                /*Debug*/context.write(new Text(String.format("Gram %s", value.getNGram())), new Text(""));

                if (lastVal == null){ // Set the first value
                    lastVal = value;
                    sum = value.getCount().get();
                    continue;
                }

                if (value.getNGram().equals(lastVal.getNGram())) {
                    sum += value.getCount().get();
                    lastRoundUpdate = false;

//                    /*Debug*/context.write(new Text(String.format("In gram %s, changed sum to: %d", lastVal.getNGram(), sum)), new Text(""));
                }
                else {

                    context.write(key, lastVal);

//                    /*Debug*/context.write(new Text(String.format("Moving from value %s to value %s", lastVal.getNGram(), value.getNGram())), new Text(""));
                    lastRoundUpdate = true;
                    lastVal = value;
                    sum = value.getCount().get();
                }
            }

            if (lastVal != null && !lastRoundUpdate)
                context.write(key, lastVal);
        }

    }

    ///
    /// Partition by the first word
    ///
//    public static class PartitionerClass extends Partitioner<Text, Text> {
//
//        @Override
//        public int getPartition(Text key, Text value, int numPartitions) {
//            String w1 = key.toString().split(" ")[0];
//            return Math.abs(w1.hashCode() % numPartitions);
//        }
//    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1_2_Combined started!");

//         String jarBucketName = "jarbucket1012";
        String jarBucketName = "hadoop-map-reduce-bucket";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1");

//        int NUM_MAPPERS = 4;
//        conf.setInt("mapreduce.job.maps", NUM_MAPPERS);
//        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 10 * 1024); // 32MB (default is 128MB)



        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
//        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TaggedValue.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        job.setInputFormatClass(TextInputFormat.class);
//        FileInputFormat.addInputPath(job, new Path("s3://" + jarBucketName + "/input/"));

        job.setInputFormatClass(SequenceFileInputFormat.class); // For S3 Ngram data
        FileInputFormat.addInputPath(job,
                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        FileInputFormat.addInputPath(job,
                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
        FileInputFormat.addInputPath(job,
                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"));


        FileOutputFormat.setOutputPath(job, new Path("s3://" + jarBucketName + "/step1_output_combined/"));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }

}
