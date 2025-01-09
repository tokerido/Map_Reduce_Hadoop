
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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

    public static class UniqueKey implements WritableComparable<UniqueKey> {
        private final Text nGram;  // e.g. "w1 w2 w3"
        private final Text tag;      // e.g. "3GRAM-2" or "3GRAM-3"

        public UniqueKey() {
            this.nGram = new Text();
            this.tag = new Text();
        }

        public UniqueKey(String nGram, String tag) {
            this.nGram = new Text(nGram);
            this.tag = new Text(tag);
        }

        public Text getNGram() {
            return nGram;
        }

        public Text getTag() {
            return tag;
        }

        private static int getTagPriority(String tag) {
            // Adjust these as you prefer
            switch (tag) {
                case "1GRAM":    return 1;
                case "2GRAM":    return 2;
                case "3GRAM-2":  return 3;
                case "3GRAM-3":  return 4;
                default:         return 999; // fallback if something unexpected
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            nGram.write(out);
            tag.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            nGram.readFields(in);
            tag.readFields(in);
        }

        /**
         * compareTo: Here you decide how to sort the data.
         * You want to IGNORE the tag in sorting order for the main sort,
         * so first compare the nGram, ignoring the tag.
         * If you want some tiebreak among tags, do that second.
         */
        @Override
        public int compareTo(UniqueKey other) {
            // Sort primarily by nGram
            int cmp = this.nGram.compareTo(other.nGram);
            if (cmp != 0) {
                return cmp;
            }
            // If you want a secondary sort by tag (like "3GRAM-2" before "3GRAM-3"),
            // then uncomment this:
            int thisPriority = getTagPriority(this.tag.toString());
            int otherPriority = getTagPriority(other.tag.toString());
            return Integer.compare(thisPriority, otherPriority);

            // If you truly want to IGNORE tag in ordering, return 0 if nGrams match
//            return 0;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof UniqueKey) {
                UniqueKey other = (UniqueKey) o;
                // For equality, both nGram AND tag must match
                return this.nGram.equals(other.nGram) &&
                        this.tag.equals(other.tag);
            }
            return false;
        }

        @Override
        public int hashCode() {
            // Combine nGram + tag in a standard way
            return (nGram.hashCode() * 163) + tag.hashCode();
        }
    }

    ///
    /// Custom writable value to store all the data necessary
    ///
    public static class TaggedValue implements Writable {
        private final Text tag;    // For example: "1GRAM", "2GRAM", "3GRAM"
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

    public static class MapperClass extends Mapper<LongWritable, Text, UniqueKey, TaggedValue> {
        public final HashSet<String> STOPWORDS = new HashSet<>();
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

        }

        ///
        /// Filter out n-grams that aren't valuable data like commonly used words or signs
        ///
        protected boolean isValuable(String[] words, int nGramSize, Context context) {

            if (words.length != nGramSize) {
                return false;
            }

            for (String word : words) {
                if (word == null) {
                    return false;
                }
                if (STOPWORDS.contains(word)) {
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
//                        context.write(new Text(String.format("%s %s %s", w2, w1, w3)), outVal2);// CHANGE IN THE WORD ORDER!
                        UniqueKey outKey2 = new UniqueKey(String.format("%s %s %s", w2, w1, w3), "3GRAM-2");
                        context.write(outKey2, outVal2);// CHANGE IN THE WORD ORDER!

                        TaggedValue outVal3 = new TaggedValue("3GRAM-3", n3, String.format("%s %s %s", w1, w2, w3));
//                        context.write(new Text(String.format("%s %s %s", w3, w2, w1)), outVal3);// CHANGE IN THE WORD ORDER!
                        UniqueKey outKey3 = new UniqueKey(String.format("%s %s %s", w3, w2, w1), "3GRAM-3");
                        context.write(outKey3, outVal3);// CHANGE IN THE WORD ORDER!
                    }

                    // Format: wA wB N2
                    else if (words.length == 2) {
                        String w1 = words[0];
                        String w2 = words[1];
                        long count = Long.parseLong(fields[2]);

                        TaggedValue outVal = new TaggedValue("2GRAM", count, String.format("%s %s", w1, w2));
//                        context.write(new Text(String.format("%s %s %s",w2 ,w1, NULL_CHARACTER)), outVal); // CHANGE IN THE WORD ORDER!
                        UniqueKey outKey = new UniqueKey(String.format("%s %s %s", w2, w1, NULL_CHARACTER), "2GRAM");
                        context.write(outKey, outVal);
                    }


                    else if (words.length == 1) {
                        String w1 = words[0];
                        long count = Long.parseLong(fields[2]);

                        TaggedValue outVal = new TaggedValue("1GRAM", count, String.format("%s", w1));
//                        context.write(new Text(String.format("%s %s %s", w1, NULL_CHARACTER, NULL_CHARACTER)), outVal);
                        UniqueKey outKey = new UniqueKey(String.format("%s %s %s", w1, NULL_CHARACTER, NULL_CHARACTER), "1GRAM");
                        context.write(outKey, outVal);
                        // Add to the total word count in the corpus - C0
                        context.getCounter("CustomGroup", "C0").increment(Integer.parseInt(fields[2]));
                    }
                }
            }
            else {
                throw new RuntimeException(String.format("fields length is %d", fields.length));
            }
        }
    }

    ///
    /// Combine all the counts of the mapper into single values
    /// For example:
    /// [(Danny,1),(Danny,1),(Danny,1),(Tammy,1),(Tammy,1)] -> [(Danny,3),(Tammy,2)]
    ///
    public static class ReducerClass extends Reducer<UniqueKey, TaggedValue, Text, Text> {

        // For 1-grams:
        public long NorC1 = 0;
        // For 2-grams:
        public long NorC2 = 0;

        @Override
        public void reduce(UniqueKey key, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {

            TaggedValue lastVal = null;
            long sum = 0;

            for (TaggedValue value : values) {

                if (lastVal == null){ // Set the first value
                    lastVal = value;
                    sum = value.getCount().get();
                    continue;
                }

                if (value.getNGram().equals(lastVal.getNGram()) && value.getTag().equals(lastVal.getTag())) {
                    sum += value.getCount().get();
                }
                else {
                    handleSum(lastVal, sum, context);
                    lastVal = value;
                    sum = value.getCount().get();
                }
            }

            if (lastVal != null)
                handleSum(lastVal,sum,context);
        }

        private void handleSum(TaggedValue val, long sum, Context context) throws IOException, InterruptedException{
            String tag = val.getTag().toString();
            String nGram = val.getNGram().toString();

            switch (tag) {
                case "3GRAM-2" : // W2 case
                    context.write(new Text(nGram), new Text(String.format("C1:%d C2:%d N3:%d", NorC1, NorC2, sum)));
                    break;

                case "3GRAM-3": // W3 case
                    context.write(new Text(nGram), new Text(String.format("N1:%d N2:%d N3:%d", NorC1, NorC2, sum)));
                    break;

                case "2GRAM" :
                    NorC2 = sum;
                    break;

                case "1GRAM" :
                    NorC1 = sum;
                    break;
            }
        }
    }

    ///
    /// Combine all the counts of the mapper into single values
    /// For example:
    /// [(Danny,1),(Danny,1),(Danny,1),(Tammy,1),(Tammy,1)] -> [(Danny,3),(Tammy,2)]
    ///
    public static class CombinerClass extends Reducer<UniqueKey, TaggedValue, UniqueKey, TaggedValue> {
        @Override
        public void reduce(UniqueKey key, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {

            TaggedValue lastVal = null;
            long sum = 0;

            for (TaggedValue value : values) {

                if (lastVal == null){ // Set the first value
                    lastVal = value;
                    sum = value.getCount().get();
                    continue;
                }

                if (value.getNGram().equals(lastVal.getNGram()) && value.getTag().equals(lastVal.getTag())) {
                    sum += value.getCount().get();
                }
                else {
                    context.write(key, new TaggedValue(lastVal.getTag().toString(), sum, lastVal.getNGram().toString()));
                    lastVal = value;
                    sum = value.getCount().get();
                }
            }

            if (lastVal != null)
                context.write(key, new TaggedValue(lastVal.getTag().toString(), sum, lastVal.getNGram().toString()));
        }
    }


    ///
    /// Partition by the first word
    ///
    public static class PartitionerClass extends Partitioner<UniqueKey, TaggedValue> {

        @Override
        public int getPartition(UniqueKey key, TaggedValue value, int numPartitions) {
            String w1 = key.getNGram().toString().split(" ")[0];
            return Math.abs(w1.hashCode() % numPartitions);
        }
    }

    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(UniqueKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            UniqueKey key1 = (UniqueKey) a;
            UniqueKey key2 = (UniqueKey) b;
            // Group only by w1w2
            return key1.getNGram().compareTo(key2.getNGram());
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1_2_Combined started!");

//        String jarBucketName = "jarbucket1012";
        String jarBucketName = "hadoop-map-reduce-bucket";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1");

//        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 64 * 1024 * 1024); // 64MB (default is 128MB)

        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setCombinerClass(CombinerClass.class);

        job.setPartitionerClass(PartitionerClass.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setMapOutputKeyClass(UniqueKey.class);
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


        FileOutputFormat.setOutputPath(job, new Path("s3://" + jarBucketName + "/step1_2_combined_output_large/"));

        boolean success = job.waitForCompletion(true);

        if (success) {
            // Write down C0 value to use in the next step
            long c0Value = job.getCounters()
                    .findCounter("CustomGroup", "C0")
                    .getValue();

            String s3OutputPath = "s3://" + jarBucketName + "/step1_2_combined_output_large/C0";

            FileSystem fs = FileSystem.get(URI.create(s3OutputPath), new Configuration());
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fs.create(new Path(s3OutputPath), true)))) {
                writer.write("C0 " + c0Value);
                writer.newLine();
            }
        }
        System.exit(success ? 0 : 1);
    }

}
