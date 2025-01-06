
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

import java.net.URI;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import org.apache.hadoop.fs.FileSystem;

public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        public final HashSet<String> STOPWORDS = new HashSet<>();

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

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String currentFileName = fileSplit.getPath().getName();
            System.out.println("Processing file: " + currentFileName);

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
                    context.write(new Text(String.join(" ", words)), new Text(fields[2]));
                    if (words.length == 1) {
                        // Add to the total word count in the corpus - C0
                        context.getCounter("CustomGroup", "C0").increment(Integer.parseInt(fields[2]));
                    }
                }
            } else {
                System.out.println("fields length is 0");
            }
        }
    }

    ///
    /// Combine all the counts of the mapper into single values
    /// For example:
    /// [(Danny,1),(Danny,1),(Danny,1),(Tammy,1),(Tammy,1)] -> [(Danny,3),(Tammy,2)]
    ///
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> multipleOutputs;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] words = key.toString().split(" ");

            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }

            String output;
            switch (words.length) {
                case 1:
                    output = "1gram";
                    break;
                case 2:
                    output = "2gram";
                    break;
                case 3:
                    output = "3gram";
                    break;
                default:
                    output = "error"; // Default case
                    break;
            }

            multipleOutputs.write(output, key, new Text(Integer.toString(sum)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    ///
    /// Combine all the counts of the mapper into single values
    /// For example:
    /// [(Danny,1),(Danny,1),(Danny,1),(Tammy,1),(Tammy,1)] -> [(Danny,3),(Tammy,2)]
    ///
    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }

            context.write(key, new Text(Integer.toString(sum)));
        }
    }

    ///
    /// Partition by the first word
    ///
    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String w1 = key.toString().split(" ")[0];
            return Math.abs(w1.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");

//         String jarBucketName = "jarbucket1012";
        String jarBucketName = "hadoop-map-reduce-bucket";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1");

//        int NUM_MAPPERS = 4;
//        conf.setInt("mapreduce.job.maps", NUM_MAPPERS);
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 10 * 1024); // 32MB (default is 128MB)



        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://" + jarBucketName + "/input/"));

//        job.setInputFormatClass(SequenceFileInputFormat.class); // For S3 Ngram data
//        FileInputFormat.addInputPath(job,
//                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
//        FileInputFormat.addInputPath(job,
//                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
//        FileInputFormat.addInputPath(job,
//                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"));



        MultipleOutputs.addNamedOutput(job, "1gram", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "2gram", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "3gram", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "error", TextOutputFormat.class, Text.class, Text.class);


        FileOutputFormat.setOutputPath(job, new Path("s3://" + jarBucketName + "/step1_output_small/"));

        boolean success = job.waitForCompletion(true);

        if (success) {
            // Write down C0 value to use in the next step
            long c0Value = job.getCounters()
                    .findCounter("CustomGroup", "C0")
                    .getValue();

            String s3OutputPath = "s3://" + jarBucketName + "/step1_output_small/part-r-00000";

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
