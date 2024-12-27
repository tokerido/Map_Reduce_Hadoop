
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import software.amazon.ion.SystemSymbols;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

public class WordCount {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        public final HashSet<String> STOPWORDS = new HashSet<>();
        private String currentFileName;

        @Override
        protected void setup(Context context) {
            Collections.addAll(STOPWORDS,
                    "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה", "מ", "למה", "לכל", "לי", "לו",
                    "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר", "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא",
                    "והוא", "ואם", "ו", "הרבה", "הנה", "היו", "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם",
                    "אלה", "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*", "\"", "!", "שלשה", "בעל",
                    "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל", "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית",
                    "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו",
                    "מלא", "מזה", "ם", "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן", "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף",
                    "אי", "אותה", "או", "אבל", "א", "");

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            currentFileName = fileSplit.getPath().getName();
            System.out.println("Processing file: " + currentFileName);
        }

        protected boolean isValuable(String[] words, int nGramSize){

            if (words.length != nGramSize){
                return false;
            }

            for (String word : words){
                if (word == null || STOPWORDS.contains(word)){
                    return false;
                }
            }

            return true;
        }


        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {

            String[] fields = line.toString().split("\t");

            if (fields.length > 0) {
                String[] words = fields[0].split(" ");
                if (isValuable(words, words.length)) {
                    context.write(new Text(fields[0]), new Text(fields[2]));
                    // Increment total words counter for 1-grams
                    if (words.length == 1) {
                        context.write(new Text("C0"), new Text(fields[2]));
                    }
                }
            }else{
                System.out.println("fields length is 0");
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> multipleOutputs;
        private long totalWords = 0;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            if (key.toString().equals("C0")) {
                for (Text value : values) {
                    totalWords += Long.parseLong(value.toString());
                }
                return;
            }

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
                    output = "1gram"; // Default case
                    break;
            }

            multipleOutputs.write(output, key, new Text(Integer.toString(sum)));
//            context.write(key, new Text(Integer.toString(sum)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.write("1gram", new Text("C0"), new Text(Long.toString(totalWords)));
            multipleOutputs.close();
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {

//        private MultipleOutputs<Text, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            if (key.toString().equals("C0")) {
                long wordTotal = 0;
                for (Text value : values) {
                    wordTotal += Long.parseLong(value.toString());
                }
                context.write(key, new Text(Long.toString(wordTotal)));
                return;
            }

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
                    output = "1gram"; // Default case
                    break;
            }


//            multipleOutputs.write(output, key, new Text(Integer.toString(sum)));
            context.write(key, new Text(Integer.toString(sum)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
//            multipleOutputs.close();
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            if (key.toString().equals("C0")) {
                return 0; // Send all C0 counts to the same reducer
            }

            // Partition based on the first word
            String w1 = key.toString().split(" ")[0];
            return Math.abs(w1.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);

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

        // Enable MultipleOutputs
        MultipleOutputs.addNamedOutput(job, "1gram", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "2gram", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "3gram", TextOutputFormat.class, Text.class, Text.class);


//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        FileInputFormat.addInputPath(job, new Path("/user/local/input/1gram_sample.csv"));
        FileInputFormat.addInputPath(job, new Path("/user/local/input/2gram_sample.csv"));
        FileInputFormat.addInputPath(job, new Path("/user/local/input/3gram_sample.csv"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));





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
