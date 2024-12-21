import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

public class WordCount {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        public final HashSet<String> STOPWORDS = new HashSet<>();

        @Override
        protected void setup(Context context) {
            Collections.addAll(STOPWORDS, "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה", "מ", "למה", "לכל", "לי", "לו", "להיות",
                    "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר", "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו",
                    "הרבה", "הנה", "היו", "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", "אלה", "אל", "אך", "איש", "אין",
                    "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום",
                    "לעולם", "לנו", "להם", "ישראל", "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם",
                    "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם", "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל",
                    "ובין", "ואין", "הן", "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א");
        }

        protected boolean isValuable(String[] words){
            if (words.length != 3){
                return false;
            }

            for (String word : words){
                if (word.length() < 2 || STOPWORDS.contains(word)){
                    return false;
                }
            }

            return true;
        }

        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            Counter globalCounter = context.getCounter("Global", "TotalRows");

            if (globalCounter.getValue() < 10) {
                String[] fields = line.toString().split("\t");

                if (fields.length < 3) {
                    System.out.println("[ERROR] No occurrence number found in " + line);
                } else {
                    String[] words = fields[0].split(" ");
                    if (isValuable(words)) {
                        IntWritable occurrences = new IntWritable(Integer.parseInt(fields[2]));
                        String delimiter = "--";
                        context.write(new Text("*" + delimiter + words[1] + delimiter + words[2]), occurrences);
                        context.write(new Text(words[0] + delimiter + words[1] + delimiter + "*"), occurrences);
                        context.write(new Text(String.join(delimiter, words)), occurrences);
                        globalCounter.increment(1);
                    }
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {


        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));

        }
    }

    public static class CombinerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            // Partition based on the first field (before the first "--")
            String w2 = key.toString().split("--")[1];
            return Math.abs(w2.hashCode() % numPartitions);
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
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job,
                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucketforjars/output_word_count"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
