import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class WordCount {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Context context) {

        }

        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            Counter globalCounter = context.getCounter("Global", "TotalRows");

            if (globalCounter.getValue() < 10) {
                String[] fields = line.toString().split("\t");
                Integer fieldsLength = fields.length;
                String msg = "";
                for (int i = 0; i < fieldsLength; i++) {
                    msg += fields[i] + "--";
                }
                context.write(new Text(msg), new Text(fieldsLength.toString()));
                globalCounter.increment(1);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Simply output the first value since we're just counting field lengths
            for (Text value : values) {
                context.write(key, value);
                break; // We only need the first value
            }
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Pass through the values as is
            for (Text value : values) {
                context.write(key, value);
                break; // We only need the first value
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            // Partition based on the first field (before the first "--")
            String firstField = key.toString().split("--")[0];
            return Math.abs(firstField.hashCode() % numPartitions);
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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setCombinerClass(CombinerClass.class);
        FileInputFormat.addInputPath(job,
                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucketforjars/output_word_count"));
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