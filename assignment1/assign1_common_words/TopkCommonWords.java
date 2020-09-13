import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopkCommonWords {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set stopWords = new HashSet();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path path = new Path(cacheFiles[0].toString());
                readFile(path);
            }
        }

        private void readFile(Path filePath) {
            try{
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
                String stopWord = null;
                while((stopWord = bufferedReader.readLine()) != null) {
                    stopWords.add(stopWord);
                }
            } catch(IOException ex) {
                System.err.println("Exception while reading stop words file: " + ex.getMessage());
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String curr_word = itr.nextToken();

                curr_word = curr_word.replaceAll("\n","");
                curr_word = curr_word.replaceAll("\t","");
                curr_word = curr_word.replaceAll("\r","");
                curr_word = curr_word.replaceAll("\f","");
                if (!stopWords.contains(curr_word)){
                    word.set(curr_word);
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class Mapper1 extends Mapper<Text, Text, Text, Text> {
        private Text frequency = new Text();
        private Text word = new Text();
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            word.set(key);
            frequency.set(value);
            context.write(key, value);

        }
    }

    public static class Mapper2 extends Mapper<Text, Text, Text, Text> {
        private Text frequency = new Text();
        private Text word = new Text();
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            word.set(key);
            frequency.set(value);
            context.write(key, value);
        }

    }

    public static class CommonWordsCount extends Reducer<Text, Text, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable commoncount = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int k = 0;
            int frequency1 = 0;
            int frequency2 = 0;
            int commonFreq = 0;
            for (Text value : values) {
                k++;
                if (k == 1) {
                    frequency1 = Integer.parseInt(value.toString());
                } else if (k == 2) {
                    frequency2 = Integer.parseInt(value.toString());
                }
                if (k == 2) {
                    commonFreq = Math.min(frequency1, frequency2);
                    word.set(key);
                    commoncount.set(commonFreq);
                    context.write(word, commoncount);
                }
            }
        }
    }

    public static class SortMapper extends Mapper<Text, Text, IntWritable, Text>{
        private IntWritable count = new IntWritable();
        private Text word = new Text();

        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException{
            word.set(key);
            count.set(Integer.parseInt(value.toString()));
            context.write(count, word);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private IntWritable count = new IntWritable();
        private Text word = new Text();
        public static final int top = 20;
        public int size = 0;

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            //ToDo
            if(size < top) {
                count.set(key.get());
                for(Text value: values) {
                    word.set(value.toString());
                    context.write(count, word);
                    size++;
                    if(size >= top) {
                        break;
                    }
                }
            }
        }
    }

        //comparator --> descending sort
    public static class IntKeyDescComparator extends WritableComparator{
        protected IntKeyDescComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path("temp_output1"))){
            fs.delete(new Path("temp_output1"));
        }

        if (fs.exists(new Path("temp_output2"))){
            fs.delete(new Path("temp_output2"));
        }

        if (fs.exists(new Path("temp_output3"))){
            fs.delete(new Path("temp_output3"));
        }

        Job job1 = Job.getInstance(conf, "word count 1");
        job1.setMapperClass(TokenizerMapper.class);
        job1.setJarByClass(TopkCommonWords.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp_output1"));
        job1.addCacheFile(new Path(args[2]).toUri());
//        System.exit(job1.waitForCompletion(true) ? 0 : 1);
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "word count 2");
        job2.setMapperClass(TokenizerMapper.class);
        job2.setJarByClass(TopkCommonWords.class);
        job2.setReducerClass(IntSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path("temp_output2"));
        job2.waitForCompletion(true);

        Job job3 = new Job(conf, "count common wods");
        job3.setJarByClass(TopkCommonWords.class);
        job3.setReducerClass(CommonWordsCount.class);
        MultipleInputs.addInputPath(job3, new Path("temp_output1"), KeyValueTextInputFormat.class, Mapper1.class);
        MultipleInputs.addInputPath(job3, new Path("temp_output2"), KeyValueTextInputFormat.class, Mapper2.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job3, new Path("temp_output3"));
        job3.waitForCompletion(true);
        fs.delete(new Path("temp_output1"),true);
        fs.delete(new Path("temp_output2"),true);

        Job job4 = new Job(conf, "sort");
        job4.setJarByClass(TopkCommonWords.class);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setMapperClass(SortMapper.class);
        job4.setSortComparatorClass(IntKeyDescComparator.class);
        job4.setReducerClass(SortReducer.class);
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(Text.class);
        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job4, new Path("temp_output3"));
        FileOutputFormat.setOutputPath(job4, new Path(args[3]));
        job4.waitForCompletion(true);
        fs.delete(new Path("temp_output3"),true);
        System.exit(0);
    }
}
