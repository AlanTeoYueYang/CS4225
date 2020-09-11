//assignment1 task1 commonwords java
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class TopkCommonWords {

	public static class TokenizerWCMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		Set<String> stopwords = new HashSet<String>();
		private static final String STOP_WORD_PATH = "assignment1_task1/stopwords.txt";

		@Override
		protected void setup(Context context) {
			try {
				Path path = new Path(STOP_WORD_PATH);
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(path)));
				String word = null;
				while ((word = br.readLine()) != null) {
					stopwords.add(word.toLowerCase());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			//delimiter: " \t\n\r\f"
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				if (stopwords.contains(word.toString().toLowerCase()))
					continue;
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable count = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
						   Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			count.set(sum);
			context.write(key, count);
		}
	}

	//Mapper1
	public static class Mapper1 extends Mapper<Text, Text, Text, Text> {
		private Text frequency = new Text();
		private Text word = new Text();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			//ToDo
			//System.out.println("key: "+key+" value: "+value);
			word.set(key);
			frequency.set(value);
			context.write(key, value);

		}
	}

	//Mapper2
	public static class Mapper2 extends Mapper<Text, Text, Text, Text> {
		private Text frequency = new Text();
		private Text word = new Text();
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			//ToDo
			word.set(key);
			frequency.set(value);
			context.write(key, value);
		}

	}

	//Get the number of common words reduce
	public static class CountCommonReducer extends
			Reducer<Text, Text, Text, IntWritable> {

		private Text word = new Text();
		private IntWritable commoncount = new IntWritable();

		public void reduce(Text key, Iterable<Text> values,
						   Context context) throws IOException, InterruptedException {

			//ToDo
			int k = 0;
			int frequency1 = 0;
			int frequency2 = 0;
			int commonFreq = 0;
			for(Text value: values) {
				k++;
				if (k == 1) {
					frequency1 = Integer.parseInt(value.toString());
				}else if(k == 2) {
					frequency2 = Integer.parseInt(value.toString());
				}
			}
			if(k == 2) {
				commonFreq = Math.min(frequency1, frequency2);
				word.set(key);
				commoncount.set(commonFreq);
				context.write(word, commoncount);
			}

		}

	}

	//sort the result
	public static class SortMapper extends Mapper<Text, Text, IntWritable, Text> {
		private IntWritable count = new IntWritable();
		private Text word = new Text();

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			//ToDo
			word.set(key);
			count.set(Integer.parseInt(value.toString()));
			context.write(count, word);
		}
	}

	public static class SortReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		private IntWritable count = new IntWritable();
		private Text word = new Text();
		public static final int NUM = 15;
		public int size = 0;

		public void reduce(IntWritable key, Iterable<Text> values,
						   Context context) throws IOException, InterruptedException {
			//ToDo
			if(size < NUM) {
				count.set(key.get());
				for(Text value: values) {
					word.set(value.toString());
					context.write(count, word);
					size++;
					if(size >= NUM) {
						break;
					}
				}
			}
		}
	}

	public static class ReverseComparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public ReverseComparator() {
			super(Text.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return (-1)* TEXT_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof Text && b instanceof Text) {
				return (-1)*(((Text) a).compareTo((Text) b));
			}
			return super.compare(a, b);
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 6) {
			System.err
					.println("Usage: CommonWords <input1> <output1> <input2> <output2> "
							+ "<output3> <output4>");
			System.exit(2);
		}

		Job job1 = new Job(conf, "WordCount1");
		job1.setJarByClass(CommonWords.class);
		job1.setMapperClass(TokenizerWCMapper.class);
		job1.setCombinerClass(IntSumReducer.class);
		job1.setReducerClass(IntSumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "WordCount2");
		job2.setJarByClass(CommonWords.class);
		job2.setMapperClass(TokenizerWCMapper.class);
		job2.setCombinerClass(IntSumReducer.class);
		job2.setReducerClass(IntSumReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
		job2.waitForCompletion(true);

		Job job3 = new Job(conf, "Count words in common");
		job3.setJarByClass(CommonWords.class);
		job3.setReducerClass(CountCommonReducer.class);
		MultipleInputs.addInputPath(job3, new Path(otherArgs[1]),
				KeyValueTextInputFormat.class, Mapper1.class);
		MultipleInputs.addInputPath(job3, new Path(otherArgs[3]),
				KeyValueTextInputFormat.class, Mapper2.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		job3.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));
		job3.waitForCompletion(true);

		Job job4 = new Job(conf, "sort");
		job4.setJarByClass(CommonWords.class);
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.setMapperClass(SortMapper.class);
		job4.setSortComparatorClass(ReverseComparator.class);
		job4.setReducerClass(SortReducer.class);
		job4.setOutputKeyClass(IntWritable.class);
		job4.setOutputValueClass(Text.class);
		job4.setMapOutputKeyClass(IntWritable.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job4, new Path(otherArgs[4]));
		FileOutputFormat.setOutputPath(job4, new Path(otherArgs[5]));
		System.exit(job4.waitForCompletion(true) ? 0 : 1);
	}

}