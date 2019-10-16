import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.json.*;

public class RedditAverage extends Configured implements Tool {
	
	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private final static long one = 1;
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			JSONObject record = new JSONObject(value.toString());
			
			String subReddit= (String)record.get("subreddit");
			int score=(Integer)record.get("score");
			
			System.out.print(subReddit+score);
			
			LongPairWritable pair = new LongPairWritable();
			pair.set(one, score);

			//System.out.println(pair.get_0()); // 2
			//System.out.println(pair.get_1()); // 9

			
			/*for(int i=0;i<arr.length;i++)*/
				word.set(subReddit);
				context.write(word,pair);
			
		}
	}

	
	public static class LongPairCombiner
	extends Reducer<Text,LongPairWritable,Text,LongPairWritable>{
		LongPairWritable result = new LongPairWritable();
		
		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long sum1=0;
			long sum2=0;
			for (LongPairWritable val : values) {
				sum1 += val.get_0();
				sum2 += val.get_1();
			}
			result.set(sum1,sum2);
			context.write(key,result);
		}
	}
	
	
	
	public static class DoubleSumReducer
	extends Reducer<Text,LongPairWritable,Text,DoubleWritable>{
		DoubleWritable result = new DoubleWritable();
		
		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			double avg = 0;
		        long sum1=0;
			long sum2=0;
			for (LongPairWritable val : values){
				sum1 += val.get_0();
				sum2 += val.get_1();	
			}
			avg=((double)sum2/(double)sum1);
			result.set(avg);
			context.write(key, result);
		}
	}
	






	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "RedditAverage");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongPairCombiner.class);
		job.setReducerClass(DoubleSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
