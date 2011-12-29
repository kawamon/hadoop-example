import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovieLensDriver extends Configured implements Tool {

	public static class KeyPartitioner implements Partitioner<TextPair, Text> {
		public void configure(JobConf job) {}
		
		public int getPartion(TextPair key, Text value, int numPartitions) {
			return(key.getFirst().hashCode()& Integer.MAX_VALUE) % numPartitions;
		}

		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			return 0;
		}
	}

	public int run(String[] args) throws Exception {
	
		if (args.length != 3) {
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		JobConf conf = new JobConf(getConf(), MovieLensDriver.class);
		conf.setJobName("MovieLensJoin");
		
		conf.setMapOutputKeyClass(TextPair.class);
		conf.setMapOutputValueClass(Text.class);
		//conf.setInputFormat(KeyValueTextInputFormat.class);

		conf.setReducerClass(JoinReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setPartitionerClass(KeyPartitioner.class);
		conf.setOutputValueGroupingComparator(FirstComparator.class);

		Path movieInputPath = new Path(args[0]);
		Path movieRatingInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		MultipleInputs.addInputPath(conf,movieInputPath,TextInputFormat.class, MovieMapper.class);
		MultipleInputs.addInputPath(conf,movieRatingInputPath,TextInputFormat.class, MovieRatingMapper.class);
		FileOutputFormat.setOutputPath(conf, outputPath);
		


		JobClient.runJob(conf);
		return 0;
	}
		
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new MovieLensDriver(), args);
		System.exit(exitCode);
	}
}
