package solution;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * This is the SumReducer class from the word count exercise.
 */ 
public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  private MultipleOutputs<Text, IntWritable> multipleOutputs;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
	multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
  }

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int wordCount = 0;
    for (IntWritable value : values) {
      wordCount += value.get();
    }
    multipleOutputs.write(key, new IntWritable(wordCount), key.toString());
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
	multipleOutputs.close();
  }
}
