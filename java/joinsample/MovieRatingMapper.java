import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MovieRatingMapper extends MapReduceBase 
		implements Mapper<Object, Text, TextPair, Text> {
	private MovieRatingGenericParser parser = new MovieRatingGenericParser();
	
	public void map(Object key, 
			Text value, 
			OutputCollector<TextPair, Text> output, 
			Reporter reporter) throws IOException {

		parser.parse(value);
		output.collect(new TextPair(parser.getMovieId(),"1"), value);
	}
}
