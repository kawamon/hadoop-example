import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MovieMapper extends MapReduceBase 
		implements Mapper<Object, Text, TextPair, Text> {
	
	private MovieGenericParser parser = new MovieGenericParser();
	
	public void map(Object key, 
			Text value, 
			OutputCollector<TextPair, Text> output, 
			Reporter reporter) throws IOException {

		parser.parse(value);
		output.collect(new TextPair(parser.getId(),"0"), value);
	}
}
