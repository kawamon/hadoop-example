import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class JoinReducer extends MapReduceBase implements Reducer<TextPair, Text, Text, Text> {
	private MovieGenericParser parserM = new MovieGenericParser();
	private MovieRatingGenericParser parserR = new MovieRatingGenericParser();
    private FloatWritable avg = new FloatWritable();

	public void reduce(TextPair key, Iterator<Text> values, 
	             OutputCollector<Text, Text> output, Reporter reporter)
	                    throws IOException {

		int numRatings=0;
		int totalRating = 0;
		// Fetch Movie Name and year from MovieGenericParser
		parserM.parse(values.next());		
		Text outKey = new Text(parserM.getId() + "\t" + parserM.getName() + "\t" + parserM.getYear());

		while (values.hasNext()) {
			numRatings++;
			parserR.parse(values.next());
			totalRating += parserR.getRating();
		}
        avg.set((float) totalRating / numRatings);
		Text outValue = new Text(numRatings + "\t" + avg);
        output.collect(outKey, outValue);
       }
}