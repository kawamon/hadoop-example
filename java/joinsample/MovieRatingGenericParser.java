import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;


public class MovieRatingGenericParser {

	private int userid;
	private String movieid;
	private int rating;
	
	public void parse(String record) {

		StringTokenizer wordLists = new StringTokenizer(record.toString(),"\t");

        userid = Integer.parseInt(wordLists.nextToken());
        movieid = wordLists.nextToken();
        rating = Integer.parseInt(wordLists.nextToken());
        
	}
	
	public void parse(Text record) {
		parse(record.toString());
	}
	
	public int getUserId() {
      	return userid;
	}
	
    public String getMovieId() {
       	return movieid; 
    }
    
    public int getRating() {
		return rating;
    }
}