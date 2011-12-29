import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;


public class MovieGenericParser {

	private String id;
	private String name;
	private int year;
	
	public void parse(String value) {

		StringTokenizer wordLists = new StringTokenizer(value.toString(),"\t");

        id = wordLists.nextToken();
        name = wordLists.nextToken();
        year = Integer.parseInt(wordLists.nextToken());
        
	}
	public void parse(Text record) {
		parse(record.toString());
	}
	
	public String getId() {
      	return id;
	}
	
    public int getYear() {
       	return year; 
    }
    
    public String getName() {
		return name;
    }
}