
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

enum missing{COUNTRY,
			FIRST,
			MIDDLE,
			LAST
			}
enum Total{COUNT,
			WRITTEN,
			SKIPPED}
public class map_class extends Mapper<LongWritable, Text, Text, name> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		StringTokenizer tokens = new StringTokenizer(line,",");
		long pat_no = 0;
		String last=" ",first=" ",middle=" ",country=" ";
		String token=null;
		token = tokens.nextToken();
		if(token.length()!=8){
			pat_no = Long.parseLong(token.substring(0, token.length()));
			token = tokens.nextToken();
			if(token.length()>1)
				last = token.substring(1, token.length()-1);
			else
				context.getCounter(missing.LAST).increment(1);
			token = tokens.nextToken();
			if(token.length()>1)
				first = token.substring(1, token.length()-1);
			else
				context.getCounter(missing.FIRST).increment(1);
			token = tokens.nextToken();
			if(token.length()>1)
				middle = token.substring(1, token.length()-1);
			else
				context.getCounter(missing.MIDDLE).increment(1);
			for(int i  = 0 ;i < 5;i++){
				token = tokens.nextToken();
			}
			if(token.length()>1)
				country = token.substring(1, token.length()-1);
			else
				context.getCounter(missing.COUNTRY).increment(1);
			
			name n = new name(first,middle,last,pat_no);
			context.write(new Text(country),n);
			context.getCounter(Total.WRITTEN).increment(1);}
		else
			context.getCounter(Total.SKIPPED).increment(1);
		context.getCounter(Total.COUNT).increment(1);
	}

}
