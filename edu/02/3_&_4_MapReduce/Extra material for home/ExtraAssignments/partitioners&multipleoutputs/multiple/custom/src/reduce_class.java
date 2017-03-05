
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class reduce_class extends Reducer<Text, name, NullWritable, Text> {
	public void reduce(Text key,Iterable<name> values,Context context) throws IOException, InterruptedException{
		MultipleOutputs<NullWritable,Text> m = new MultipleOutputs<NullWritable,Text>(context);
		long pat;
		String n;
		NullWritable out = NullWritable.get();
		TreeMap<Long,ArrayList<String>> map = new TreeMap<Long,ArrayList<String>>();
		for(name nn : values){
			pat = nn.patent_No.get();
			if(map.containsKey(pat))
				map.get(pat).add(nn.getName().toString());
			else{
				map.put(pat,(new ArrayList<String>()));
				map.get(pat).add(nn.getName().toString());}
		}
		for(Map.Entry entry : map.entrySet()){
			n = entry.getKey().toString();
			m.write(out, new Text("--------------------------"), key.toString());
			m.write(out, new Text(n), key.toString());
			ArrayList<String> names = (ArrayList)entry.getValue();
			Iterator i = names.iterator();
			while(i.hasNext()){
				n = (String)i.next();
				m.write(out, new Text(n), key.toString());
			}
			m.write(out, new Text("--------------------------"), key.toString());			
		}	
		m.close();
	}	
}
