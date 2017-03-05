
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class name implements Writable {
	Text first = null;
	Text middle = null;
	Text last = null;
	LongWritable patent_No = null;

	public name(){
		this.first = new Text();
		this.middle = new Text();
		this.last = new Text();
		this.patent_No = new LongWritable();
	}
	public name(Text f,Text m,Text l,LongWritable c){
		this.first = f;
		this.middle = m;
		this.last = l;
		this.patent_No = c;
	}
	public name(String f,String m,String l,long c){
		this.first = new Text(f);
		this.middle = new Text(m);
		this.last = new Text(l);
		this.patent_No = new LongWritable(c);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		middle.readFields(in);
		last.readFields(in);
		patent_No.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		middle.write(out);
		last.write(out);
		patent_No.write(out);
	}
	public Text getName(){
		return new Text(first.toString()+" "+middle.toString()+" "+last.toString());
	}
}
