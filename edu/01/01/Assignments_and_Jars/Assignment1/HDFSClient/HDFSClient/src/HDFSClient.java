

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;


public class HDFSClient {

	private static String HDFS_Host_name="hdfs://cloudera-vm/";
	
	public void printUsage(){
		System.out.println("Usage: hdfsclient read <hdfs_path>");
		System.out.println("Usage: hdfsclient delete <hdfs_path>");
		System.out.println("Usage: hdfsclient mkdir <hdfs_path>");
		System.out.println("Usage: hdfsclient rename_file <hdfs_path>");
		System.out.println("Usage: hdfsclient add_file <local_path> <hdfs_path>");
		System.out.println("Usage: hdfsclient CopyToLocal <hdfs_path> <local_path>");
		System.out.println("Usage: hdfsclient CopyFromLocal <local_path> <hdfs_path>");	
	}
	public void read(String file) throws IOException{
		Path path = new Path(HDFS_Host_name+file);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(path.toUri(),conf);
		if(!fs.exists(path)){
			System.out.println("File "+ file +" does not exists!");
			return;
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		String s = br.readLine();
		while(s!=null){
			System.out.println(s);
			s = br.readLine();
		}
		br.close();
		fs.close();
	}
	public void delete(String file) throws IOException{
		Path path = new Path(HDFS_Host_name+file);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(path.toUri(),conf);
		if(!fs.exists(path)){
			System.out.println("File "+ file +" does not exists!");
			return;
		}
		fs.delete(path, true);
		fs.close();
	}
	public void mkdir(String file) throws IOException{
		Path path = new Path(HDFS_Host_name+file);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(path.toUri(),conf);
		if(fs.exists(path)){
			System.out.println("Dir. "+ file +" already exists");
			return;
		}
		fs.mkdirs(path);
		fs.close();
	}
	public void rename_file(String from_file,String to_file) throws IOException{
		Path from_path = new Path(HDFS_Host_name+from_file);
		Path to_path = new Path(HDFS_Host_name+to_file);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(from_path.toUri(),conf);
		if(!fs.exists(from_path)){
			System.out.println("File "+ from_file +" does not exists");
			return;
		}
		if(fs.exists(to_path)){
			System.out.println("File "+ to_file +" already exists");
			return;
		}
		fs.rename(from_path, to_path);
		fs.close();
	}
	public void add_file(String source,String dest) throws IOException{
		Path path = new Path(HDFS_Host_name+dest);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(path.toUri(),conf);
		if(fs.exists(path)){
			System.out.println("File "+ dest +" already exits");
			return;
		}
		FSDataOutputStream out = fs.create(path);
		InputStream in= new BufferedInputStream(new FileInputStream(new File(source)));
		byte[] b =new byte[1024];
		int numbytes=0;
		while((numbytes=in.read(b))>0){
			out.write(b, 0, numbytes);
		}
		in.close();
		out.close();
		fs.close();
	}
	public void CopyToLocal(String source,String dest) throws IOException{
		Path path1 = new Path(HDFS_Host_name+source);
		Configuration conf = new Configuration();
		FileSystem fs1 = FileSystem.get(path1.toUri(),conf);
		if(!fs1.exists(path1)){
			System.out.println("File "+ source +" does not exists");
			return;
		}
		Path path2 = new Path(dest);
		FileSystem fs2 = FileSystem.get(path2.toUri(),conf);
		if(fs2.exists(path2)){
			System.out.println("File "+ dest +" already exists");
			return;
		}
		FileUtil.copy(fs1, path1, new File(dest), false, conf);
	}
	public void CopyFromLocal(String source, String dest) throws IOException {

		Path destPath = new Path(HDFS_Host_name + dest);
		Configuration conf = new Configuration();
		FileSystem hdfsfileSystem = FileSystem.get(destPath.toUri(), conf);
		
		Path srcPath = new Path(source);
		FileSystem localfileSystem = FileSystem.get(srcPath.toUri(),conf);
		try {
		
			ArrayList<Path> srcPaths = getSourcePaths(localfileSystem, srcPath);
			Path[] sourcePaths = srcPaths.toArray(new Path[srcPaths.size()]);
			FileUtil.copy(localfileSystem, sourcePaths, hdfsfileSystem, destPath, false, false, conf);
			System.out.println("File " + srcPaths + "copied to " + dest);
		} catch (Exception e) {
			System.err.println("Exception caught! :" + e);
			e.printStackTrace();
			System.exit(1);
		} finally {
			localfileSystem.close();
			hdfsfileSystem.close();
		}
	}
	
	private ArrayList<Path> getSourcePaths(FileSystem localfileSystem, Path srcPath) throws Exception{
		ArrayList<Path> sourcePaths = new ArrayList<Path>();
		for (FileStatus file : localfileSystem.listStatus(srcPath)){
			if(file.isDir()){
				sourcePaths.addAll(getSourcePaths(localfileSystem,file.getPath()));
			}
			else
				sourcePaths.add(file.getPath());
		}
		return sourcePaths;
	}
}