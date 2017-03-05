import java.io.IOException;


public class Main {
	public static void main(String[] args) throws IOException {
		HDFSClient client = new HDFSClient();
		
		if (args.length < 1) {
			client.printUsage();
			System.exit(1);
		}

		
		if (args[0].equals("add")) {
			if (args.length < 3) {
				System.out.println("Usage: hdfsclient add_file <local_path> <hdfs_path>");
				System.exit(1);
			}
			client.add_file(args[1], args[2]);

		} else if (args[0].equals("read")) {
			if (args.length < 2) {
				System.out.println("Usage: hdfsclient read <hdfs_path>");
				System.exit(1);
			}
			client.read(args[1]);

		} else if (args[0].equals("delete")) {
			if (args.length < 2) {
				System.out.println("Usage: hdfsclient delete <hdfs_path>");
				System.exit(1);
			}

			client.delete(args[1]);
		} else if (args[0].equals("mkdir")) {
			if (args.length < 2) {
				System.out.println("Usage: hdfsclient mkdir <hdfs_path>");
				System.exit(1);
			}

			client.mkdir(args[1]);
		} else if (args[0].equals("CopyFromLocal")) {
			if (args.length < 3) {
				System.out
						.println("Usage: hdfsclient copyfromlocal <from_local_path> <to_hdfs_path>");
				System.exit(1);
			}

			client.CopyFromLocal(args[1], args[2]);
		} else if (args[0].equals("rename")) {
			if (args.length < 3) {
				System.out
						.println("Usage: hdfsclient rename <old_hdfs_path> <new_hdfs_path>");
				System.exit(1);
			}

			client.rename_file(args[1], args[2]);
		} else if (args[0].equals("CopyToLocal")) {
			if (args.length < 3) {
				System.out
						.println("Usage: hdfsclient copytolocal <from_hdfs_path> <to_local_path>");
				System.exit(1);
			}

			client.CopyToLocal(args[1], args[2]);
		} else {
			client.printUsage();
			System.exit(1);
		}
		System.out.println("Done!");
	}
}
