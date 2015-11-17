package org.wordCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

public class MyInputFormat extends InputFormat<Text, Text> {

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		Path[] paths = getInputPaths(context);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		System.out.println("paths = " + paths.length);
		for (Path path : paths) {
			System.out.println("path = " + path);
			// Long length = getFileContentsLength(path, conf);

			FileSystem fileFS = path.getFileSystem(context.getConfiguration());
			Long len = (long) 0; // 每次重新计算分片的长度，用文件夹下面每个文件的长度之和作为分片的长度
			for (FileStatus f : fileFS.listStatus(path)) {
				len += f.getLen();
			}
			System.out.println("split length = " + len);

			splits.add(new FileSplit(path, 0, len, null));
		}
		System.out.println("split size = " + splits.size());

		return splits;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		MyInputRecordReader reader = new MyInputRecordReader();
		reader.initialize(split, context);
		return reader;
	}

	// 获取目录下面文件的长度
	public static Long getFileContentsLength(Path path, Configuration conf) throws Exception {
		Long length = 0L;
		FileSystem fs = path.getFileSystem(conf);
		FileStatus[] stats = fs.listStatus(path);
		for (FileStatus stat : stats) {
			length += stat.getLen();
		}

		return length;

	}

	public static Path[] getInputPaths(JobContext context) {
		String dirs = context.getConfiguration().get("mapred.input.dir", "");
		String[] list = StringUtils.split(dirs);
		Path[] result = new Path[list.length];
		for (int i = 0; i < list.length; i++) {
			result[i] = new Path(StringUtils.unEscapeString(list[i]));
			System.out.println(result[i]);
		}
		System.out.println("sum_map = " + list.length);
		return result;
	}

	public static void addInputPath(Job job, Path path) throws IOException {
		Configuration conf = job.getConfiguration();
		path = path.getFileSystem(conf).makeQualified(path);
		String dirStr = StringUtils.escapeString(path.toString());

		String dirs = conf.get("mapred.input.dir");
		
		conf.set("mapred.input.dir", dirs == null ? dirStr : dirs + "," + dirStr);
	}

}

class MyInputRecordReader extends RecordReader<Text, Text> {
	private FileSplit filesplit;
	private Configuration conf;
	private Text value = new Text();
	private Text key = new Text();
	private boolean processed = false;
	// private FileStatus paths[];
	// private int index = 0;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.filesplit = (FileSplit) split;
		this.conf = context.getConfiguration();
		// paths =
		// filesplit.getPath().getFileSystem(conf).listStatus(filesplit.getPath());

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			byte[] contents = new byte[(int) filesplit.getLength()];

			Path dirPath = filesplit.getPath(); // 包含分片的路径，整个文件夹为一个分片，这就是一个文件夹的路径
			int read = 0;
			FileSystem fs = dirPath.getFileSystem(conf);

			FSDataInputStream in = null;

			FileStatus[] stats = fs.listStatus(dirPath); // 数组里面是一个个文件

			key.set(dirPath.getName());// 类别名

			for (FileStatus stat : stats) {
				int fileLength = (int) stat.getLen();
				Path filePath = stat.getPath();
				FileSystem fsFile = filePath.getFileSystem(conf);
				in = fsFile.open(filePath);
				try {
					// in = fs.open(filePath);
					IOUtils.readFully(in, contents, read, fileLength);
					read += fileLength;
				} finally {
					IOUtils.closeStream(in);
				}
			}
			value.set(contents, 0, contents.length);
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {

		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {

		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {

	}

}
