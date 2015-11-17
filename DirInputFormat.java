package org.lueky.hadoop.bayes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
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
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StringUtils;

public class DirInputFormat extends InputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		return new smallfileRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);

		Configuration conf = job.getConfiguration();

		for (FileStatus file : files) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(conf);
			Long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length); // 都是小文件只有一个块
			// 可以加判断blkLocations.length()==1
			splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));

		}

		return splits;
	}

	// 得到所有的输入路径
	public static Path[] getInputPaths(JobContext context) {
		String dirs = context.getConfiguration().get("mapred.input.dir", "");
		String[] list = StringUtils.split(dirs);
		Path[] result = new Path[list.length];
		for (int i = 0; i < list.length; i++) {
			result[i] = new Path(StringUtils.unEscapeString(list[i]));
		}
		return result;
	}

	// List是每个文件的信息
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		List<FileStatus> result = new ArrayList<FileStatus>();
		Path[] dirs = getInputPaths(job);
		if (dirs.length == 0) {
			throw new IOException("No input paths specified in job");
		}

		for (int i = 0; i < dirs.length; ++i) {
			Path p = dirs[i];
			FileSystem fs = p.getFileSystem(job.getConfiguration());
			FileStatus[] matches = fs.listStatus(p);
			if (matches.length == 0) {
				throw new IOException("Input Pattern " + p + " matches 0 files");
			} else {
				// 遍历文件状态数组
				for (FileStatus globStat : matches) {
					if (globStat.isDir()) {// 如果是文件夹继续遍历
						for (FileStatus stat : fs.listStatus(globStat.getPath())) {
							result.add(stat);// 将文件加入到List
						}
					} else {
						result.add(globStat);
					}
				}
			}
		}

		return result;
	}

	// 添加路径
	public static void addInputPath(Job job, Path path) throws IOException {
		Configuration conf = job.getConfiguration();
		path = path.getFileSystem(conf).makeQualified(path);
		String dirStr = StringUtils.escapeString(path.toString());
		String dirs = conf.get("mapred.input.dir");
		conf.set("mapred.input.dir", dirs == null ? dirStr : dirs + "," + dirStr);
	}

}

class smallfileRecordReader extends RecordReader<Text, Text> {

	private Text key;
	private Text value;
	private int index;
	private LineReader in = null;
	private Configuration conf;
	private FileSystem fs;
	private FileStatus[] stats;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Path path = split.getPath();
		conf = context.getConfiguration();
		fs = path.getFileSystem(conf);
		stats = fs.listStatus(path);
		key = new Text();
		value = new Text();

		index = 0;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		while (index < stats.length) {
			Path filePath = stats[index].getPath();
			key.set(filePath.getParent().getName()); // 直接用类别做key
			@SuppressWarnings("unused")
			int newSize = 0;
			StringBuffer values = new StringBuffer();
			FSDataInputStream inputStream = fs.open(filePath);
			in = new LineReader(inputStream, conf);

			while ((newSize = in.readLine(value)) > 0) {
				values.append(value.toString() + "\n");
			}
			value.set(values.toString());
			IOUtils.closeStream(inputStream);
			in = null;
			index++;
			return true;

		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (index == stats.length) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (index) / (float) (stats.length));
		}
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}