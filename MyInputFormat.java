package org.lukey.hadoop.bayes.trainning;

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
		Path[] paths = getInputPaths(context);	//paths�����ÿ������·�����ļ��е�ַ
		List<InputSplit> splits = new ArrayList<InputSplit>();
//		System.out.println("-6----paths = " + paths.length);
		
		for (Path path : paths) {	//ÿ���ļ�������������ļ���Ϊһ����Ƭ
//			System.out.println("path = " + path);
			FileSystem fileFS = path.getFileSystem(context.getConfiguration());
			Long len = (long) 0; // ÿ�����¼����Ƭ�ĳ��ȣ����ļ�������ÿ���ļ��ĳ���֮����Ϊ��Ƭ�ĳ���
			for (FileStatus f : fileFS.listStatus(path)) {
				len += f.getLen();
			}
			splits.add(new FileSplit(path, 0, len, null));	//û�п�����������Ϣ
		}
//		System.out.println("-7----split size = " + splits.size());
		return splits;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
//		System.out.println("-8---CreatRecordReader---");
		MyInputRecordReader reader = new MyInputRecordReader();	//�Լ������recordReader
		reader.initialize(split, context);
		return reader;
	}

	public static Path[] getInputPaths(JobContext context) {
		String dirs = context.getConfiguration().get("mapred.input.dir", "");
		String[] list = StringUtils.split(dirs);
		Path[] result = new Path[list.length];
		for (int i = 0; i < list.length; i++) {
			result[i] = new Path(StringUtils.unEscapeString(list[i]));
		}
//		System.out.println("-5----sum_map = " + list.length);
		return result;
	}

	public static void addInputPath(Job job, Path path) throws IOException {
		//�����е������ļ���·�����Ϊ����·�����ο�FileInputFormat�е�addInputPath
		Configuration conf = job.getConfiguration();
		path = path.getFileSystem(conf).makeQualified(path);
		String dirStr = StringUtils.escapeString(path.toString());

		String dirs = conf.get("mapred.input.dir");		
		conf.set("mapred.input.dir", dirs == null ? dirStr : dirs + "," + dirStr);
//		System.out.println("-4--addInputPath---ok---");
	}
}

class MyInputRecordReader extends RecordReader<Text, Text> {//���Ƶ�RecordReader
	private FileSplit filesplit;	//��������ķ�Ƭ��������ת����һ����key��value����¼
	private Configuration conf;		//���ö���
	private Text value = new Text();//value��������Ϊ��
	private Text key = new Text();	//key��������Ϊ��
	private boolean processed = false;//����������¼��¼�Ƿ񱻴����

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//		System.out.println("-9----initialize---");
		this.filesplit = (FileSplit) split;		//�������Ƭǿ��ת����FIleSplit
		this.conf = context.getConfiguration();	//��context�л�ȡ������Ϣ
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
//		System.out.println("-10----nextKeyValue---");
		if (!processed) {//�����¼û�д����
			//��fileSplit�����ȡsplit���ֽ���������byte����contents
			byte[] contents = new byte[(int) filesplit.getLength()];
			Path dirPath = filesplit.getPath(); // ��fileSplit�����ȡ�����ļ�·���������ļ���Ϊһ����Ƭ�������һ���ļ��е�·��
			key.set(dirPath.getName());// key��Ϊ������������
			FileSystem fs = dirPath.getFileSystem(conf);//��ȡ�ļ�ϵͳ����
			FSDataInputStream in = null;//�����ļ�����������
			FileStatus[] stats = fs.listStatus(dirPath); // ��ȡ�ļ�״̬��Ϣ
			int read = 0;
			for (FileStatus stat : stats) {	//�������ļ������ݶ�ȡ����	
				int fileLength = (int) stat.getLen();
				Path filePath = stat.getPath();
				FileSystem fsFile = filePath.getFileSystem(conf);
				
				try {
					in = fsFile.open(filePath);	//���ļ��������ļ�����������
					IOUtils.readFully(in, contents, read, fileLength);//���ļ����ݶ�ȡ��byte�����У�һ����Ϊvalueֵ
					read += fileLength;
				} finally {
					IOUtils.closeStream(in);//�ر�������
				}
			}
			value.set(contents, 0, contents.length);//��ǰ�ļ����е�����������Ϊvalueֵ
			processed = true;//�������־��Ϊtrue���´ε��ø÷����᷵��False
			return true;
		}
		return false;//�����¼�����������false����ʾsplit�������
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
//		System.out.println("-12----getCurrentKey---");
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
//		System.out.println("-13----getCurrentValue---");
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
//		System.out.println("-11----getProgress---");
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
//		System.out.println("-14----close---");
	}

}
