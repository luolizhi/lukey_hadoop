package org.lueky.hadoop.bayes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * 
 * һ�ν���Ҫ�Ľ����ͳ�Ƶ���Ӧ���ļ����� AFRICA 484017newsML.txt afford 1
 * 
 * ���������ʽ��������洦��õ���Ҫ�ģ� 1. AFRICA 484017newsML.txt AFRICA 487141newsML.txt
 * ����е��ı����� ---> �����������(����������) ��������е��ı������� ---> ����������õ��������������
 * 
 * 2. AFRICA afford 1 AFRICA boy 3 ÿ�����е�ÿ�����ʵĸ�����---> ����������е��ʵĸ���
 * 
 * 3. AFRICA 768 ���е��������� ---> ��2�еĵ�һ��key��ͬ�ĵ���������Ӽ���
 * 
 * 4. AllWORDS 12345 ��������е��������� ---> ��1�еĵ�����key�鲢���������
 *
 */

public class MyWordCount {

	private static MultipleOutputs<Text, IntWritable> mos;

	static enum WordsNature {
		CLSASS_NUMBER, CLASS_WORDS, TOTALWORDS
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.set("mapred.job.tracker", "192.168.190.128:9001");
		conf.set("mapred.jar", "E://eclipse//jar-work//WordMain.jar");
		// ���ò�ͬ�ļ���·��

		// "/user/hadoop/input/NBCorpus/Country"
		String[] other = { "/user/hadoop/test11", "/user/hadoop/mid11/wordsFre1" };

		Job job = new Job(conf, "file count");

		job.setJarByClass(MyWordCount.class);

//		job.setInputFormatClass(CombineSmallfileInputFormat.class);

		job.setMapperClass(First_Mapper.class);
		job.setReducerClass(First_Reducer.class);

		// ���˵��ı�������10�����
		List<Path> inputPaths = getSecondDir(conf, other[0]);
		for (Path path : inputPaths) {
			FileInputFormat.addInputPath(job, path);
		}

		FileOutputFormat.setOutputPath(job, new Path(other[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		int exitCode = job.waitForCompletion(true) ? 0 : 1;

		// ���ü�����
		Counters counters = job.getCounters();
		Counter c1 = counters.findCounter(WordsNature.TOTALWORDS);
		System.out.println("-------------->>>>: " + c1.getDisplayName() + ":" + c1.getName() + ": " + c1.getValue());

		// ������������д���ļ���
		Path totalWordsPath = new Path("/user/hadoop/output11/totalwords.txt");
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream outputStream = fs.create(totalWordsPath);
		outputStream.writeBytes(c1.getDisplayName() + ":" + c1.getValue());
		IOUtils.closeStream(outputStream);

		// �´�������ǳ��Ե�����������д��configuration��
		//
		// conf.set("TOTALWORDS", totalWords.toString());

		System.exit(exitCode);

	}

	// Mapper
	static class First_Mapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable zero = new IntWritable(0);

		private Text className = new Text();
		private Text countryName = new Text("hello");

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			CombineFileSplit fileSplit = (CombineFileSplit) context.getInputSplit();

			Path[] paths = fileSplit.getPaths();
			for (Path path : paths) {
				// �ļ���
				String fileName = path.getName();

				// �ļ�����(�������)
				String dirName = path.getParent().getName();

				// Pattern.compile("-?[0-9]+.?[0-9]+");//�޳������ֵĵ���������ʽ

				// ���Լ���ͣ�ôʴ���
				if (!(hasDigit(value.toString()) || value.toString().contains("."))) {// �������ֵĵ���

					className.set(dirName + "\t" + value.toString());
					countryName.set(dirName + "\t" + fileName + "\t" + value.toString());

					context.write(className, one); // ÿ������ÿ�������� // ABDBI hello
													// 1
					context.write(new Text(dirName), one);// ͳ��ÿ�����еĵ�������
															// //ABDBI 1
					context.write(value, zero); // ����ͳ���������е��ʸ���

				}
			}
		}
	}

	// Reducer
	static class First_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// result ��ʾÿ�������ÿ�����ʵĸ���
		IntWritable result = new IntWritable();
		Map<String, List<String>> classMap = new HashMap<String, List<String>>();
		Map<String, List<String>> fileMap = new HashMap<String, List<String>>();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			// sumΪ0���ܵõ�������1��ͳ�����е��ʵ�����
			if (sum == 0) {
				context.getCounter(WordsNature.TOTALWORDS).increment(1);
			} else {// sum��Ϊ0ʱ��ͨ��key�ĳ������жϣ�
				String[] temp = key.toString().split("\t");
				if (temp.length == 2) { // ��tab�ָ����͵���
					result.set(sum);
					context.write(key, result);
					// mos.write(new Text(temp[1]), result, temp[0]);
				} else { // ����е�������
					result.set(sum);
					mos.write(key, result, "_wordsInClass" + "\\" + "wordsInClass");
				}

			}

		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos.close();
		}

		@Override
		protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}

	}

	// ��ȡ�ļ�����������ļ���·���ķ���
	static List<Path> getSecondDir(Configuration conf, String folder) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folder);
		FileStatus[] stats = fs.listStatus(path);
		List<Path> folderPath = new ArrayList<Path>();
		for (FileStatus stat : stats) {
			if (stat.isDir()) {
				if (fs.listStatus(stat.getPath()).length > 10) { // ɸѡ���ļ�������10���������Ϊ
																	// ����·��
					folderPath.add(stat.getPath());
				}
			}
		}
		return folderPath;
	}

	// �ж�һ���ַ����Ƿ�������
	static boolean hasDigit(String content) {

		boolean flag = false;

		Pattern p = Pattern.compile(".*\\d+.*");

		Matcher m = p.matcher(content);

		if (m.matches())

			flag = true;

		return flag;

	}

}