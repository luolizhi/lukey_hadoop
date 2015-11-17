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
 * 一次将需要的结果都统计到对应的文件夹中 AFRICA 484017newsML.txt afford 1
 * 
 * 按照这个格式输出给后面处理得到需要的： 1. AFRICA 484017newsML.txt AFRICA 487141newsML.txt
 * 类别中的文本数， ---> 计算先验概率(单独解决这个) 所有类别中的文本总数， ---> 可以由上面得到，计算先验概率
 * 
 * 2. AFRICA afford 1 AFRICA boy 3 每个类中的每个单词的个数，---> 计算各个类中单词的概率
 * 
 * 3. AFRICA 768 类中单词总数， ---> 将2中的第一个key相同的第三个数相加即可
 * 
 * 4. AllWORDS 12345 所有类别中单词种类数 ---> 将1中的第三个key归并，计算个数
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
		// 设置不同文件的路径

		// "/user/hadoop/input/NBCorpus/Country"
		String[] other = { "/user/hadoop/test11", "/user/hadoop/mid11/wordsFre1" };

		Job job = new Job(conf, "file count");

		job.setJarByClass(MyWordCount.class);

//		job.setInputFormatClass(CombineSmallfileInputFormat.class);

		job.setMapperClass(First_Mapper.class);
		job.setReducerClass(First_Reducer.class);

		// 过滤掉文本数少于10的类别
		List<Path> inputPaths = getSecondDir(conf, other[0]);
		for (Path path : inputPaths) {
			FileInputFormat.addInputPath(job, path);
		}

		FileOutputFormat.setOutputPath(job, new Path(other[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		int exitCode = job.waitForCompletion(true) ? 0 : 1;

		// 调用计数器
		Counters counters = job.getCounters();
		Counter c1 = counters.findCounter(WordsNature.TOTALWORDS);
		System.out.println("-------------->>>>: " + c1.getDisplayName() + ":" + c1.getName() + ": " + c1.getValue());

		// 将单词种类数写入文件中
		Path totalWordsPath = new Path("/user/hadoop/output11/totalwords.txt");
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream outputStream = fs.create(totalWordsPath);
		outputStream.writeBytes(c1.getDisplayName() + ":" + c1.getValue());
		IOUtils.closeStream(outputStream);

		// 下次求概率是尝试单词总种类数写到configuration中
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
				// 文件名
				String fileName = path.getName();

				// 文件夹名(即类别名)
				String dirName = path.getParent().getName();

				// Pattern.compile("-?[0-9]+.?[0-9]+");//剔除含数字的单词正则表达式

				// 可以加入停用词处理
				if (!(hasDigit(value.toString()) || value.toString().contains("."))) {// 不含数字的单词

					className.set(dirName + "\t" + value.toString());
					countryName.set(dirName + "\t" + fileName + "\t" + value.toString());

					context.write(className, one); // 每个类别的每个单词数 // ABDBI hello
													// 1
					context.write(new Text(dirName), one);// 统计每个类中的单词总数
															// //ABDBI 1
					context.write(value, zero); // 用于统计所有类中单词个数

				}
			}
		}
	}

	// Reducer
	static class First_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// result 表示每个类别中每个单词的个数
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

			// sum为0，总得单词数加1，统计所有单词的种类
			if (sum == 0) {
				context.getCounter(WordsNature.TOTALWORDS).increment(1);
			} else {// sum不为0时，通过key的长度来判断，
				String[] temp = key.toString().split("\t");
				if (temp.length == 2) { // 用tab分隔类别和单词
					result.set(sum);
					context.write(key, result);
					// mos.write(new Text(temp[1]), result, temp[0]);
				} else { // 类别中单词总数
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

	// 获取文件夹下面二级文件夹路径的方法
	static List<Path> getSecondDir(Configuration conf, String folder) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folder);
		FileStatus[] stats = fs.listStatus(path);
		List<Path> folderPath = new ArrayList<Path>();
		for (FileStatus stat : stats) {
			if (stat.isDir()) {
				if (fs.listStatus(stat.getPath()).length > 10) { // 筛选出文件数大于10个的类别作为
																	// 输入路径
					folderPath.add(stat.getPath());
				}
			}
		}
		return folderPath;
	}

	// 判断一个字符串是否含有数字
	static boolean hasDigit(String content) {

		boolean flag = false;

		Pattern p = Pattern.compile(".*\\d+.*");

		Matcher m = p.matcher(content);

		if (m.matches())

			flag = true;

		return flag;

	}

}