package org.wordCount;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WordMain {

	// private static List<String> secondDir = new ArrayList<String>();

	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		//下面两行很重要
		conf.set("mapred.jar", "E://eclipse//jar-work//WordMain.jar");
		conf.set("mapred.job.tracker", "192.168.190.128:9001");
		
		//设置单词先验概率的保存路径
		String priorProbality = "hdfs://192.168.190.128:9000/user/hadoop/output/priorP/priorProbability.txt";
		conf.set("priorProbality", priorProbality);
		
		//单词总种类数的保存路径
		String totalWordsPath = "hdfs://192.168.190.128:9000/user/hadoop/output/totalwords.txt";
		conf.set("totalWordsPath", totalWordsPath);
		
		//每个类别中单词总数
		String wordsInClassPath = "hdfs://192.168.190.128:9000/user/hadoop/mid/wordsFrequence/_wordsInClass/wordsInClass-r-00000";
		conf.set("wordsInClassPath", wordsInClassPath);
		
		//设置输入 和 单词词频的输出路径
		// "/user/hadoop/input/NBCorpus/Country"
		String input = "hdfs://192.168.190.128:9000/user/hadoop/input/NBCorpus/Country";
		String wordsOutput = "hdfs://192.168.190.128:9000/user/hadoop/mid/wordsFrequence";
		conf.set("input", input);
		conf.set("wordsOutput", wordsOutput);
		
		//每个类别单词概率保存路径,
		//单词词频的输入路径也就是单词词频的输出路径
		
		String freqOutput = "hdfs://192.168.190.128:9000/user/hadoop/output/probability/";
		conf.set("freqOutput", freqOutput);

		
		
		
		FileCount.run(conf);
		WordCount.run(conf);
		Probability.run(conf);
/*		
		System.out.print("----------");
		
		
		String[] otherArgs = new String[] { "hdfs://192.168.190.128:9000/user/hadoop/test/",
				"hdfs://192.168.190.128:9000/user/hadoop/wordcount/output2/" };
		conf.set("mapred.jar", "E://eclipse//jar-work//WordMain.jar");
	
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordMain.class);

		job.setInputFormatClass(MyInputFormat.class);

		job.setMapperClass(WordMapper.class);
//		job.setCombinerClass(WordReducer.class);
		job.setReducerClass(WordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// MyUtils.addInputPath(job, new Path(otherArgs[0]), conf);

		
		
		List<Path> inputPaths = getSecondDir(conf, otherArgs[0]);
		for (Path path : inputPaths) {
			System.out.println("path = " + path.toString());
			MyInputFormat.addInputPath(job, path);
			
		}
		System.out.println("addinputpath 	ok" );
//		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		


		
		System.exit(job.waitForCompletion(true) ? 0 : 1);*/
		
		
	}

	// 获取文件夹下面二级文件夹路径的方法
	static List<Path> getSecondDir(Configuration conf, String folder) throws Exception {
		Path path = new Path(folder);
		FileSystem fs = path.getFileSystem(conf);
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

	

	

}
