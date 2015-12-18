package org.lukey.hadoop.bayes.trainning;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WordCount {
	//������LOG��ӡ������Ϣ
	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

	//������������ͬ���ļ�����
	private static MultipleOutputs<Text, IntWritable> mos;
	//ʹ�ü�������ǰ������û���ú�
	static enum WordsNature {
		CLSASS_NUMBER, CLASS_WORDS, TOTALWORDS
	}

	// map
	static class First_Mapper extends Mapper<Text, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable zero = new IntWritable(0);

		private Text countryName = new Text();
	
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
//			System.out.println("-00--before map" + key + ":-----");//��logs/userlogs�е�stdout���
			LOG.info("--map000--- " + key + "---------");	//��logs/userlogs�е�syslog���
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				String word = itr.nextToken();
				
				if (!(MyUtils.hasDigit(word) || word.contains("."))) { // ȥ���������
					countryName.set(key.toString() + "\t" + word);
					
					context.write(countryName, one); // ͳ��ÿ������еĵ��ʸ��� ABL have 1
					context.write(key, one); // ͳ������еĵ�������
					context.write(new Text(word), zero); // ͳ�Ƶ�������				
				}				
			}
//			System.out.println("-00------�ҽ����� map---over" + key + ":");
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
			System.out.println("reduce reduce");
//			LOG.info("---reduce-- " + key);

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

//			result.set(sum);
//			context.write(key, result);
			
			// sumΪ0���ܵõ�������1��ͳ�����е��ʵ�����
			if (sum == 0) {
				context.getCounter(WordsNature.TOTALWORDS).increment(1);
			} else {// sum��Ϊ0ʱ��ͨ��key�ĳ������жϣ�
				result.set(sum);
				String[] temp = key.toString().split("\t");
				if (temp.length == 2) { // ��tab�ָ����͵���
					
					context.write(key, result);
					// mos.write(new Text(temp[1]), result, temp[0]);
				}
				
				else { // ����е�������	
					mos.write(key, result, "_wordsInClass" + "\\" + "wordsInClass");
				}
			}
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			mos.close();
		}

		@Override
		protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			mos = new MultipleOutputs<Text, IntWritable>(context);
		}

	}

	public static int run(Configuration conf) throws Exception {
		Job job = new Job(conf, "file count");

		job.setJarByClass(WordCount.class);

		job.setInputFormatClass(MyInputFormat.class);

		job.setMapperClass(WordCount.First_Mapper.class);
		job.setReducerClass(WordCount.First_Reducer.class);
//		System.out.println("-1--job-------");
		// ���˵��ı�������FILENUMBER�����

		String input = conf.get("input");
		
		List<Path> inputPaths = MyUtils.getSecondDir(conf, input);
		for (Path path : inputPaths) {
//			System.out.println("-3----path = " + path.toString());
			MyInputFormat.addInputPath(job, path);
		}

		String wordsOutput = conf.get("wordsOutput");
		FileOutputFormat.setOutputPath(job, new Path(wordsOutput));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		int exitCode = job.waitForCompletion(true) ? 0 : 1;

		// ���ü�����
		Counters counters = job.getCounters();
		Counter c1 = counters.findCounter(WordsNature.TOTALWORDS);
		System.out.println("------------>>>>: " + c1.getDisplayName() + ":" + c1.getName() + ": " + c1.getValue());
		
		
		// ������������д���ļ��У�ֻ��Ϊ�˿�������
		Path totalWordsPath = new Path("hdfs://192.168.190.128:9000/user/hadoop/output/totalwords.txt");
		FileSystem fs = totalWordsPath.getFileSystem(conf);
		FSDataOutputStream outputStream = fs.create(totalWordsPath);
		outputStream.writeBytes(c1.getDisplayName() + ":" + c1.getValue());
		IOUtils.closeStream(outputStream);

		// ֱ�ӵ�����������д��configuration�У���Ҫֱ�Ӷ�ȡ���ö��ļ�
		conf.setInt("TOTALWORDS", (int)c1.getValue());

		return exitCode;

	}

}
