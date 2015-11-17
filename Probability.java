package org.wordCount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Probability {

	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);
	public static int total = 0;
	private static MultipleOutputs<Text, DoubleWritable> mos;

	// Client
	public static void run(Configuration conf) throws Exception {


		// ��ȡ�������������õ�congfiguration��
		String totalWordsPath = conf.get("totalWordsPath");
//		String wordsInClassPath = conf.get("wordsInClassPath");




		// �ȶ�ȡ�����������
		FileSystem fs = FileSystem.get(URI.create(totalWordsPath), conf);
		FSDataInputStream inputStream = fs.open(new Path(totalWordsPath));
		BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));
		String strLine = buffer.readLine();
		String[] temp = strLine.split(":");
		if (temp.length == 2) {
			// temp[0] = TOTALWORDS
			conf.set(temp[0], temp[1]);// ��������String
		}

		total = Integer.parseInt(conf.get("TOTALWORDS"));
		LOG.info("------>total = " + total);

		System.out.println("total ==== " + total);
		
		
		Job job = new Job(conf, "file count");

		job.setJarByClass(Probability.class);

		job.setMapperClass(WordsOfClassCountMapper.class);
		job.setReducerClass(WordsOfClassCountReducer.class);

		String input = conf.get("wordsOutput");
		String output = conf.get("freqOutput");

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	// Mapper
	static class WordsOfClassCountMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		private static DoubleWritable number = new DoubleWritable();
		private static Text className = new Text();

		// ��������е�������
		private static Map<String, Integer> filemap = new HashMap<String, Integer>();

		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
						throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int tot = Integer.parseInt(conf.get("TOTALWORDS"));

			System.out.println("total = " + total);
			System.out.println("tot = " + tot);

			// ����ĸ�ʽ���£�
			// ALB weekend 1
			// ALB weeks 3
			Map<String, Map<String, Integer>> baseMap = new HashMap<String, Map<String, Integer>>(); // �����������
			// Map<String, Map<String, Double>> priorMap = new HashMap<String,
			// Map<String, Double>>(); // ����ÿ�����ʳ��ֵĸ���

			String[] temp = value.toString().split("\t");
			// �Ƚ����ݴ浽baseMap��
			if (temp.length == 3) {
				// �ļ����������
				if (baseMap.containsKey(temp[0])) {
					baseMap.get(temp[0]).put(temp[1], Integer.parseInt(temp[2]));
				} else {
					Map<String, Integer> oneMap = new HashMap<String, Integer>();
					oneMap.put(temp[1], Integer.parseInt(temp[2]));
					baseMap.put(temp[0], oneMap);
				}

			} // ��ȡ������ϣ�ȫ��������baseMap��

			int allWordsInClass = 0;
			

			for (Map.Entry<String, Map<String, Integer>> entries : baseMap.entrySet()) { // �������
				allWordsInClass = filemap.get(entries.getKey());
				for (Map.Entry<String, Integer> entry : entries.getValue().entrySet()) { // ��������еĵ��ʴ�Ƶ�����
					double p = (entry.getValue() + 1.0) / (allWordsInClass + tot);

					className.set(entries.getKey() + "\t" + entry.getKey());
					number.set(p);
					LOG.info("------>p = " + p);
					mos.write(new Text(entry.getKey()), number, entries.getKey() /*+ "\\" + entries.getKey()*/);//���һ��������Ϊ�������ļ��ж�Ӧ���ļ�

//					context.write(className, number);
				}
			}

		}

		//����������в����ڵ��ʵĸ��ʣ�ÿ�������һ������
		protected void cleanup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			int tot = Integer.parseInt(conf.get("TOTALWORDS"));
			for (Map.Entry<String, Integer> entry : filemap.entrySet()) { // �������
				
				double notFind =  (1.0) / (entry.getValue() + tot);
				number.set(notFind);
				mos.write(new Text(entry.getKey()), number, "_notFound" + "\\" +"notFound");
			
			}
			mos.close();
		}

		protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = context.getConfiguration();
			mos = new MultipleOutputs<Text, DoubleWritable>(context);
			String filePath = conf.get("wordsInClassPath");
			FileSystem fs = FileSystem.get(URI.create(filePath), conf);
			FSDataInputStream inputStream = fs.open(new Path(filePath));
			BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));
			String strLine = null;
			while ((strLine = buffer.readLine()) != null) {
				String[] temp = strLine.split("\t");
				filemap.put(temp[0], Integer.parseInt(temp[1]));
			}
		}

	}

	// Reducer
	static class WordsOfClassCountReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		// result ��ʾÿ���ļ����浥�ʸ���
		DoubleWritable result = new DoubleWritable();
		// Configuration conf = new Configuration();
		// int total = conf.getInt("TOTALWORDS", 1);

		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
						throws IOException, InterruptedException {

			double sum = 0L;
			for (DoubleWritable value : values) {
				sum += value.get();
			}
			result.set(sum);

			context.write(key, result);
		}

	}

}
