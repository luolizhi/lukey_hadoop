package org.lukey.hadoop.bayes.trainning;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * ������Ϊ������ԭʼ������+�ļ���+���������������㣩
 * �������ͳ�Ƽ��㣬������һ�����ݣ�����������ͬ��ʾ�ֶ��ˣ�����ʹ��ˡ�
 * 
 */
public class Predict {
	
	 private static final Log LOG = LogFactory.getLog(Predict.class);


	static enum counter {// �ֱ��ʾ�����ı��ĵ������������
		words_in_file, class_counte
	}

	// ������� ALB 0.375
	static Map<String, Double> priorMap = new HashMap<>();

	// ÿ��������浥�ʵĸ��� abassi 3.476341324572953E-5 ��������ļ�������Ҫ���⴦��
	// �����뷨�ǽ��ļ���������������Ϊmap��key�������������Ϊmap��value������ ���ʣ�
	static Map<String, Map<String, Double>> conditionMap = new HashMap<>();

	// ÿ���������û���ҵ��ĵ��ʵĸ���ALB 4.062444649191655E-7
	static Map<String, Double> notFoundMap = new HashMap<>();

	// ���������������ҪԤ����ļ���ѭ�����㲻ͬ���ĸ��ʣ���ʶֵ������Ϊ�������
	// ������� ALB 0.375��key���ļ���+��������
	// ÿ��ȡvalue�۳ɣ���Ҫ��log����
	static Map<String, Double> predictMap = new HashMap<String, Double>();

	public static int run(Configuration conf) throws Exception  {
//		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "192.168.190.128:9001");
		
		

		Job job = new Job(conf, "predict");
		job.setJarByClass(Predict.class);

		job.setInputFormatClass(WholeFileInputFormat.class);

		job.setMapperClass(PredictMapper.class);
		job.setReducerClass(PredictReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// WholeFileInputFormat.addInputPath(job, new Path(input));

		String testInput = conf.get("testInput");
		
		List<Path> paths = getSecondDir(conf, testInput);
		for (Path path : paths) {
			WholeFileInputFormat.addInputPath(job, path);
		}

		String testOutput = conf.get("testOutput");
		FileOutputFormat.setOutputPath(job, new Path(testOutput));

		int exitCode = job.waitForCompletion(true) ? 0 : 1;

		// ���ü�����
		Counters counters = job.getCounters();
		Counter c1 = counters.findCounter(counter.class_counte);
		System.out.println(c1.getDisplayName() + " : " + c1.getValue());

		return exitCode;

	}

	// map
	public static class PredictMapper extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			FileSplit split = (FileSplit) context.getInputSplit();
			Path file = split.getPath();
			String fileName = file.getName(); // �õ��ļ����������ʱ��д��context
			String claName = file.getParent().getName().toString();
			
			
			// ����preditMap�����Ӧ������иõ���value�ͳ�����ʣ����ڳ���notFoundMap����ĸ���
			/* Map<String, Map<String, Double>> conditionMap */

			for (String className : priorMap.keySet()) {
				context.getCounter(counter.class_counte).increment(1);
				// �þ�̬����������ĵ������ĸ����ĸ���
				double p = conditionalProbabilityForClass(value.toString(), className);
				LOG.info(className + "------->" + p);
//				System.out.println(className + "------->" + p);
				Text prob = new Text(className + "\t" + p);
				context.write(new Text(claName + "\t"+ fileName), prob);

			}

		}

		// static
		public static double conditionalProbabilityForClass(String content, String className) {

			// className����ÿ�����ʵĸ���
			Map<String, Double> condMap = conditionMap.get(className);
			double notFindProbability = notFoundMap.get(className);
			double priorProbability = priorMap.get(className);

			double pClass = Math.log(priorProbability);

			StringTokenizer itr = new StringTokenizer(content.toString());
			while (itr.hasMoreTokens()) {

				String word = itr.nextToken();
				if (condMap.containsKey(word)) {
					pClass += Math.log(condMap.get(word));
				} else {
					pClass += Math.log(notFindProbability);
				}
			}

			return pClass;
		}

		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// mos = new MultipleOutputs<Text, Text>(context);
			Configuration conf = context.getConfiguration();
			FileSystem fs = null;
			// ��ȡ�������
			String priorPath = conf.get("priorProbality");
			fs = FileSystem.get(URI.create(priorPath), conf);
			FSDataInputStream inputStream = fs.open(new Path(priorPath));
			BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));
			String strLine = null;
			while ((strLine = buffer.readLine()) != null) {
				String[] temp = strLine.split("\t");
				priorMap.put(temp[0], Double.parseDouble(temp[1])); // �õ�������ʱ��浽Ԥ���map��
			}

			// ��ȡÿ�����û���ҵ����ʵĸ���
			String notFoundPath = conf.get("notFoundPath");
			fs = FileSystem.get(URI.create(notFoundPath), conf);
			FSDataInputStream notFoundStream = fs.open(new Path(notFoundPath));
			BufferedReader notFoundBuffer = new BufferedReader(new InputStreamReader(notFoundStream));
			while ((strLine = notFoundBuffer.readLine()) != null) {
				String[] temp = strLine.split("\t");
				notFoundMap.put(temp[0], Double.parseDouble(temp[1]));
			}

			// ��ȡ��������
			String conditionPath = conf.get("conditionPath");
			Path condPath = new Path(conditionPath);
			fs = condPath.getFileSystem(conf);
//			fs = FileSystem.get(URI.create(conditionPath), conf);
			FileStatus[] stats = fs.listStatus(condPath);
			for (FileStatus stat : stats) {
				if (!stat.isDir()) {
					Path filePath = stat.getPath();
					String fileName = filePath.getName();
					String[] temp = fileName.split("-");
					if (temp.length == 3) {
						String className = temp[0];// �õ������

						// �����ļ�·����ȡ�ļ��������ݱ��浽map
						Map<String, Double> oneMap = new HashMap<>();
						fs = filePath.getFileSystem(conf);
//						fs = FileSystem.get(URI.create(fileName.toString()), conf);
						FSDataInputStream fileStream = fs.open(filePath);
						BufferedReader fileBuffer = new BufferedReader(new InputStreamReader(fileStream));
						while ((strLine = fileBuffer.readLine()) != null) {
							String[] tmp = strLine.split("\t");
							oneMap.put(tmp[0], Double.parseDouble(tmp[1]));// ��ȡ�ļ�����������
						}

						conditionMap.put(className, oneMap);

					}
				}
			}

		}

	}

	// reduce

	public static class PredictReducer extends Reducer<Text, Text, Text, Text> {

		Double maxP = 0.0;

		Map<String, Double> resultMap = new HashMap<>();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// find the max probability
			double maxP = -100000000.0;
			String maxClass = "";

			for (Text value : values) {
				String[] temp = value.toString().split("\t");
				double p = Double.parseDouble(temp[1]);

				if (p > maxP) {
					maxP = p;
					maxClass = temp[0];
				}
			}
			context.write(key, new Text(maxClass));
		}


	}

	// ��ȡ�ļ�����������ļ���·���ķ���
	static List<Path> getSecondDir(Configuration conf, String folder) throws Exception {
		
		Path path = new Path(folder);
		FileSystem fs = path.getFileSystem(conf);
		FileStatus[] stats = fs.listStatus(path);
		List<Path> folderPath = new ArrayList<Path>();
		for (FileStatus stat : stats) {
			if (stat.isDir()) {

				folderPath.add(stat.getPath());
			}
		}

		return folderPath;
	}



}