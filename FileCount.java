package org.lukey.hadoop.bayes.trainning;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 
 * ��ȡ�ļ�������������������� ������ʱ�����/user/hadoop/output/priorP/prior.txt
 * 
 */

public class FileCount {

	public static void run(Configuration conf) throws Exception {
		
		String inputFolder = conf.get("input");
		int FILENUMBER = conf.getInt("FILENUMBER", 10);
		
		Map<String, Integer> map = new HashMap<>(); // ������������ı���
		Map<String, Double> priorMap = new HashMap<>();// ����ÿ�������������

		// ��ȡÿ�������ļ���������Ҫɸѡ���Լ����е�����ı���̫��Ҫɾ����
		map = FileCount.getFileNumber(inputFolder);

		// ���Դ�ӡ��ÿ�������ļ�������ɾ�����ı�������10�����
		int sum = 0;
		Iterator<Map.Entry<String, Integer>> itrs = map.entrySet().iterator();
		while (itrs.hasNext()) {
			// System.out.println("ok");
			Map.Entry<String, Integer> it = itrs.next();
			if (it.getValue() <= FILENUMBER) { // �����д�����Բ������ı�������FILENUMBER�����
				itrs.remove();
			} else {
				sum += it.getValue();
				System.out.println(it.getKey() + "\t" + it.getValue());
			}
		}
		System.out.println("��������������"+ map.size());
		System.out.println("��������ı������ܺ� �� " + sum);

		// ������ʵı���·��
		String output = conf.get("priorProbality");
		Path outputPath = new Path(output);
		FileSystem fs = outputPath.getFileSystem(conf);
		FSDataOutputStream outputStream = fs.create(outputPath);

		// ���� ÿ������ı�ռ���ı��ı��ʣ����������
		// д���ļ������������
		String ctx = "";
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			Double result = 0.0;
			result = Double.parseDouble(entry.getValue().toString()) / sum;
			priorMap.put(entry.getKey(), result);// ������priorMap��
			ctx += entry.getKey() + "\t" + result + "\n";
		}
		outputStream.writeBytes(ctx);
		IOUtils.closeStream(outputStream);
		/*
		 * // ��ӡ������Ϣ��ͬʱ����д���ļ��� // map������һ�ֱ������� Iterator<Map.Entry<String,
		 * Double>> iterators = priorMap.entrySet().iterator(); while
		 * (iterators.hasNext()) { Map.Entry<String, Double> iterator =
		 * iterators.next(); System.out.println(iterator.getKey() + "\t" +
		 * iterator.getValue()); }
		 */

	}

	// ��ȡ�ļ������ķ���
	public static Map<String, Integer> getFileNumber(String folderPath) throws Exception {

		Map<String, Integer> fileMap = new HashMap<>();
		Configuration conf = new Configuration();

		Path path = new Path(folderPath);
		FileSystem hdfs = path.getFileSystem(conf);
		FileStatus[] status = hdfs.listStatus(path);
		// System.out.println(folderPath);
		// System.out.println("status.length = " + status.length);
		for (FileStatus stat : status) {
			if (stat.isDir()) {
				int length = hdfs.listStatus(stat.getPath()).length;
				String name = stat.getPath().getName();
				fileMap.put(name, length);
			}
		}
		return fileMap;
	}
}

