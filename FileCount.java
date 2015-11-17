package org.lueky.hadoop.bayes;

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
 * 获取文件个数，并计算先验概率 先验概率保存在/user/hadoop/output/priorP/prior.txt
 * 
 */

public class FileCount {

	public static void main(String[] args) throws Exception {
		// Path outputPath = new Path("/user/hadoop/output/priorP/prior.txt");

		int sum = 0;
		String in = "/user/hadoop/test";
//		String in = "/user/hadoop/input/NBCorpus/Country";

		Map<String, Integer> map = new HashMap<>();
		Map<String, Double> priorMap = new HashMap<>();


		// map传值（需要筛选测试集，有的类别文本数太少要删除）
		map = FileCount.getFileNumber(in);

		//测试打印出每个类别和文件总数
		Iterator<Map.Entry<String, Integer>> itrs = map.entrySet().iterator();
		while (itrs.hasNext()) {
//			System.out.println("ok");
			Map.Entry<String, Integer> it = itrs.next();
			if(it.getValue() <= 10){	//这两行代码可以不计算文本数少于10的类别
				itrs.remove();
			}else{
				sum += it.getValue();
				System.out.println(it.getKey() + "\t" + it.getValue());
			}
		}
		
		System.out.println("sum = " + sum);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path("/user/hadoop/output/priorP/prior.txt");
		FSDataOutputStream outputStream = fs.create(outputPath);
		
		//计算每个类别文本占总文本的比率，即先验概率
		String ctx = "";
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			Double result = 0.0;
			result = Double.parseDouble(entry.getValue().toString()) / sum;
			priorMap.put(entry.getKey(), result);//保存在priorMap中
			ctx += entry.getKey() + "\t" + result + "\n";
		}
		outputStream.writeBytes(ctx);
		IOUtils.closeStream(outputStream);
		
		// 打印概率信息，同时可以写入文件中
		// map的另外一种遍历方法
		Iterator<Map.Entry<String, Double>> iterators = priorMap.entrySet().iterator();
		while (iterators.hasNext()) {
			Map.Entry<String, Double> iterator = iterators.next();
			System.out.println(iterator.getKey() + "\t" + iterator.getValue());
		}

	}

	// get 方法
	public static Map<String, Integer> getFileNumber(String folderPath) throws Exception {

		Map<String, Integer> fileMap = new HashMap<>();
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		FileStatus[] status = hdfs.listStatus(path);

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
