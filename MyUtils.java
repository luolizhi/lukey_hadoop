package org.lukey.hadoop.bayes.trainning;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MyUtils {

	// 获取文件夹下面二级文件夹路径的方法
		static List<Path> getSecondDir(Configuration conf, String folder) throws Exception {
//			System.out.println("-2---getSencondDir----" + folder);
			int FILENUMBER = conf.getInt("FILENUMBER", 10);
			Path path = new Path(folder);
			
			FileSystem fs = path.getFileSystem(conf);
			FileStatus[] stats = fs.listStatus(path);
			System.out.println("stats.length = " + stats.length);
			List<Path> folderPath = new ArrayList<Path>();
			for (FileStatus stat : stats) {
				if (stat.isDir()) {
//					System.out.println("----stat----" + stat.getPath());
					if (fs.listStatus(stat.getPath()).length > FILENUMBER) { // 筛选出文件数大于FILENUMBER的类别作为
																		// 输入路径
						folderPath.add(stat.getPath());
					}
				}
			}
//			System.out.println("----folderPath----" + folderPath.size());
		
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
