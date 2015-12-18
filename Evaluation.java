package org.lukey.hadoop.bayes.trainning;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * ���Լ����˵�����𣬷ֱ���USA 50����INDIA 20���� ARG 15�����ֱ������tp fp  fn tn
 */
public class Evaluation {
	static String[] country = { "USA", "INDIA", "ARG" };
	static int TP[] = { 0, 0, 0 };
	static int FP[] = { 0, 0, 0 };
	static int FN[] = { 0, 0, 0 };
	static int TN[] = { 0, 0, 0 };

	public static void run(Configuration conf) throws Exception {
		String input = conf.get("result");
		
		//��ȡ������𣬼���P R F1
		FileSystem fs = FileSystem.get(URI.create(input), conf);
		FSDataInputStream inputStream = fs.open(new Path(input));
		BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));
		String strLine = "";

		while ((strLine = buffer.readLine()) != null) {
			for (int i = 0; i < country.length; i++) {
//				System.out.println(country[i]);
				String[] temp = strLine.split("\t");
//				System.out.println(temp[0]);
//				System.out.println(temp.length);
				if (country[i].equals(temp[0])) { // ͳ������country[0]��tp fn
					if (temp[2].equals(temp[0])) {
						TP[i]++;
					} else {
						FN[i]++;
					}
				} else {
					if (country[i].equals(temp[2])) {
						FP[i]++;
					} else {
						TN[i]++;
					}
				}
			}
		}

		//��������������ļ��У�
		String output = conf.get("resultOut");
		Path outputPath = new Path(output);
		FileSystem outFs = outputPath.getFileSystem(conf);
		FSDataOutputStream outputStream = outFs.create(outputPath);
		String ctx ="";
		double P[] = { 0.0, 0.0, 0.0 };
		double R[] = { 0.0, 0.0, 0.0 };
		double F1[] = { 0.0, 0.0, 0.0 };
		for (int i = 0; i < 3; i++) {
			P[i] = (double) TP[i] / (TP[i] + FP[i]);
			R[i] = (double) TP[i] / (TP[i] + FN[i]);
			F1[i] = (double) 2 * P[i] * R[i] / (P[i] + R[i]);
			ctx += country[i] + "\tP=" + P[i] +"\tR=" + R[i] + "\tF1=" + F1[i] + "\n";
			System.out.println(country[i] + "\tP=" + P[i] +"\tR=" + R[i] + "\tF1=" + F1[i]);
		}
		outputStream.writeBytes(ctx);
		outputStream.close();
	}
}
