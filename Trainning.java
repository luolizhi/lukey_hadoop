package org.lukey.hadoop.bayes.trainning;

import org.apache.hadoop.conf.Configuration;


/**
 * 
 * �����࣬���𴫵����е��������·�����Լ���ת·����ͨ��configuration����
 * 
 * 
 *
 */
public class Trainning {

	public static void main(String[] args) throws Exception {
		

		Configuration conf = new Configuration();
		conf.set("mapred.jar", "E://eclipse//jar-work//Trainning.jar");	//��eclipse����ant���ʹ��
		conf.set("mapred.job.tracker", "192.168.190.128:9001");
		
		//����һ������FILENUMBER�������ı��������ڸó��������
		conf.setInt("FILENUMBER", 30);//ע�͸��к�ȡĬ�ϵ�����10
		
		// ���õ���������ʵı���·��
		String priorProbality = "hdfs://192.168.190.128:9000/user/hadoop/output/priorP/priorProbability.txt";
		conf.set("priorProbality", priorProbality);

		// �������������ı���·�����Ż����������д�ļ���ֱ�ӱ�����conf����
		String totalWordsPath = "hdfs://192.168.190.128:9000/user/hadoop/output/totalwords.txt";
		conf.set("totalWordsPath", totalWordsPath);

		// ÿ������е�������
		String wordsInClassPath = "hdfs://192.168.190.128:9000/user/hadoop/mid/wordsFrequence/_wordsInClass/wordsInClass-r-00000";
		conf.set("wordsInClassPath", wordsInClassPath);

		// �������� �� ���ʴ�Ƶ�����·��
		String input = "hdfs://192.168.190.128:9000/user/hadoop/input/NBCorpus/Country";
		//���Գ������õ������ı���������ʱע�͵�23��
//		String input = "hdfs://192.168.190.128:9000/user/hadoop/test/";	
		
		String wordsOutput = "hdfs://192.168.190.128:9000/user/hadoop/mid/wordsFrequence";
		conf.set("input", input);
		conf.set("wordsOutput", wordsOutput);

		// ÿ����𵥴ʸ��ʱ���·��,
		// ���ʴ�Ƶ������·��Ҳ���ǵ��ʴ�Ƶ�����·��
		String conditionPath = "hdfs://192.168.190.128:9000/user/hadoop/output/probability/";
		conf.set("conditionPath", conditionPath);
	
		//predict
		//���Ե��������Ŀ¼
		String testInput = "hdfs://192.168.190.128:9000/user/hadoop/test";
		conf.set("testInput", testInput);
		
		String testOutput = "hdfs://192.168.190.128:9000/user/hadoop/output/Predict";
		conf.set("testOutput", testOutput);
		
		//��������conditionPath���������priorProbality
		//�����Ѿ����ú���

		String notFoundPath = "hdfs://192.168.190.128:9000/user/hadoop/output/probability/_notFound/notFound-m-00000";
		conf.set("notFoundPath", notFoundPath);
		

		//�����������ĺû�
		String result = "hdfs://192.168.190.128:9000/user/hadoop/output/Predict/part-r-00000";
		conf.set("result", result);
		String resultOut = "hdfs://192.168.190.128:9000/user/hadoop/output/Result";
		conf.set("resultOut", resultOut);
		
		System.out.println("-------Start-------");
		FileCount.run(conf);
		System.out.println("------FileCount OK------");
		
		WordCount.run(conf);
		Probability.run(conf);
		System.out.println("------Probability OK------");
		
		Predict.run(conf);
		System.out.println("------Predict OK------");
		
		Evaluation.run(conf);
		
		System.out.println("------Evaluation OK------");
		System.out.println("------All Over------");
	
	}

}
