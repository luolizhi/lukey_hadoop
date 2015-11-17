package org.lueky.hadoop.bayes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

public class CombineSmallFilesInputFormat extends CombineFileInputFormat<LongWritable, Text>{

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	
}
