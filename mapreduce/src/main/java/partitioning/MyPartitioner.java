package partitioning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numReducers) {
		int index = (key.toString().charAt(0)) % numReducers;
		
		//permanent  p%2
		//contract c%2
		
		return index;
	}

}
