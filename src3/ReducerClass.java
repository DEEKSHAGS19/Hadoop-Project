package deekshaproject;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0,k=0;
		for (IntWritable count : values) {
			sum += count.get();
			k++;
		}
		fot(int i=k;i<=k-10;i--)
		{
			context.write(key, new IntWritable(sum));
		}
	}
}