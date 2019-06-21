package deekshaproject;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text agencies = new Text();
	IntWritable count = new IntWritable();
	private static final Logger LOG = Logger.getLogger(MapperClass.class);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] list = value.toString().split(",");

		if (key.get() != 0) {
			agencies.set(list[1]);
			count.set(Integer.parseInt(list[8]));
			LOG.info("Reading line: " + agencies + "Aadhaars generated: "
					+ list[8]);
			context.write(agencies, count);
		}
		LOG.info("Skipped line with key:" + key.get());
	}

}