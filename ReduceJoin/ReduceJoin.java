import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;   
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
 
 public class ReduceJoin {
	 
	 public static class CustsMapper extends Mapper <Object, Text, Text, Text>
	 {
		 public void map(Object key, Text value, Context context)
		 throws IOException, InterruptedException 
		 {
		 	String record = value.toString();
		 	String[] parts = record.split(",");
		 	context.write(new Text(parts[0]), new Text("n " + parts[1]));
		 }
	 }
	 
	 public static class TxnsMapper extends Mapper <Object, Text, Text, Text>
	 {
		 public void map(Object key, Text value, Context context) 
		 throws IOException, InterruptedException 
		 {
		 	String record = value.toString();
		 	String[] parts = record.split(",");
		 	context.write(new Text(parts[1]), new Text("t " + parts[0] + " " + parts[2]));
		 }
	 }
	 
	 public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text>
	 {
	 	public void reduce(Text key, Iterable<Text> values, Context context)
	 	throws IOException, InterruptedException 
	 		{

				String uname = "";
				ArrayList<String> tId = new ArrayList<String>();
				ArrayList<String> cost = new ArrayList<String>();
	 			for (Text t : values) 
				{ 
					String parts[] = t.toString().split(" ");
					if (parts[0].equals("t")) 
					{	
						tId.add(parts[1]);
						cost.add(parts[2]);
					} 
					else if (parts[0].equals("n")) 
					{
						uname = parts[1];
					}
				}
				for(int i = 0;i < tId.size();i++) {
					String str = key + "," + cost.get(i) + "," + uname;
					context.write(new Text(tId.get(i)), new Text(str));
				}
	 		}	
	 }

	 public static class SecondMapper extends Mapper <Object, Text, IntWritable, Text>
	 {
		 public void map(Object key, Text value, Context context)
		 throws IOException, InterruptedException 
		 {
		 	String record = value.toString();
		 	String[] parts = record.split("\\s+");
		 	int newKey = Integer.parseInt(parts[0]);

		 	context.write(new IntWritable(newKey), new Text(parts[1]));
		 }
	 }

	 public static class SecondReducer extends Reducer <IntWritable, Text, IntWritable, Text>
	 {
	 	public void reduce(IntWritable key, Iterable<Text> values, Context context)
	 	throws IOException, InterruptedException 
	 		{
	 			String merge = "";
	 			for (Text value : values)
	 			{
	 				merge += value.toString();
	 			}

				context.write(key, new Text(merge));
	 		}	
	 }

	 public class IntComparator extends WritableComparator {

	  public IntComparator() {
	    super(IntWritable.class);
	  }

	  @Override
	  public int compare(byte[] b1, int s1, int l1, byte[] b2,
	        int s2, int l2) {
	    Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
	    Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
	    return v1.compareTo(v2) * (-1);
	  }
	}
	 
	 public static void main(String[] args) throws Exception {

	 	// JobControl jobControl = new JobControl("jobChain"); 

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Reduce-side join");
		job.setJarByClass(ReduceJoin.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path outputPath = new Path(args[2] + "/temp");

		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TxnsMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "Sorting");
		job2.setJarByClass(ReduceJoin.class);
		job2.setMapperClass(SecondMapper.class);
		job2.setReducerClass(SecondReducer.class);

		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[2] + "/temp")); 
		FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/final"));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}