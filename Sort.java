import java.io.IOException; 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

public class Sort { 
    public static class SortMapper extends Mapper<LongWritable, Text, Text, Text> { 
        Text color = new Text();
        Text fruitNumber = new Text();
        
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            String[] tokens = value.toString().split(",");
            
            if (tokens.length == 3) {
                // Emit color as key and "fruit-number" as value
                color.set(tokens[2]);
                fruitNumber.set(tokens[0] + "-" + tokens[1]);
                context.write(color, fruitNumber);
            }
        } 
    }

    public static class SortReducer extends Reducer<Text, Text, NullWritable, Text> { 
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
            for (Text value : values) {
                // Combine color with fruit-number to get the final output format
                context.write(NullWritable.get(), new Text(key.toString() + "-" + value.toString()));
            }
        } 
    }

    public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "Sort with Reducer"); 
        job.setJarByClass(Sort.class); 
        job.setMapperClass(SortMapper.class); 
        job.setReducerClass(SortReducer.class); 
        
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(Text.class); 
        
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
}

