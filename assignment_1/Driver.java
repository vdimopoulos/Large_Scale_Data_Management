package gr.aueb.panagiotisl.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static  void main(String[] args) throws Exception {
        
        // Set the Hadoop home directory to resolve any compatibility issues
        System.setProperty("hadoop.home.dir", "/");

        // instantiate a configuration
        Configuration configuration = new Configuration();

        // Instantiate a MapReduce job
        Job job = Job.getInstance(configuration, "Song Danceability");
        
        // Set the main class containing the MapReduce job configuration
        job.setJarByClass(SongDanceability.class);
        
        // Set the Mapper and Reducer classes
        job.setMapperClass(SongDanceability.DanceabilityMapper.class);
        job.setReducerClass(SongDanceability.DanceabilityReducer.class);
        
        // Set the output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path("/user/hdfs/input/universal_top_spotify_songs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/user/hdfs/output/"));

        // Wait for the job to complete and exit the program accordingly
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
