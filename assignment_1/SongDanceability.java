package gr.aueb.panagiotisl.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.logging.Logger;
import java.io.IOException;


public class SongDanceability {
    
    // Mapper class for Danceability
    public static class DanceabilityMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private static final Logger log = Logger.getLogger(DanceabilityMapper.class.getName());
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            // Skip header line
            if (key.get() == 0) {
                return;
            }

            // Split CSV line into columns
            String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            // Extract relevant columns
            String country = columns[6].trim().replaceAll("\"",""); // Country column
            if (country.equals(null) || country.isEmpty()){
                return;
            }

            String date = columns[7].trim().replaceAll("\"",""); // Date column
            if (date.equals(null) || date.isEmpty()){
                return;
            }

            // Format date to year-month
            String[] date_parts = date.split("-");
            String formatted_date = date_parts[0] + "-" + date_parts[1];

            String song = columns[1].trim().replaceAll("\"",""); // Song column
            if (song.equals(null) || song.isEmpty()){
                return;
            }

            String danceability = columns[13].trim().replaceAll("\"",""); // Danceability column
            if (danceability.equals(null) || danceability.isEmpty()){
                return;
            }
            

            // Set output key-value pairs
            outputKey.set(country + ": " + formatted_date);
            outputValue.set(song + ";" + danceability);

            // Log output value for debugging
            log.info(outputValue.toString());

            // Emit key-value pair
            context.write(outputKey, outputValue);
        }
    }

    // Reducer class for Danceability
    public static class DanceabilityReducer extends Reducer<Text, Text, Text, Text> {

        private static final Logger log = Logger.getLogger(DanceabilityReducer.class.getName());

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Initialize variables for calculating average danceability and finding the top song
            float sum = 0;
            float max = 0;
            int count = 0;
            String top_song = null;

            // Iterate through values for the same key
            for (Text value : values) {
                String value_s = value.toString();
                log.info(value_s.toString());

                // Split value into song and danceability parts
                String[] parts = value_s.split(";");
                String song = parts[0];
                String danceability = parts[1];
                float danceability_i = Float.parseFloat(danceability.toString());

                // Update sum, count, and find the top song with maximum danceability
                sum += danceability_i;
                count++;
                if (max < danceability_i) {
                    max = danceability_i;
                    top_song = song;
                }
            
            }

            // Calculate average danceability
            float avg = sum/count;

            // Output (key, value) pair with top song and its danceability, along with average danceability
            context.write(key, new Text(top_song + ": " + max + ", avg: " + avg));
        }
    }
}
