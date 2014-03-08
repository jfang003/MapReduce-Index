import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by cloudera on 3/7/14.
 */
class Reduce extends Reducer<IntWritable, Words, IntWritable, Text> {
    private Text text = new Text("one");
    private IntWritable one = new IntWritable(1);

    public void reduce(IntWritable key, Iterable<Words> weather, Context context)
            throws IOException, InterruptedException {
        context.write(one, text);
    }
}