import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by cloudera on 3/7/14.
 */
class Reduce extends Reducer<Text, Words, Text, Text> {

    public void reduce(Text key, Iterable<Words> words, Context context)
            throws IOException, InterruptedException {
        String pairs="";
        for(Words w: words){
            pairs+="("+w.url+","+w.position+") ";
        }
        context.write(key,new Text(pairs));
    }
}