import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created by cloudera on 3/7/14.
 */
class Reduce extends Reducer<Text, Words, Text, Text> {

    public void reduce(Text key, Iterable<Words> words, Context context)
            throws IOException, InterruptedException {
        String pairs="";
        int count=0;
        if(key.toString().contains("URL_Length")==false)
        {
            for(Words w: words){
                pairs+="("+w.url+","+w.position+") ";
                count++;
            }
            context.write(key,new Text(count+" "+pairs));
        }
        else
        {
            String output=FileOutputFormat.getOutputPath(context).toString();
            if(output=="") System.out.println("No output path");
            else System.out.println(output);
            if(output.substring(output.length()-1)!="/") output+="/";
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataOutputStream out = fs.create(new Path(output+"URL_length.txt"));
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(out));
            for(Words w: words){
                String o= w.url+" "+w.position+"\n";
                System.out.println(o);
                br.write(o);
            }
            br.close();
        }
    }

}