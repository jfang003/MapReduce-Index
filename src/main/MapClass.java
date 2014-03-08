import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class Map extends Mapper<LongWritable, Text, Text, Words>
{
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            Path inputDir = fileSplit.getPath().getParent();
            StringTokenizer filename = new StringTokenizer(value.toString());
            while (filename.hasMoreTokens()) {
            Path path = new Path(inputDir, filename.nextToken());
            process(path,context);
            }
    }

    //process a file
    protected void process(Path file, Context context) throws IOException,
            InterruptedException {

        String filename = file.getName();
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream in = fs.open(file);
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(in));
        String line = null;
        int pos=0;
        while ((line = reader.readLine()) != null) {
            String[] strs = line.split("[^a-zA-Z']+");
            for (String s : strs){
                Words w=new Words();
                w.set_url(filename);
                w.set_pos(pos);
                context.write(new Text(s),w);
                pos++;
            }
        }
    }
}