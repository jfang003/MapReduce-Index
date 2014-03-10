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
import java.util.regex.Pattern;
import java.util.regex.Matcher;

class Map extends Mapper<LongWritable, Text, Text, Words>
{
    //Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String url="";
        String pattern="##$$$$$";
        int pos = 0;
        /*
        for(Text t: value)
        {
            String line=value.toString();
            if (line.substring(0,7)==pattern)
            {
                url=line.substring(7,line.length()-7);
                pos=0;
                continue;
            }
            line=line.replaceAll("[^\\w\\d]"," ");
            StringTokenizer contents = new StringTokenizer(line.toLowerCase());
            while (contents.hasMoreTokens()) {
                String word=contents.nextToken();
                Words w=new Words();
                w.url=url;
                w.position=pos;
                context.write(new Text(word), w);
                pos++;
            }
        }*/

        //, " ,?!:;()<>[]\b\t\n\f\r\"\'\\\""
        /*FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        System.out.println("File name "+filename+"\n Directory and File name"+fileSplit.getPath().toString()+value.toString());
        StringTokenizer contents = new StringTokenizer(value.toString().toLowerCase());
        while (contents.hasMoreTokens()) {
            String word=contents.nextToken();
            Words w=new Words();
            w.url=url;
            w.position=pos;
            context.write(new Text(word), w);
            pos++;
        }*/
        process(new Path(value.toString()),context);
    }

    //process a file
    protected void process(Path file, Context context) throws IOException,
            InterruptedException {
        String url="";
        String pattern="##$$$$$";
        String filename = file.getName();
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream in = fs.open(file);
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(in));
        String line = null;
        int pos=0;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            if (line.contains(pattern)==true)
            {
                url=line.substring(7,line.length()-7);
                System.out.println(url);
                pos=0;
                continue;
            }
            line=line.replaceAll("[^\\w\\d]"," ");
            StringTokenizer contents = new StringTokenizer(line.toLowerCase());
            while (contents.hasMoreTokens()) {
                String word=contents.nextToken();
                Words w=new Words();
                w.url=url;
                w.position=pos;
                context.write(new Text(word), w);
                pos++;
            }
        }
    }
}