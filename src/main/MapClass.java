import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

class Map extends Mapper<LongWritable, Text, Text, Words>
{
    private Set<String> patternsToSkip = new HashSet<String>();
    String pattern="##$$$$$";
    private void parseSkipFile(Path patternsFile, Configuration conf) throws IOException {
        FSDataInputStream in = FileSystem.get(conf).open(patternsFile);
        try {
            //BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
            BufferedReader fis = new BufferedReader(new InputStreamReader(in));
            String pattern = null;
            while ((pattern = fis.readLine()) != null) {
                //pattern=pattern.replaceAll("^\\w","");
                //System.out.println("Pattern: "+pattern);
                patternsToSkip.add(pattern.toLowerCase());
            }
        } catch (IOException ioe) {
            System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : "
                    + StringUtils.stringifyException(ioe));
        }
    }

    protected void setup(Context context)throws IOException,InterruptedException{
        Configuration conf = context.getConfiguration();
        if(DistributedCache.getCacheFiles(conf)!=null)
        {
            URI[] cacheFile= DistributedCache.getCacheFiles(conf);
            for(int i=0;i<cacheFile.length;i++)
            {
                System.out.println("Cache File: "+cacheFile[i].getPath());
                parseSkipFile(new Path(cacheFile[i].getPath()), conf);
            }
        }
        else
        {
            Path[] cacheFile=DistributedCache.getLocalCacheFiles(conf);
            for(int i=0;i<cacheFile.length;i++)
            {
                System.out.println("Local Cache File: "+cacheFile[i]);
                parseSkipFile(cacheFile[i], conf);
            }
        }
        for(String s: patternsToSkip)
        {
            System.out.println("Pattern: "+s);
        }
    }
    //Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //process(new Path(value.toString()),context);
        ArrayList<Words> list=new ArrayList<Words>();
        String url="";
        String fileString = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
        System.out.println(fileString);
        String line=value.toString();
        System.out.println(line);
        Words w= new Words();
        int pos=0;

        StringTokenizer contents = new StringTokenizer(line.toLowerCase());
        String word;
        if(contents.hasMoreTokens()) word=contents.nextToken();
        else {
            System.out.println("Missing url");
            return;
        }
        if (word.contains(pattern))
        {
            url=line.substring(7,word.length()-7);
            System.out.println(url+" "+line.length());
        }
        else {
            System.out.println(word.contains(pattern)+ " " + word.substring(0,7)+pattern);
            System.out.println(word);
            System.out.println("No url in line");
            return;
        }
        System.out.println(line + word.length());
        line=line.substring(word.length()+1);
        System.out.println(line);
        for(String s : patternsToSkip)
        {
            line = line.replaceAll(" "+s+" ", " ");
        };
        System.out.println("After replace: "+line);
        line=line.replaceAll("^\\w\\d", " ");
        System.out.println(url+ " "+ line);
        contents = new StringTokenizer(line.toLowerCase());
        while (contents.hasMoreTokens()) {
            word=contents.nextToken();
            System.out.println("Checking: "+word);
            Words ww=new Words();
            ww.url=url;
            ww.position=pos;
            ww.word=word;
            ww.file=fileString;
            ww.file_pos=key.get();
            //context.write(new Text(word), w);
            list.add(ww);
            pos++;
        }
        for (int i=0;i<list.size();i++)
        {
            System.out.println(pos);
            Words ww=list.get(i);
            ww.doc_len=pos;
            System.out.println(i+" "+ww.word+" "+ww.doc_len);
            context.write(new Text(ww.word), ww);
        }
        context.write(new Text("URL_Length"), w);
    }

    //process a file
    /*
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
        boolean first=true;
        ArrayList<Words> list=new ArrayList<Words>();
        Words w;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            w=new Words();
            if (line.contains(pattern))
            {
                url=line.substring(7,line.length()-7);
                System.out.println(url);
                w.position=pos;
                if(first)
                {
                    first=false;
                    continue;
                }
                for (int i=0;i<list.size();i++)
                {
                    System.out.println(pos);
                    Words words1=list.get(i);
                    Words ww=new Words();
                    ww.doc_len=pos;
                    ww.url= words1.url;
                    ww.position=words1.position;
                    ww.word=words1.word;
                    System.out.println(words1.word+ww.doc_len);
                    context.write(new Text(ww.word), ww);
                }
                context.write(new Text("URL_Length"),w);
                System.out.println("AFTer sending the keys");
                list=new ArrayList<Words>();
                pos=0;
                System.out.println("Continuing onto "+url);
                continue;
            }
            line=line.replaceAll("[^\\w\\d]"," ");
            for(String s : patternsToSkip)
            {
                line = line.replaceAll(" "+s+" ", " ");
            };
            System.out.println(line);
            StringTokenizer contents = new StringTokenizer(line.toLowerCase());
            while (contents.hasMoreTokens()) {
                String word=contents.nextToken();
                Words ww=new Words();
                System.out.println("Checking: "+word);
                ww.url=url;
                ww.position=pos;
                ww.word=word;
                //context.write(new Text(word), w);
                list.add(ww);
                pos++;
            }
        }
        Words www=new Words();
        www.url=url;
        www.word="";
        www.doc_len=pos;
        www.position=pos;
        context.write(new Text("URL_Length"),www);
        System.out.println(pos);
        for (int i=0;i<list.size();i++)
        {
            System.out.println(pos);
            Words words1=list.get(i);
            Words ww=new Words();
            ww.doc_len=pos;
            ww.url= words1.url;
            ww.position=words1.position;
            ww.word=words1.word;
            System.out.println(words1.word+ww.doc_len);
            context.write(new Text(ww.word), ww);
        }
    }*/
}