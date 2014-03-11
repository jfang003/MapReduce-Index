import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException {
        //String s = args[0].length() > 0 ? args[0] : "skyline.in";
        Path input, output;
        Configuration conf = new Configuration();
        Path hdfsinput;
        String inputfile="";

        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
                + "org.apache.hadoop.io.serializer.WritableSerialization");
        try {
            input = new Path(args[0]);
            if(args[0].substring(args[0].length()-1)=="/") inputfile=args[0];
            else inputfile=args[0]+"/";
            hdfsinput=new Path(inputfile+"HDFS.txt");

        } catch (ArrayIndexOutOfBoundsException e) {
            input = new Path("hdfs://localhost/user/cloudera/in/skyline.in");
            hdfsinput=new Path("hdfs://localhost/user/cloudera/skyline/HDFS.txt");
        }
        try {
            output = new Path(args[1]);
            //FileSystem.getLocal(conf).delete(output, true);;
        } catch (ArrayIndexOutOfBoundsException e) {
            output = new Path("hdfs://localhost.localdomain/user/cloudera/out/");
            //FileSystem.getLocal(conf).delete(output, true);;
        }

        //conf.set("mapred.max.split.size", "134217728"); // 128 MB

        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(hdfsinput)) {
            fs.delete(hdfsinput,true);
        }
        FileStatus[] status = fs.listStatus(input);  // you need to pass in your hdfs path;
        System.out.println(input+ " "+ status.length);

        FSDataOutputStream out = fs.create(hdfsinput);
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(out));
        for (int i=0;i<status.length;i++){
            String path=status[i].getPath().toString();
            br.write(path+"\n");
            System.out.println(path);
        }
        br.close();

        //Process p=Runtime.getRuntime().exec("hadoop fs -ls "+ input +" | sed '1d;s/  */ /g' | cut -d\\  -f8 | xargs -n 1 basename");
        /*BufferedReader br=new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line=br.readLine();
        while(line==br.readLine())
        {
            line=inputfile+line;
            System.out.println(line);
            out.writeChars(line);
        }*/

        Job job = new Job(conf, "driver");

        job.setJarByClass(Driver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Words.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(2);

        job.setInputFormatClass(TextInputFormat.class);
        //job.setInputFormatClass(CombinedInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, hdfsinput);
        FileOutputFormat.setOutputPath(job, output);
        try {
            job.waitForCompletion(true);
        } catch (InterruptedException e) {
            System.out.println("Interrupted Exception");
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFoundException");
        }

        //combine the fs output into one file for each word if in parts
        status=fs.listStatus(output);
        HashMap<String,Integer> m=new HashMap<String,Integer>();
        String home = output.toString();
        if (home.substring(home.length()-1)!="/") home=home+"/";
        for (int i=0;i<status.length;i++){
            String path=status[i].getPath().toString();
            if(path.startsWith("_")) fs.delete(status[i].getPath(), true);
            String[] parts=path.split("_");
            String word=parts[0];
            Path p=new Path(home+parts[0]);
            if(!fs.exists(p)) out=fs.create(p);
            else out=fs.append(p);
            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
            String line;
            line=bufferedReader.readLine();
            Integer count= m.get(word);
            if(count==null) count=0;
            while (line != null){
                System.out.println(line);
                boolean first=true;
                if(first)
                {
                    first=false;
                    String[] contents=line.split(" ");
                    count+=Integer.parseInt(contents[1]);
                    m.put(word, count);
                }
                else{
                    br=new BufferedWriter(new OutputStreamWriter(out));
                    br.write(line);
                }
                line=bufferedReader.readLine();
            }
            br.close();
            bufferedReader.close();
        }
        for(String s: m.keySet())
        {
            out=fs.create(new Path(home+"__temp__"));
            br=new BufferedWriter(new OutputStreamWriter(out));
            br.write(s + " " + m.get(s));
            br.close();
            fs.rename(new Path(home+s), new Path(home+"_temp_"));
            String command= "hadoop -fs cat "+home+"__temp__"+" "+home+"_temp_ >"+home+s;
            Runtime r = Runtime.getRuntime();
            Process p = r.exec(command);
            fs.delete(new Path(home+"_temp_"),true);
            fs.delete(new Path(home+"__temp__"),true);
        }
    }

}
