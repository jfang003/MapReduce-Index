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

    public static void main(String[] args) throws IOException, InterruptedException {
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
        /*
        BufferedReader br=new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line=br.readLine();
        while(line==br.readLine())
        {
            line=inputfile+line;
            System.out.println(line);
            out.writeChars(line);
        }
        */

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
            Path path=status[i].getPath();
            String name=status[i].getPath().getName();
            if(name.startsWith("_") || name.contains("part-r"))
            {
                System.out.println("deleting "+status[i].getPath());
                fs.delete(status[i].getPath(), true);
                continue;
            }
            //rename file
            String[] parts=name.split("_");
            System.out.println(name);
            String word=parts[0];
            System.out.println("Word: "+word);
            if(word=="")continue;
            Path p=new Path(home+word);
            //System.out.println("BEFORE Creating path: "+p.toString());
            /*if(!fs.exists(p))
            {
                System.out.println("Creating path");
                out=fs.create(p);
            }
            else
            {
                System.out.println("Appending path");
                if(fs.exists(p)) System.out.println("The file already exist");
                out=fs.create(new Path("_temp"));
            }*/

            //System.out.println("After Creating path");
            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
            String line;
            line=bufferedReader.readLine();
            System.out.println("READING first line");
            Integer count= m.get(word);
            if(count==null) count=0;
            out=fs.create(new Path(home+"_temp"));
            boolean first=true;
            br=new BufferedWriter(new OutputStreamWriter(out));
            while (line != null){
                System.out.println(line);
                if(first==true)
                {
                    System.out.println("Processing First Line");
                    first=false;
                    String[] contents=line.split(" ");
                    count+=Integer.parseInt(contents[1]);
                    m.put(word, count);
                }
                else{
                    System.out.println("Processing Second Line: ");
                    br.write(line+"\n");
                }
                line=bufferedReader.readLine();
            }
            //br.close();
            bufferedReader.close();
            if (fs.exists(p))
            {
                System.out.println("Opened: "+p.toString());
                bufferedReader=new BufferedReader(new InputStreamReader(fs.open(p)));
                line=bufferedReader.readLine();
                while (line != null){
                    //System.out.println("Processing: "+line);
                     //System.out.println("Processing Second Line: ");
                    br.write(line+"\n");
                line=bufferedReader.readLine();
                }
            }
            br.close();
            bufferedReader.close();
            fs.rename(new Path(home+"_temp"),p);

            /*
            String command="hadoop fs -cat ";
            //fs.rename(p,new Path(home+"__temp__"));
            if(fs.exists(p))
            {
                command=command+home+"__temp__ ";
                fs.rename(p,new Path(home+"__temp__"));
            }
            else
            {
                fs.create(p);
            }
            command=command+home+"_temp >"+p.toString();
            System.out.println(command);
            Runtime r = Runtime.getRuntime();
            Process process = r.exec(command);
            process.waitFor();
            break;
*/
            fs.delete(path,true);
            //fs.delete(new Path(home+"_temp"),true);
            //fs.delete(new Path(home+"__temp__"),true);
        }

        for(String s: m.keySet())
        {
            System.out.println("Word: "+s);
            Path temp=new Path(home+"_temp");
            out=fs.create(temp);
            br=new BufferedWriter(new OutputStreamWriter(out));
            br.write(s + " " + m.get(s)+"\n");
            Path p=new Path(home+s);
            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(fs.open(p)));
            String line=bufferedReader.readLine();
            while (line != null){
                //System.out.println("Processing: "+line);
                //System.out.println("Processing Second Line: ");
                br.write(line+"\n");
                line=bufferedReader.readLine();
            }
            br.close();
            bufferedReader.close();
            //must delete the file otherwise we can't rename
            fs.delete(p,true);
            fs.rename(temp,p);
            /*br.close();
            fs.rename(new Path(home+s), new Path(home+"_temp_"));
            fs.create(new Path(home+s));
            String command= "hadoop fs -cat "+home+"__temp__"+" "+home+"_temp_ >"+home+s;
            System.out.println(command);
            Runtime r = Runtime.getRuntime();
            Process process = r.exec(command);
            process.waitFor();*/
            //fs.delete(new Path(home+"_temp_"),true);
            //fs.delete(new Path(home+"__temp__"),true);
        }
//        */
    }

}
