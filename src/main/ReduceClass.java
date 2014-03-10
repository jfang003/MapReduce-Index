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
import java.util.*;
import java.util.Map;

/**
 * Created by cloudera on 3/7/14.
 */
class Reduce extends Reducer<Text, Words, Text, Text> {

    TreeMap map;
    /*
    //@Override
    public int compare(List<Integer> s1, List<Integer> s2) {
        //Here I assume both keys exist in the map.
        List<Integer> list1 = (List<Integer>) map.get(s1);
        List<Integer> list2 = (List<Integer>)map.get(s2);
        Integer length1 = list1.size();
        Integer length2 = list2.size();
        return length1.compareTo(length2);
    }*/

    Comparator<String> lengthComparator = new Comparator<String>() {
        public int compare(String a,String b) {
            List<Integer> l1 = (List<Integer>)map.get(a);
            List<Integer> l2 = (List<Integer>)map.get(b);
            return l1.size()-l2.size();
            // size() is always nonnegative, so this won't have crazy overflow bugs
        }
    };



    public void reduce(Text key, Iterable<Words> words, Context context)
            throws IOException, InterruptedException {
        String pairs="";
        int count=0;
        map = new TreeMap<String, List<Integer>>();
        if(key.toString().contains("URL_Length")==false)
        {
            for(Words w: words){
                //pairs+="("+w.url+","+w.position+") ";
                count++;
                List<Integer> values;
                Object value=map.get(w.url);
                if(value==null) values= new ArrayList();
                else values=(List<Integer>) value;
                if(values==null) System.out.println("Values is null");
                values.add(w.position);
                map.put(w.url,values);
                System.out.printf("Key %s, url %s %d\n", key.toString(),w.url,w.position);
            }
            System.out.println("Mapped "+map.toString());
            ArrayList<String> set=new ArrayList<String>(map.keySet());

            for (int j=0;j<set.size();j++) {
                String k = set.get(j);
                List<Integer> value = (List<Integer>)map.get(k);
                System.out.printf("%s : %s\n",k, value.toString());
                pairs=pairs+"["+k+",(";
                boolean first=true;
                for(int i=0;i<value.size();i++)
                {
                    if(!first) pairs=pairs+",";
                    pairs=pairs+value.get(i);
                }
                pairs=pairs+")] ";
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
                count++;
            }
            br.write("Total: "+count);
            br.close();
        }
    }

}