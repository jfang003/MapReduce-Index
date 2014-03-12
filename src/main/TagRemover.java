import java.io.*;
import java.util.StringTokenizer;

/**
 * Created by cloudera on 3/12/14.
 */
public class TagRemover {
    public static void main(String args[]) throws IOException {
        String Path = args[0];
        BufferedReader br = new BufferedReader(new FileReader(Path));
        String line=br.readLine();
        String pattern="##$$$$$";
        BufferedWriter bw = new BufferedWriter(new FileWriter("temp.txt"));
        String url;
        while(line!=null)
        {
            StringTokenizer contents = new StringTokenizer(line.toLowerCase());
            String word;
            if(contents.hasMoreTokens()) word=contents.nextToken();
            else {
                System.out.println("Missing url");
                return;
            }
            if (word.contains(pattern))
            {
                url=word;
                System.out.println(url+" "+line.length());
            }
            else {
                System.out.println(line);
                throw new IOException();
            }
            line=line.substring(word.length()+1);
            line=line.replaceAll("\\<.*?\\>", "");
            line=line.replaceAll("[^a-zA-Z]"," ");
            bw.write(url+" "+line+"\n");
            line=br.readLine();
        }
        bw.close();
        br.close();
    }
}
