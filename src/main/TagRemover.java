import java.io.*;
import java.util.StringTokenizer;

/**
 * Created by cloudera on 3/12/14.
 */
public class TagRemover {
    public static void main(String args[]) throws IOException {
        String InputPath = args[0];
        String OutputPath=args[1];
        File folder = new File(InputPath);
        File[] listOfFiles = folder.listFiles();
        if(!InputPath.endsWith("/")) InputPath+="/";
        if(!OutputPath.endsWith("/")) OutputPath+="/";
        for (int i = 0; i < listOfFiles.length; i++)
        {
            if (listOfFiles[i].isFile())
            {
                String name = listOfFiles[i].getName();
                write(InputPath+name,OutputPath+name);
                System.out.println(name);
            }
        }
    }

    public static void write(String input, String output) throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader(input));
        String line=br.readLine();
        String pattern="##$$$$$";
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));
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
                //System.out.println(url+" "+line.length());
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
