package Searcher;

import java.io.IOException;

/**
 * Created by cloudera on 3/12/14.
 */
public class Main {
    public static void main(String[] args) throws IOException{
        String input,query;
        try {
            input = args[0];
        } catch (ArrayIndexOutOfBoundsException e) {
            input = "fsout/";
        }
        try {
            query = args[1];
        } catch (ArrayIndexOutOfBoundsException e) {
            query="map ucr";
        }
        MRSearcher searcher=new MRSearcher();
        String[] output=searcher.run(input,query);
        for(String s:output)
            System.out.println(s);
    }
}
