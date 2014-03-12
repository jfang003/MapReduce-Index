package Searcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by cloudera on 3/10/14.
 */
public class URLMap {
    public HashMap<String,URLInfo> urlmap= new HashMap<String,URLInfo>();
    public int total_occurrences=0;


    public void store(String url, ArrayList<Integer> l, Float rank, Integer doc_len)
    {
        URLInfo info=new URLInfo();
        info.positions=l;
        info.score=rank;
        info.doc_len=doc_len;
        urlmap.put(url,info);
    }
}
