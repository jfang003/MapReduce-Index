package Searcher;

import java.util.ArrayList;

/**
 * Created by cloudera on 3/10/14.
 */
public class URLInfo {
    public ArrayList positions=new ArrayList<Integer>();
    public Float score=new Float(0);
    public Integer doc_len= new Integer(-1);
    public String file;
    public Integer file_pos=new Integer(0);
}
