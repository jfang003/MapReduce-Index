import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * Created by cloudera on 3/8/14.
 */
public class Words implements Writable{
    public String url="";
    public int position=-1;
    public int doc_len=-1;
    public String word="";

    public Words(){
    }


    /**
     * Mandatory function for implementing writable
     *
     * @param in datainput from the mapreduce framework
     * @throws IOException in case we can't read the file
     */
    public void readFields(DataInput in) throws IOException {
        this.url=in.readUTF();
        this.position=in.readInt();
        this.doc_len=in.readInt();
        this.word=in.readUTF();
    }

    /**
     * Mandatory function for implementing writable
     *
     * @param out object that allows us to write to disk
     * @throws IOException in case we can't write to disk
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.url);
        out.writeInt(this.position);
        out.writeInt(this.doc_len);
        out.writeUTF(this.word);
    }
}
