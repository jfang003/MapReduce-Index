/**
 * Created by cloudera on 3/9/14.
 * not used anymore because of another workaround
 */
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.JobContext;

public class NonSplittableTextInputFormat extends TextInputFormat {
    //@override
    protected boolean isSplitable(JobContext context,
                                  Path file){
        return false;
    }
}