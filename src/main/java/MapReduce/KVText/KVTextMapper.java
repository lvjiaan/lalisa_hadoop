package MapReduce.KVText;

/**
 * @Describe:
 * @Author£∫lvja
 * @Date£∫2020/10/10 14:18
 * @Modifier£∫
 * @ModefiedDate:
 */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable>{

    // 1 …Ë÷√value
    LongWritable v = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

// banzhang ni hao

        // 2 –¥≥ˆ
        context.write(key, v);
    }
}
