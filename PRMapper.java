import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PRMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


    String line = value.toString();

    String []split = line.split("\\s+");
    String startNode = split[0];
    double pr = Double.parseDouble(split[split.length-1]);
    int divisor = split.length - 2;


    for(int i = 1; i<split.length-1; i++){
      String valueString = startNode + ", " + pr/divisor;
      context.write(new Text(split[i]), new Text(valueString));

    }

    String orgValue = "";
    for(int j = 1; j < split.length-1; j++){
      orgValue = orgValue + split[j]+ " ";
    }
    orgValue = orgValue.trim();

    context.write(new Text(startNode), new Text(orgValue));



  }
}
