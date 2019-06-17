import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PRReducer
extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    Double sum = 0.0;
    String outlinks = "";

    for (Text value : values) {
      String tempString = value.toString();

      if(Character.isDigit(tempString.charAt(tempString.length()-1))){
        String[] split2 = tempString.split("\\s+");
        sum += Double.parseDouble(split2[split2.length-1]);
      } else {
        outlinks = tempString;
      }

    }

    String outputValue = key+ " " + outlinks + " " + sum;

    context.write(new Text(outputValue), new Text(""));
  }


}
