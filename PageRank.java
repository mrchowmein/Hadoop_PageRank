import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

  public static void main(String[] args) throws Exception {
    if (args.length != 2){
      System.err.println("Usage: PageRank <input path> <output path>");
      System.exit(-1);
    }

    Path inpath = new Path(args[0]);
    Path outpath = null;

    int i = 0;
    while (i < 3){

      outpath = new Path(args[1]+i);

      Job job = new Job();
      job.setJarByClass(PageRank.class);
      job.setJobName("PageRank");
      job.setNumReduceTasks(1);

      FileInputFormat.addInputPath(job, inpath);
      FileOutputFormat.setOutputPath(job, outpath);

      job.setMapperClass(PRMapper.class);
      job.setReducerClass(PRReducer.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.waitForCompletion(true);
      inpath = outpath;
      i++;
    }

    //System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
