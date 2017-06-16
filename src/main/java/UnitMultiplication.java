import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability

            //value = from\tto1, to2, to3...
            //read transition.txt, output to reducer
            //outputKey = fromPageID
            //outputValue = toPageId=relation

            String line = value.toString().trim();
            String[] fromTo = line.split("\t");

            //bad input
            if(fromTo.length < 2 || fromTo[1].trim().equals("")){
                return;
                //error handler
            }

            String outputKey = fromTo[0];
            String[] tos = fromTo[1].split(",");

            for(String to : tos){
                context.write(new Text(outputKey), new Text(to + "=" + (double)1/tos.length));
            }

        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer

            //value = PageId\tpageValue
            String[] pr = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication

            //key = fromPageId a
            //values = <b=1/2, c=1/2, 1>
            //differentiate value from transitionMapper and value from prMapper
            //list<b=1/2, c=1/2>
            //pr = 1
            //b\t 1/2*1
            //c\t 1/2*1

            double prCell = 0;
            List<String> transitionCells = new ArrayList<String>();
            for (Text value: values){
                if(value.toString().contains("=")){
                    transitionCells.add(value.toString());
                }
                else {
                    prCell = Double.parseDouble(value.toString());
                }
            }

            for (String cell:transitionCells){
                //cell: b=1/2
                //outputKey = b
                //outputValue = 1/2*prCell

                String outputKey = cell.split("=")[0];
                double relation = Double.parseDouble(cell.split("=")[1]);
                String outputValue = String.valueOf(relation * prCell);
                context.write(new Text(outputKey), new Text(outputValue));

                //String or double of outputValue, either will do
            }


        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Multiple inputs
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
