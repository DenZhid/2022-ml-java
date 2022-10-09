import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

import java.lang

object WordCount extends Configured with Tool {
  val IN_PATH_PARAM: String = "wordcount.input"
  val OUT_PATH_PARAM: String = "wordcount.output"

  override def run(args: Array[String]): Int = {
    val job: Job = Job.getInstance(getConf, "Word Count")
    job.setJarByClass(getClass)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job.setMapperClass(classOf[TokenizerMapper])
    job.setReducerClass(classOf[IntSumReducer])

    job.setNumReduceTasks(2)
    job.setPartitionerClass(classOf[AlphabetPartitioner])

    val in: Path = new Path(getConf.get(IN_PATH_PARAM))
    val out: Path = new Path(getConf.get(OUT_PATH_PARAM))
    FileInputFormat.addInputPath(job, in)
    FileOutputFormat.setOutputPath(job, out)

    val fs: FileSystem = FileSystem.get(getConf)
    if (fs.exists(out)) fs.delete(out, true)

    if (job.waitForCompletion(true)) 0 else 1
  }

  def main(args: Array[String]): Unit = {
    val res: Int = ToolRunner.run(new Configuration(), this, args)
    System.exit(res)
  }
}

class TokenizerMapper extends Mapper[AnyRef, Text, Text, IntWritable] {
  private val one: IntWritable = new IntWritable(1)
  private val text: Text = new Text()

  override def map(key: AnyRef,
                   value: Text,
                   context: Mapper[AnyRef, Text, Text, IntWritable]#Context
                  ): Unit = {
    for (word: String <- value.toString.toLowerCase().split("\\s")) {
      text.set(word)
      context.write(text, one)
    }
  }
}

class IntSumReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  private val result: IntWritable = new IntWritable()

  override def reduce(key: Text,
                      values: lang.Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context
                     ): Unit = {
    var sum: Int = 0
    values.forEach((value: IntWritable) => sum = sum + value.get())
    result.set(sum)
    context.write(key, result)
  }
}

class AlphabetPartitioner extends Partitioner[Text, IntWritable] {
  override def getPartition(key: Text, value: IntWritable, numPartitions: Int): Int = {
    if (numPartitions == 1) return 0
    if (key.getLength > 2)  1 else  0
  }
}