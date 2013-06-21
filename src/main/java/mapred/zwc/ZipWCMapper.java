package mapred.zwc;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class ZipWCMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        // NOTE: the filename is the *full* path within the ZIP file
        // e.g. "subdir1/subsubdir2/Ulysses-18.txt"
        String filename = key.toString();
        System.out.println("map: " + filename);
        // We only want to process .txt files
        if (filename.endsWith(".txt") == false)
            return;
        // Prepare the content
        String content = new String(value.getBytes(), "UTF-8");
        content = content.replaceAll("[^A-Za-z \n]", "").toLowerCase();
        // Tokenize the content
        StringTokenizer tokenizer = new StringTokenizer(content);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
}