
package com.sparktest.hdfs;



import java.io.IOException;
import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsMain {

    public static void main(String[] args) {
        URI uri = URI.create("hdfs://localhost:9000/");

        try {
            Configuration conf = new Configuration();
            FileSystem fileSystem = FileSystem.get(uri, conf);

            Path path = new Path("hdfs://localhost:9000/first/hdfs");

            if (!fileSystem.exists(path)) {
                fileSystem.mkdirs(path);
            }

            Path file = new Path(path.toString() + "/test.txt");

            System.out.println(file.toString());

            /*写*/
            FSDataOutputStream outputStream = fileSystem.create(file);

            outputStream.writeChars("hello\nworld\n");

            outputStream.writeChars("hello2\nworld2\n");

            outputStream.close();

            /*读*/
            FSDataInputStream inputStream = fileSystem.open(file);
            byte[] buffer = new byte[100];
            inputStream.read(buffer);


            System.out.println(new String(buffer));

            /*获取文件属性*/
            FileStatus status = fileSystem.getFileStatus(file);
            System.out.println(status.getPath().toString());
            System.out.println(status.getLen());


        }
        catch (IOException e) {
            // hehe
            System.out.println("cat exception");
        }


    }
}
