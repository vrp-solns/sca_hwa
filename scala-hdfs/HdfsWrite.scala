package com.example

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;


object HdfsWrite {
def main(args : Array[String]) {
println( "Trying to write to HDFS..." )
val conf = new Configuration()
val avroStr = "Avro String here"
conf.set("fs.defaultFS", "hdfs://localhost:9000")
conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);
val fs= FileSystem.get(conf)
val output = fs.create(new Path("/tmp/avro.txt"))

try {
    val os = fs.create(new Path("/tmp/avro.txt"))
    os.write(avroStr.getBytes)
}
catch {
    case e: Exception => e.printStackTrace
}
println("Done!")
}

}