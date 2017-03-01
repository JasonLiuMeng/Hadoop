package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSTest {
	
	static Configuration conf = new Configuration();
	static FileSystem hdfs; 
	static{
		Path dstPath = new Path("hdfs://master:8020/");
		try {
			hdfs = dstPath.getFileSystem(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void copyFileToHDFS() throws Exception {
		Path src = new Path("e://test.txt");
		Path dstPath = new Path("/input");
		hdfs.copyFromLocalFile(src, dstPath);
		FileStatus files[] = hdfs.listStatus(dstPath);
		for( FileStatus fs : files ){
			System.out.println(fs.getPath() + " " +fs.getBlockSize() + " " + fs.getModificationTime());
		}
	}
	
	public void createFileInHDFS() throws Exception{
		Path dstPath = new Path("/input1/newFile.txt");
		FSDataOutputStream out = hdfs.create(dstPath);
		byte[] buffer = ("我是新建的HDFS文件！").getBytes();
		out.write(buffer, 0, buffer.length);
		out.close();
	}
	
	
	public void deleteFileInHDFS(String fileName) throws Exception{
		Path dstPath = new Path("/input/" + fileName);
		Boolean state = hdfs.deleteOnExit(dstPath);
		System.out.println("文件：" + fileName + "删除"+ (state ? "成功" : "失败") +"！");
	}
	
	public static void main(String[] args) {
		HDFSTest hdfsTest = new HDFSTest();
		try {
			hdfsTest.deleteFileInHDFS("test.txt");
			hdfsTest.deleteFileInHDFS("newFile.txt");
			hdfsTest.copyFileToHDFS();
			hdfsTest.createFileInHDFS();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
