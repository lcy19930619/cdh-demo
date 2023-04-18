package com.example.cdh.service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author chunyang.leng
 * @date 2023-04-17 11:06
 */
@Component
public class HdfsService {
    @Autowired
    private FileSystem fileSystem;
    /**
     * 上传文件到 HDFS
     * @param data 文件数据
     * @param url 文件名称和路径
     * @param overwrite  是否允许覆盖文件
     */
    public void uploadFile(byte[] data, String url,boolean overwrite) throws IOException {
        try (FSDataOutputStream stream = fileSystem.create(new Path(url), overwrite)){
            IOUtils.write(data, stream);
        }
    }

    /**
     * 下载文件到本地
     * @param url
     * @return
     */
    public void download(String url, OutputStream outputStream) throws IOException {
        Path path = new Path(url);
        try (FSDataInputStream open = fileSystem.open(path)){
            IOUtils.copy(open, outputStream);
        }
    }

    /**
     * 遍历全部文件，并返回所有文件路径
     * @param url
     * @param recursive 是否为递归遍历
     * @return
     * @throws IOException
     */
    public List<Path> listFiles(String url,boolean recursive) throws IOException {
        Path path = new Path(url);
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, true);
        List<Path> list= new ArrayList<Path>();
        while (iterator.hasNext()){
            LocatedFileStatus file = iterator.next();
            Path filePath = file.getPath();
            list.add(filePath);
        }
        return list;
    }

    /**
     * 删除文件
     * @param path 文件路径
     * @param recursive 是否为递归删除
     * @throws IOException
     */
    public void delete(String path,boolean recursive) throws IOException{
        fileSystem.delete(new Path(path),recursive);
    }
}
