package com.verizon.oneparser.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class ZipExtractor {

    public static class Metrics {
        public final String entryName;
        public final Long startTime;
        public final Long endTime;

        public Metrics(String entryName, Long startTime, Long endTime) {
            this.entryName = entryName;
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }
    private static final Logger log = Logger.getLogger(ZipExtractor.class);
    private static final int DEFAULT_BUF_SIZE = 1024;
    private static final String CONST_BUFF_STR = "_BufferSizeTest_";

    public Metrics extractZipFileIntoHdfs(ZipInputStream zipInputStream, Configuration broadcastValue,String zipOutputPath) throws Exception {
        String entryName = null;
        Long startTime = null;
        Long endTime = null;
        try {
           log.info("Zip Extraction Output Path:"+zipOutputPath);

            ZipEntry zipEntry = null;

          /*  if(zipEntry.isDirectory()){
                throw new NotSupportedException("Directory Inside Zip file is not supported");
            }

            ZipFile zipFile = new ZipFile("");
            if (countRegularFiles(zipFile) >1){
                throw new NotSupportedException("More than 1 file inside is not supported yet");
            }*/

            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                int bytesRead = 0;
                entryName = removeSpecialCharsfromFileName(zipEntry.getName());
                Integer actualBufSize = getBufferBytesByFileName(entryName);
                byte[] buf = new byte[actualBufSize];
                startTime = System.currentTimeMillis();
                log.info("Zip Extraction Started for: " + entryName + " at : "+ new Timestamp(startTime));
                if (!zipEntry.isDirectory()) {
                    FileSystem fs = FileSystem.get(broadcastValue);
                    FSDataOutputStream hdfsfile = fs.create(new Path(zipOutputPath + "/" + entryName));
                    while ((bytesRead = zipInputStream.read(buf, 0, actualBufSize)) > -1) {
                        hdfsfile.write(buf, 0, bytesRead);
                    }
                    endTime = System.currentTimeMillis();
                    log.info("Successfully extracted zip file under : "+zipOutputPath  + entryName + " at : "+ new Timestamp(endTime) + " Time Taken in Total : "+(endTime - startTime)/1000+" Seconds");
                    hdfsfile.close();
                    hdfsfile.flush();
                }
                zipInputStream.closeEntry();
            }
            zipInputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Metrics(entryName, startTime,endTime);
    }


    int countRegularFiles(final ZipFile zipFile) {
        final Enumeration<? extends ZipEntry> entries = zipFile.entries();
        int numRegularFiles = 0;
        while (entries.hasMoreElements()) {
            if (! entries.nextElement().isDirectory()) {
                ++numRegularFiles;
            }
        }
        return numRegularFiles;
    }

    public String removeSpecialCharsfromFileNameOld(String fileName) {
        fileName = fileName.replaceAll("\\s+", "_");// replace space with underscore
        fileName = fileName.replaceAll("[^a-zA-Z0-9\\.\\-\\_\\&]", "_");
        fileName = StringUtils.startsWithIgnoreCase(fileName, "_") ? "1" + fileName : fileName;
        return fileName;
    }

    public String removeSpecialCharsfromFileName(String fileName) {
        fileName = fileName.replaceAll("\\s+", "_");// replace space with underscore

        String atntPattern = "AT&T";
        if(fileName.toUpperCase().contains(atntPattern))
        {
            atntPattern = fileName.substring(fileName.toUpperCase().indexOf(atntPattern), fileName.toUpperCase().indexOf(atntPattern)+4);
            //log.info("atntPattern = {}", atntPattern);
            String[] filenameArray = fileName.split(atntPattern);
            String filePartName = "";
            for (int i = 0; i < filenameArray.length; i++) {
                filePartName = filePartName.concat(filenameArray[i].replaceAll("[^a-zA-Z0-9\\.\\-\\_]", "_"));
                if(filenameArray.length > 1 && i < filenameArray.length-1)
                {
                    filePartName=filePartName.concat(atntPattern);
                }
            }
            fileName = filePartName;
        }
        else
        {
            fileName = fileName.replaceAll("[^a-zA-Z0-9\\.\\-\\_]", "_");
        }

        fileName = StringUtils.startsWithIgnoreCase(fileName, "_") ? "1" + fileName : fileName;

        //fileName = removePSPatternfromPSFile(fileName);

        return fileName;
    }

    private int getBufferBytesByFileName(String fileName){
        int defaultBuffer = 0;
        if(fileName.contains(CONST_BUFF_STR)){
            String[] splt_arr = fileName.split(CONST_BUFF_STR);
            if(splt_arr.length > 0){
                String[] innerSplt = splt_arr[0].split("_");
                String fileBufferValue = innerSplt[innerSplt.length-1];
                try {
                    defaultBuffer = Integer.parseInt(fileBufferValue);
                } catch (NumberFormatException nfe){
                    defaultBuffer = DEFAULT_BUF_SIZE;
                    System.out.println("Unable to get Buffer Size From FileName. So using Default Buffer : "+defaultBuffer);
                }
            }
        }
        if(defaultBuffer < DEFAULT_BUF_SIZE) defaultBuffer = DEFAULT_BUF_SIZE;
        System.out.println("Final Buffer Size for file: "+fileName + " is: "+defaultBuffer);
        return defaultBuffer;
    }


}
