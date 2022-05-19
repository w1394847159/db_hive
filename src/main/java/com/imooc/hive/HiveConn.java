package com.imooc.hive;

import com.jcraft.jsch.ChannelShell;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.IOUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Vector;


public class HiveConn {
    private String username;
    private String password;
    private int port;
    private String ip;
    ChannelShell channel = null;
    PrintWriter printWriter = null;
    BufferedReader input = null;




    public static void main(String[] args) {
        HiveConn hiveConn = new HiveConn("root","bigdata01",22,"root");
        String hive = hiveConn.executeForResult("hive");
        String show_tables = hiveConn.executeForResult("show tables;");
        System.out.println(show_tables);
       /* String hive = hiveConn.executeForResult("hive");
        String show_tables = hiveConn.executeForResult("show tables;");*/

        hiveConn.close();
    }

    Logger log = LoggerFactory.getLogger(HiveConn.class);

    private Vector<String> stdout;

    //会话Session
    Session session;

    public HiveConn(String username,String ip,int port,String password){
        JSch jSch = new JSch();
        try {
            session = jSch.getSession(username,ip,port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking","no");
            session.connect(100000);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    public int execute(final String command){
        int returnCode = 0;

        stdout = new Vector<String>();
        try {
            channel = (ChannelShell) session.openChannel("shell");
            channel.connect();
            input = new BufferedReader(new InputStreamReader(channel.getInputStream()));
            printWriter = new PrintWriter(channel.getOutputStream());
            printWriter.println(command);
            printWriter.println("show tables;");
            printWriter.println("show create table  hello;");
            if(printWriter.toString().contains("hello")){
                System.out.println("我包含hellotable");
            }
            printWriter.println("CREATE EXTERNAL TABLE test_table(id STRING, name STRING) \n" +
                    "ROW FORMAT DELIMITED \n" +
                    "FIELDS TERMINATED BY ','\n" +
                    "LOCATION '/data/test/test_table';");
            printWriter.println("LOAD DATA INPATH 'hdfs://bigdata01:9000/hello.txt' INTO TABLE test_table;");
            printWriter.println("desc user;"); //查看表描述
            printWriter.println("select * from user;"); //查询表数据
            //printWriter.println("update test_user set user_id = 'text' where user_id = 'hello you';");
            printWriter.println("drop table if exists test_user;"); //删除表
            printWriter.flush();
            log.info("The remote command is: ");
            String line;
            while ((line = input.readLine()) != null) {
                stdout.add(line);
                System.out.println(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
        return returnCode;

    }

    // 断开连接
    public void close(){
        printWriter.close();
        try {
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (channel != null) {
            channel.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }

    // 执行命令获取执行结果
    public String executeForResult(String command) {
        execute(command);
        StringBuilder sb = new StringBuilder();
        for (String str : stdout) {
            sb.append(str);
        }
        return sb.toString();
    }


}
