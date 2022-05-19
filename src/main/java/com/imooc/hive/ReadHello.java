package com.imooc.hive;

import org.apache.commons.io.IOUtils;

import java.io.*;

public class ReadHello {

    public static void main(String[] args) {
        ReadHello readHello = new ReadHello();
        try {
            File hello = readHello.getResource("/hello.txt");
            if(hello.exists()){
                InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(hello));
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                //String s = bufferedReader.readLine();
                String line = "";
                while ((line = bufferedReader.readLine()) != null){
                    System.out.println(line);
                }
                bufferedReader.close();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public File getResource(String name) throws IOException{
        String[] split = name.split("/");
        File txt = new File(split[split.length - 1]);
        InputStream ist = this.getClass().getResourceAsStream(name);
        FileOutputStream outputStream = new FileOutputStream(txt);
        IOUtils.copy(ist,outputStream);
        return txt;
    }
}
