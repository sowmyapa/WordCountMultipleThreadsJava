package com.cs451;

import java.io.*;
import java.util.*;

/**
 * Created by sowmyaparameshwara on 11/21/16.
 * This program reads input from a given file and spans multiple threads for calculating word count.
 * The output is written into a file.
 * Maven is used as a build tool for this java program.
 *
 * Steps to run :
 * 1) Copy entire code folder
 * 2) cd into WordCount/ directory
 * 3) Run : mvn exec:java -Dexec.mainClass="com.cs451.WordCount" -Dexec.args="4 /home/ec2-user/dummy_input.txt /home/ec2-user/Output.txt"
 */
public class WordCount {

   WordCountRunnable[] wordCountRunnable;

    public static void main(String args[]) {
        WordCount wc = new WordCount();
        if(args.length>2){
            wc.initialise(args);
        }else{
            System.out.println("Usage : <numberOfThreads> <InputFilePath> <OutputFilePath>");
        }

    }

    private void initialise(String[] args) {
        long startTime = System.currentTimeMillis();
        int numberOfThreads = Integer.parseInt(args[0]);
        String fileName = args[1];
        String outputFileName = args[2];
        Thread[] threads = new Thread[numberOfThreads];
        wordCountRunnable = new WordCountRunnable[numberOfThreads];
        try {
            LineNumberReader lnr = new LineNumberReader(new FileReader(new File(fileName)));
            lnr.skip(Long.MAX_VALUE);
            int totalNumberOfLines = lnr.getLineNumber() + 1;
            lnr.close();
            int offset = 0;
            int numberOfLinesPerThread = totalNumberOfLines/numberOfThreads + (totalNumberOfLines%numberOfThreads==0?0:1);
            for(int i =0;i<numberOfThreads;i++){
                wordCountRunnable[i] = new WordCountRunnable(fileName,offset,offset+numberOfLinesPerThread); //Reads input from file 1
                offset = offset + numberOfLinesPerThread;
                threads[i] = new Thread(wordCountRunnable[i],"thread"+(i+1));
                threads[i].start();
            }
            for(int i =0;i<numberOfThreads;i++){
                threads[i].join();
            }
            HashMap<String,Integer> finalResults = new HashMap<String, Integer>();
            for(int i =0;i<numberOfThreads;i++){
                HashMap<String,Integer> intermediateMap = wordCountRunnable[i].wordCountMap;
                Iterator it = intermediateMap.entrySet().iterator();
                while(it.hasNext()){
                    Map.Entry<String,Integer> pair = (Map.Entry<String, Integer>) it.next();
                    String key = pair.getKey();
                    Integer value = pair.getValue();
                    if(finalResults.containsKey(key)){
                        Integer computedValue = finalResults.get(key);
                        finalResults.put(key,computedValue+value);
                    }else{
                        finalResults.put(key,value);
                    }
                }
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Total Time in ms : "+(endTime-startTime));

            Iterator it = finalResults.entrySet().iterator();
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFileName));
            while(it.hasNext()){
                Map.Entry<String,Integer> pair = (Map.Entry<String, Integer>) it.next();
                bufferedWriter.write(pair.getKey()+" :: "+pair.getValue()+" \n");
            }
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    /**
     * Runnable for reading data from a file
     */
    public static class WordCountRunnable implements Runnable {

        String fileName;
        int startIndex;
        int endIndex;
        HashMap<String,Integer> wordCountMap;

        WordCountRunnable(String fileName,int startIndex,int endIndex) {
            this.fileName = fileName;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            wordCountMap = new HashMap<String, Integer>();
        }

        public synchronized void run() {
            try {
                BufferedReader br = new BufferedReader(new FileReader(fileName));
                String line;
                int lineCount=0;
                while(lineCount!=startIndex && (line = br.readLine()) != null){
                    lineCount++;
                }
                while(lineCount!=endIndex && (line = br.readLine()) != null){
                    lineCount++;
                    String[] var = line.split("\\s+");
                    for(int i = 0 ; i < var.length ; i++){
                        String newstr = var[i].replaceAll("[^A-Za-z]+", "");
                        if(wordCountMap.containsKey(newstr)){
                            int count = wordCountMap.get(newstr);
                            wordCountMap.put(newstr,++count);
                        }else{
                            wordCountMap.put(newstr,1);
                        }
                    }
                }

                br.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
