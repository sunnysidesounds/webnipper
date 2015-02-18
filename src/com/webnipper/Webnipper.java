package com.webnipper;

import java.util.StringTokenizer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.Crawl;
import org.apache.nutch.crawl.*;
import org.apache.hadoop.conf.*;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.crawl.LinkDbReader;


public class Webnipper {

    public static String crawldbPath = "./crawl/crawldb";

    public static void main(String[] args) {


        //Crawl Tool
       try {
            String crawlArg = "urls -dir crawl -threads 5 -depth 2 -topN 20";
            messageHeader("Nutch Crawl Tool!");
            ToolRunner.run(NutchConfiguration.create(), new Crawl(), tokenize(crawlArg));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }


        // Crawl Stats
       try {
            CrawlDbReader dbr = new CrawlDbReader();
            Configuration conf = NutchConfiguration.create();
            messageHeader("Nutch Stats Tool!");
            dbr.processStatJob(crawldbPath, conf, true);

        } catch (Exception e) {
            e.printStackTrace();
        }


        // Dump Nutch Results
        try {
            String dumpArg = "./crawl/linkdb -dump ./rawdumps -format csv";
            messageHeader("Nutch Dump Tool!");
            ToolRunner.run(NutchConfiguration.create(), new LinkDbReader(), tokenize(dumpArg));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }



    }

    /**
     * Helper function to convert a string into an array of strings by
     * separating them using whitespace.
     *
     * @param str string to be tokenized
     * @return an array of strings that contain a each word each
     */
    public static String[] tokenize(String str) {
        StringTokenizer tok = new StringTokenizer(str);
        String tokens[] = new String[tok.countTokens()];
        int i = 0;
        while (tok.hasMoreTokens()) {
            tokens[i] = tok.nextToken();
            i++;
        }

        return tokens;

    }

    public static void messageHeader(String title){
        System.out.println("**********************************");
        System.out.println("Running " + title);
        System.out.println("**********************************");
    }

}
