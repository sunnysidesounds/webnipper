package com.webnipper;

import java.util.StringTokenizer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.Crawl;
import org.apache.nutch.crawl.*;
import org.apache.hadoop.conf.*;
import org.apache.nutch.indexer.solr.SolrIndexer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.nutch.crawl.LinkDbReader;


public class Webnipper {

    public static String crawldbPath = "./crawl/crawldb";

    public static void main(String[] args) {


        //Crawl Tool
        try {
            String crawlArg = "urls -dir crawl -threads 5 -depth 2 -topN 20";
            messageHeader("Nutch Crawl Tool!");
            ToolRunner.run(NutchConfiguration.create(), new Crawl(),
                    tokenize(crawlArg));
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


        //Dump urls to csv
        try {
            String dumpArg = "./crawl/linkdb -dump ./rawdumps -format csv";
            messageHeader("Nutch Dump Tool!");
            ToolRunner.run(NutchConfiguration.create(), new LinkDbReader(), tokenize(dumpArg));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }



        //Run Solr Index Tool
        try {
            String indexArg = "http://localhost:8983/solr " + crawldbPath + " -linkdb ./crawl/linkdb";
            messageHeader("Solr Indexing Tool!");
            ToolRunner.run(NutchConfiguration.create(), new SolrIndexer(),
                    tokenize(indexArg));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        // Run Solr Query Tool
        try {
            String url = "http://localhost:8983/solr";
            messageHeader("Solr Query Tool");
            CommonsHttpSolrServer server = new CommonsHttpSolrServer(url);
            SolrQuery query = new SolrQuery();
            query.setQuery("id:*"); // Searching mycontent in query
          //  query.addSortField("title", SolrQuery.ORDER.asc);
            QueryResponse rsp;
            try {
                System.out.println("Block 1");
                rsp = server.query(query);
            } catch (SolrServerException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }
            // Display the results in the console
            SolrDocumentList docs = rsp.getResults();
            for (int i = 0; i < docs.size(); i++) {
                System.out.println(docs.get(i).get("title").toString() + " Link: " + docs.get(i).get("url").toString());

            }
        } catch (Exception e){
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
