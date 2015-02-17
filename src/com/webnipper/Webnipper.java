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


public class Webnipper {

    public static void main(String[] args) {


        //Run Crawl Tool
        try {
            String crawlArg = "urls -dir crawl -threads 5 -depth 3 -topN 20";
            System.out.println("Running Crawl Tool!");
            System.out.println("**********************************");
            ToolRunner.run(NutchConfiguration.create(), new Crawl(),
                    tokenize(crawlArg));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        CrawlDbReader dbr = new CrawlDbReader();
        Configuration conf = NutchConfiguration.create();
        try {
            System.out.println("Running Nutch Stats Tool!");
            System.out.println("**********************************");

            dbr.processStatJob("/Users/jasonalexander/Dropbox/development/java/webnipper/crawl/crawldb", conf, true);

           /*
            CrawlDatum res = dbr.get("/Users/jasonalexander/Dropbox/development/java/webnipper/crawl/crawldb/current", "http://edition.cnn.com/WORLD/", conf);
            if (res != null) {
                System.out.println(res);
            } else {
                System.out.println("not found");
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }

       /*

        //Run Solr Index Tool
        try {
            String indexArg = "http://localhost:8983/solr -reindex";
            System.out.println("Running Solr Index Tool!");
            System.out.println("**********************************");
            ToolRunner.run(NutchConfiguration.create(), new SolrIndexer(),
                    tokenize(indexArg));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        // Run Solr Query Tool
        try {
            String url = "http://localhost:8983/solr";
            System.out.println("Running Solr Query Tool!");
            System.out.println("**********************************");
            CommonsHttpSolrServer server = new CommonsHttpSolrServer(url);
            SolrQuery query = new SolrQuery();
            query.setQuery("content:mycontent"); // Searching mycontent in query
            query.addSortField("content", SolrQuery.ORDER.asc);
            QueryResponse rsp;
            try {
                rsp = server.query(query);
            } catch (SolrServerException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }
            // Display the results in the console
            SolrDocumentList docs = rsp.getResults();
            for (int i = 0; i < docs.size(); i++) {
                System.out.println(docs.get(i).get("title").toString() + " Link: "
                        + docs.get(i).get("url").toString());
            }
        } catch (Exception e){
            e.printStackTrace();
            return;
        }
          */

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

}
