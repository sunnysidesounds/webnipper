package com.webnipper;

import java.util.StringTokenizer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.Crawl;
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

         /*

        // Old Code Keeping just or now.

        // Let's dump the segments we have to see what we have obtained. You
        // need to refresh your workspace to see the new folders. You can see
        // plaintext by going into dump folder and examining "dump".
        String dumpArg = "-dump crawl/segments/* dump -nocontent -nofetch -nogenerate -noparse -noparsedata";

        // Run dump
        try {
            SegmentReader.main(tokenize(dumpArg));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }

        System.setProperty("solr.solr.home", "/home/emre/solr");
        CoreContainer.Initializer initializer = new CoreContainer.Initializer();
        CoreContainer coreContainer;

        try {
            coreContainer = initializer.initialize();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        } catch (ParserConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        } catch (SAXException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }
        EmbeddedSolrServer server = new EmbeddedSolrServer(coreContainer, "");

        // Arguments for indexing
        String indexArg = "local crawl/crawldb -linkdb crawl/linkdb crawl/segments/*";

        // Run indexing tool
        try {
            ToolRunner.run(NutchConfiguration.create(),
                    new SolrIndexer(server), tokenize(indexArg));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }

        // Let's query for something!
        SolrQuery query = new SolrQuery();
        query.setQuery("title:queen"); // Searching queen in query
        query.addSortField("title", SolrQuery.ORDER.asc);
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

        // Shut down the container so JVM ends.
        coreContainer.shutdown();

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
