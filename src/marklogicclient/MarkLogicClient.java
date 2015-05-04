package marklogicclient;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.io.JacksonParserHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.MatchLocation;
import com.marklogic.client.query.MatchSnippet;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;


public class MarkLogicClient {
  public MarkLogicClient() {
    super();
  }

  public void searchMarkLogic() {
    DatabaseClient client =
      DatabaseClientFactory.newClient("localhost", 9010, "admin", "marklogic1",
                                      Authentication.DIGEST);

    QueryManager queryMgr = client.newQueryManager();
   
    StringQueryDefinition query = queryMgr.newStringDefinition();
    //The search string
    query.setCriteria("bigdata");

    SearchHandle handle = new SearchHandle();

    queryMgr.search(query, handle);

    MatchDocumentSummary[] results = handle.getMatchResults();
    System.out.println("Total Results: " + handle.getTotalResults());

    // Iterate over the results
    int cnt = 0;
    long numResults = handle.getTotalResults();

    for (MatchDocumentSummary result : results) {
    	
      // get the list of match locations for this result
      MatchLocation[] locations = result.getMatchLocations();
      // iterate over the match locations
      for (MatchLocation location : locations) {
        // iterate over the snippets at a match location
        for (MatchSnippet snippet : location.getSnippets()) {
          System.out.println(cnt++ + "." + snippet.getText());
        }
      }
    }

  }
  
  public void queryMarkLogic() {
	    DatabaseClient client =
	    	      DatabaseClientFactory.newClient("localhost", 9010, "admin", "marklogic1",
	    	                                      Authentication.DIGEST);

	  // Build a structured query. Here we're doing a pretty broad collection query 
	  StructuredQueryBuilder builder = client.newQueryManager()
	    .newStructuredQueryBuilder();
	  QueryDefinition query = builder.term("bigdata");

	  DocumentPage page = client.newDocumentManager().search(query, 1);

	  // Iterate through the results, which include the raw documents,
	  // available with a ReadHandle.
	    System.out.println("Total Results: " + page.getTotalSize());
	  for (DocumentRecord doc : page) {
	    System.out.println(doc.getContent(new JacksonParserHandle()));
	  }
  }

  public static void main(String[] args) {
    MarkLogicClient markLogicClient = new MarkLogicClient();
    System.out.println("Query Results:");
    //markLogicClient.searchMarkLogic();
    markLogicClient.queryMarkLogic();
  }
}
