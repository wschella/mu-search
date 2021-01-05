
# Executes a query directly on the database.  In this setting, it
# means we pass the mu-auth-sudo header.
#
#   - q: String SPARQL query to be executed
#
def direct_query query_string, retries = 6
  MuSearch::SPARQL.direct_query(query_string, retries)
end

# Verifies whether or not the SPARQL endpoint is up.  Tries to execute
# an ASK query for any triple and outputs positively if the endpoint
# confirms there is something there.
def sparql_up
  begin
    direct_query "ASK { ?s ?p ?o }"
  rescue StandardError => e
    log.debug e
    false
  end
end


