# Executes a query with specific headers set
#
#   - query: String SPARQL query
#   - headers: Object containing the headers which need to be sent.
#     Note that the object should have downcased headers with dashes
#     in between (eg: { 'mu-auth-allowed-groups': '[]' }).
def query_with_headers query, headers
  sparql_client.query query, { headers: headers }
end

# Executes a query with specific allowed_groups set.
#
#   - query_string: String SPARQL query to be executed.
#   - allowed_groups: Ruby JSON representation of the allowed groups
#     for this query execution.
#
# TODO: This method creates a new SPARQL client because I didn't know
# how to do this in a pretty manner.  We should provide some tooling
# and share it as an addon for the users of the mu-ruby-template.
def authorized_query query_string, allowed_groups, retries = 6
  begin
    allowed_groups_object = allowed_groups.map { |group| group }.to_json

    log.debug "Authorized query with allowed groups object #{allowed_groups_object}"

    options = { headers: { 'mu-auth-allowed-groups': allowed_groups_object } }

    my_sparql_client = SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'], options)

    my_sparql_client.query query_string, options
  rescue
    next_retries = retries - 1
    if next_retries == 0
      raise
    else
      log.warn "Could not execute authorized query (attempt #{6 - next_retries}): #{query_string} \n #{allowed_groups}"
      timeout = (6 - next_retries) ** 2
      sleep timeout
      authorized_query query_string, allowed_groups, next_retries
    end
  end
end


# Executes a query authorized with the access rights of the current
# user.
#
# TODO: Shouldn't be necessary: ruby template should pass headers on
def request_authorized_query query_string
  query query_string
end

# Executes a query directly on the database.  In this setting, it
# means we pass the mu-auth-sudo header.
#
#   - q: String SPARQL query to be executed
#
# TODO: this is now configured in the settings block.  It's not a
# pretty way of handling this.  We should find a solid solution for
# the mu-auth-sudo queries.
def direct_query q, retries = 6
  begin
    settings.db.query q
  rescue
    next_retries = retries - 1
    if next_retries == 0
      raise
    else
      log.warn "Could not execute raw query (attempt #{6 - next_retries}): #{q}"
      timeout = (6 - next_retries) ** 2
      sleep timeout
      direct_query query_string, next_retries
    end
  end
end

# Executes a document count for documents of a particular type.
# Optionally overriding the allowed_groups of the current users.
#
#   - rdf_type: URI type of the documents to be counted
#   - allowed_groups: When supplied, it should be a Ruby JSON object
#     representing the allowed_groups under which the count is to be
#     executed.
#
# Returns the count as found in the database.
def count_documents rdf_type, allowed_groups = nil
  sparql_query =  <<SPARQL
      SELECT (COUNT(?doc) AS ?count) WHERE {
        ?doc a <#{rdf_type}>
      }
SPARQL

  log.debug "Counting documents for #{allowed_groups}"

  query_result =
    if allowed_groups
      authorized_query sparql_query, allowed_groups
    else
      request_authorized_query sparql_query
    end

  documents_count = query_result.first["count"].to_i

  log.info "Found #{documents_count} documents for #{allowed_groups}."

  documents_count
end


# Memoized function which verifies whether or not a given subject uri
# is of a particular type.
#
#   - s: String URI of the subject.
#   - rdf_type: String RDF type URI.
#
# TODO: Use escape helpers to escape URIs.
def is_type s, rdf_type
  @subject_types ||= {}
  if @subject_types.has_key? [s, rdf_type]
    @subject_types[s, rdf_type]
  else
    direct_query "ASK WHERE { <#{s}> a <#{rdf_type}> }"
  end
end

# Memoized function which verifies whether a user with particular
# allowed_groups can see that a uri has a particular type.
#
#   - s: String URI of the subject.
#   - rdf_type: String RDF type URI.
#   - allowed_groups: Ruby JSON representation of the allowed_groups.
#
# TODO: Use escape helpers to escape URIs.
def is_authorized s, rdf_type, allowed_groups
  @authorizations ||= {}
  if @authorizations.has_key? [s, rdf_type, allowed_groups]
    @authorizations[s, rdf_type, allowed_groups]
  else

    authorized_query "ASK WHERE { <#{s}> a <#{rdf_type}> }", allowed_groups
  end
end

# Retrieves the subject for a given UUID from the triplestore.
#
#  - s: String uuid of the item.
#
# TODO: Consider memoizing this function.
def get_uuid s
  query_result = direct_query <<SPARQL
SELECT ?uuid WHERE {
   <#{s}> <http://mu.semte.ch/vocabularies/core/uuid> ?uuid
}
SPARQL
  uuid = query_result && query_result.first && query_result.first["uuid"]
end

# Converts the string predicate from the configuration into a portion
# for the path used in a SPARQL query.
#
# The strings configered in the config file may have a ^ sign to
# indicate inverse.  If that exists, we need to interpolate the URI.
#
#   - predicate: Predicate to be escaped.
def predicate_string_term predicate
  if predicate[0] == '^'
    "^<#{predicate[1..predicate.length-1]}>"
  else
    "<#{predicate}>"
  end
end

# Coverts the SPARQL predicate definition from the config into a
# triple path.
#
# The configuration in the configuration file may contain an inverse
# (using ^) and/or a list (using the array notation).  These need to
# be converted into query paths so we can correctly fetch the
# contents.
#
#   - predicate: Predicate definition as supplied in the config file.
#     Either a string or an array.
#
# TODO: I believe the construction with the query paths leads to
# incorrect invalidation when delta's arrive.  Perhaps we should store
# the relevant URIs in the stored document so we can invalidate it
# correctly when new content arrives.
def make_predicate_string predicate
  if predicate.is_a? String
    predicate_string_term predicate
  else
    predicate.map { |pred| predicate_string_term pred }.join("/")
  end
end


# Constructs a SPARQL query which selects all requested properties for
# an ElasticSearch document.  Can identify sources either by UUID or
# by URI.
#
#   - uuid: uuid of the instance as a string.
#   - uri: URI of the instance as a string (only used when uuid is not
#     supplied.
#   - properties: properties to be discovered, as an array of
#     definitions supplied from the configuration.
def make_property_query uuid, uri, property_key, property_predicate
  id_line =
    if uuid
      "?doc <http://mu.semte.ch/vocabularies/core/uuid> \"#{uuid}\". "
    else
      " "
    end

  s = uuid ? "?doc" : "<#{uri}>"

  predicate = property_predicate.is_a?(Hash) ? property_predicate["via"] : property_predicate
  predicate_s = make_predicate_string predicate

  <<SPARQL
    SELECT DISTINCT ?#{property_key} WHERE {
     #{id_line}
     #{s} #{predicate_s} ?#{property_key}
    }
SPARQL
end


# helper function for fetch_document_to_index
# retrieves the content of a linked resource
def parse_nested_object(results, key, properties)
  links = results.collect do |result|
    link_uri = result[key]
    if link_uri
      linked_document, pipeline = fetch_document_to_index uri: link_uri, properties: properties, allowed_groups: allowed_groups
      linked_document
    else
      nil
    end
  end

  [key, denumerate(links)]
end

# helper function for fetch_document_to_index
# retrieves content of the linked attachments
def parse_attachment(results, key)
  attachments = results.collect do |result|
    file_path = result[key]
    if file_path
      file_path = file_path.to_s.sub("share://","")
      begin
        filesize=File.size?("/data/#{file_path}")
        if filesize > ENV['MAXIMUM_FILE_SIZE'].to_i
          raise "#{file_path} filesize #{filesize} is too large, not reading "
        end
        File.open("/data/#{file_path}", "rb") do |file|
          contents = Base64.strict_encode64 file.read
          contents
        end
      rescue Errno::ENOENT, IOError => e
        log.warn "Error reading \"/data/#{file_path}\": #{e.inspect}"
        nil
      end
    else
      nil
    end
  end

  case attachments.length
  when 0
    [key, nil]
  when 1
    [key, attachments.first]
  else
    [key, attachments.collect { |attachment| { data: attachment} }]
  end
end

# utility function
def denumerate results
  case results.length
  when 0 then nil
  when 1 then results.first
  else results
  end
end


# Retrieves a document to index from the available parameters.  Is
# capable of coping with uuid or uri identification schemes, with
# properties as configured in the user's config, as well as with
# optional allowed_groups.
#
# This is your one-stop shop to fetch all info to index a document.
#
#   - uuid: String uuid representing the item to fetch
#   - uri: URI of the item to fetch (not used if uuid is suplpied)
#   - properties: Array of properties as configured in the user's
#     configuration file.
#   - allowed_groups: Optional setting allowing to scope down the
#     retrieved contents by specific access rights.
def fetch_document_to_index uuid: nil, uri: nil, properties: nil, allowed_groups: nil
  pipeline = false
  key_value_tuples = properties.collect do |key, val|
    query = make_property_query(uuid, uri, key, val)
    results = allowed_groups ? authorized_query(query, allowed_groups) : request_authorized_query(query)

    if val.is_a? Hash
        # file attachment
        if val["attachment_pipeline"]
          key, value = parse_attachment(results, key)
          if value.is_a?(Array)
            pipeline = "#{val["attachment_pipeline"]}_array"
          else
            pipeline = val["attachment_pipeline"]
          end
          [key, value]
        # nested object
        elsif val["rdf_type"]
          parse_nested_object(results, key, val['properties'])
        end
    else
      values = results.collect do |result|
        case result[key]
        when RDF::Literal::Integer
          result[key].to_i
        when RDF::Literal::Double
          result[key].to_f
        when RDF::Literal::Decimal
          result[key].to_f
        when RDF::Literal::Boolean
          result[key].to_s.downcase == 'true'
        when RDF::Literal::Time
            result[key].to_s
        when RDF::Literal::Date
            result[key].to_s
        when RDF::Literal::DateTime
          result[key].to_s
        when RDF::Literal
            result[key].to_s
        else
            result[key].to_s
        end
      end
      [key, denumerate(values)]
    end
  end

  document = Hash[key_value_tuples]
  return document, pipeline
end


# Verifies whether or not the SPARQL endpoint is up.  Tries to execute
# an ASK query for any triple and outputs positively if the endpoint
# confirms there is something there.
def sparql_up
  begin
    direct_query "ASK { ?s ?p ?o }"
  rescue
    false
  end
end
