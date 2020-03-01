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
  MuSearch::SPARQL.authorized_query(query_string, allowed_groups, retries)
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
def direct_query query_string, retries = 6
  MuSearch::SPARQL.direct_query(query_string, retries)
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

# Converts the string predicate from the configuration into a portion
# for the path used in a SPARQL query.
#
# The strings configered in the config file may have a ^ sign to
# indicate inverse.  If that exists, we need to interpolate the URI.
#
#   - predicate: Predicate to be escaped.
def predicate_string_term predicate
  MuSearch::SPARQL.predicate_string_term(predicate)
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
# incorrect invalidation when delta's arrive. Perhaps we should store
# the relevant URIs in the stored document so we can invalidate it
# correctly when new content arrives.
def make_predicate_string predicate
  MuSearch::SPARQL.make_predicate_string(predicate)
end


# Constructs a SPARQL query which selects all requested properties for
# an ElasticSearch document.
#
#   - uri: URI of the instance as a string
#   - properties: properties to be discovered, as an array of
#     definitions supplied from the configuration.
def make_property_query uri, property_key, property_predicate
  predicate = property_predicate.is_a?(Hash) ? property_predicate["via"] : property_predicate
  predicate_s = make_predicate_string predicate
  <<SPARQL
    SELECT DISTINCT ?#{property_key} WHERE {
     #{sparql_escape_uri(uri)} #{predicate_s} ?#{property_key}
    }
SPARQL
end


# helper function for fetch_document_to_index
# retrieves the content of a linked resource
def parse_nested_object(results, key, properties)
  links = results.collect do |result|
    link_uri = result[key]
    if link_uri
      fetch_document_to_index uri: link_uri, properties: properties, allowed_groups: allowed_groups
    else
      nil
    end
  end

  [key, denumerate(links)]
end

# helper function for fetch_document_to_index
# retrieves content of the linked attachments
# TODO: Consider whether it's appropriate to force_encode UTF-8 for attachments
def parse_attachment(tika_client, results, key, attachment_path_base)
  attachments = results.collect do |result|
    file_path = result[key]
    if file_path
      file_path = File.join(attachment_path_base, file_path.to_s.sub("share://",""))
      begin
        filesize = File.size(file_path)
        if filesize < ENV['MAXIMUM_FILE_SIZE'].to_i
          File.open(file_path, "rb") do |file|
            blob = file.read
            filehash = Digest::SHA256.hexdigest blob
            hash_path = '/cache/' + filehash

            if File.exists?(hash_path)
              contents = File.open(hash_path, "rb") do |file|
                log.debug "Tika: reading cached file: #{hash_path}"
                file.read.force_encoding('utf-8').unicode_normalize
              end
            else
              log.debug "Tika: processing #{file_path} to #{hash_path}"
              contents = tika_client.process_document(file_path, blob).force_encoding('utf-8').unicode_normalize
              File.open(hash_path, 'w') do |file|
                file.puts contents
              end
            end
            
            contents
          end
        else
          log.warn "Ignoring attachment #{file_path}: #{filesize} bytes exceeds allowed size of #{ENV["MAXIMUM_FILE_SIZE"]} bytes"
          nil
        end
      rescue Errno::ENOENT, IOError => e
        log.warn "Error reading \"#{file_path}\": #{e.inspect}"
        nil
      end
    else
      nil
    end
  end

  case attachments.length
  when 0
    [key, ""]
  when 1
    [key, attachments.first]
  else
    attachments = attachments.keep_if { |v| v } # filter out falsy values (If one of the array is falsy, others are not taken into account)
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
# capable of coping with uri identification schemes, with
# properties as configured in the user's config, as well as with
# optional allowed_groups.
#
# This is your one-stop shop to fetch all info to index a document.
#
#   - uri: URI of the item to fetch
#   - properties: Array of properties as configured in the user's
#     configuration file.
#   - allowed_groups: Optional setting allowing to scope down the
#     retrieved contents by specific access rights.
#   - attachment_path_base: base path to use for files
def fetch_document_to_index tika_client, uri: nil, properties: nil, allowed_groups: nil, attachment_path_base: '.'
  # we include uuid because it may be used for folding
  unless properties.has_key?("uuid")
    properties["uuid"] = ["http://mu.semte.ch/vocabularies/core/uuid"]
  end
  key_value_tuples = properties.collect do |key, val|
    query = make_property_query(uri, key, val)
    results = allowed_groups ? authorized_query(query, allowed_groups) : request_authorized_query(query)

    if val.is_a? Hash
        # file attachment
        if val["attachment_pipeline"]
          key, value = parse_attachment(tika_client, results, key, attachment_path_base)
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
  return document
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
