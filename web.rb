require 'net/http'

configure do

  configuration = JSON.parse File.read('/config/config.json')

  # determines batch size for indexing documents (SPARQL OFFSET)
  set :offset, configuration["offset"]

  # invert definitions for easy lookup by path
  type_defs = {}
  configuration["types"].each do |type_def|
    type_defs[type_def["on_path"]] = type_def
  end

  set :types, type_defs
end

# a quick as-needed Elastic API, for use until
# the conflict with Sinatra::Utils is resolved
# * does not follow the standard API
# * see: https://github.com/mu-semtech/mu-ruby-template/issues/16
class Elastic
  def initialize(host: 'localhost', port: 9200)
    @host = host
    @port = port
    @port_s = port.to_s
  end

  def run(uri, req)
    req['content-type'] = 'application/json'

    res = Net::HTTP.start(uri.hostname, uri.port) do |http|
      http.request(req)
    end

    case res
    when Net::HTTPSuccess, Net::HTTPRedirection
      res.body
    else
      res.value
    end
  end

  def create_index index, mappings = nil
    uri = URI("http://#{@host}:#{@port_s}/#{index}")
    req = Net::HTTP::Put.new(uri)
    if mappings
      req.body = { mappings: { _doc: { properties: mappings } } }.to_json
    end

    run(uri, req)
  end

  def refresh_index index
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_refresh")
    req = Net::HTTP::Post.new(uri)
    run(uri, req)
  end

  def get_document index, id
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{id}")
    req = Net::HTTP::Get.new(uri)
    run(uri, req)
  end

  def put_document index, id, document
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{id}")
    req = Net::HTTP::Put.new(uri)
    req.body = document
    run(uri, req)
  end

  def update_document index, id, document
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{id}/_update")
    req = Net::HTTP::Post.new(uri)
    req.body = document
    run(uri, req)
  end

  def delete_document index, id
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{id}")
    req = Net::HTTP::Delete.new(uri)
    run(uri, req)
  end

  def search index:, query_string: nil, query: nil, sort: nil
    if query_string
      uri = URI("http://#{@host}:#{@port_s}/#{index}/_search?q=#{query_string}&sort=#{sort}")
      req = Net::HTTP::Post.new(uri)
    else 
      uri = URI("http://#{@host}:#{@port_s}/#{index}/_search")
      req = Net::HTTP::Post.new(uri)
      req.body = query.to_json
    end

    run(uri, req)    
  end
end


def make_property_query uuid, properties
  select_variables_s = ""
  property_predicates = []

  properties.each do |key, predicate|
    select_variables_s += " ?#{key} " 
    property_predicates.push "#{predicate} ?#{key}"
  end

  property_predicates_s = property_predicates.join("; ")

  <<SPARQL
    SELECT #{select_variables_s} WHERE { 
     ?doc  <http://mu.semte.ch/vocabularies/core/uuid> "#{uuid}";
           #{property_predicates_s}.
    }
SPARQL
end


def count_documents type
  query_result = query <<SPARQL
      SELECT (COUNT(?doc) AS ?count) WHERE {
        ?doc a #{type}
      }
SPARQL

  query_result.first["count"].to_i
end


# * to do: implement real matching scheme
def find_matching_access_rights type, allowed_groups, used_groups
  allowed_group_set = allowed_groups.map { |g| "\"#{g}\"" }.join(",")
  used_group_set = used_groups.map { |g| "\"#{g}\"" }.join(",")

#   query_result = query <<SPARQL
#   SELECT ?index WHERE {
#     GRAPH <http://mu.semte.ch/authorization> {
#         ?rights a <http://mu.semte.ch/vocabularies/authorization/AccessRights>;
#                <http://mu.semte.ch/vocabularies/authorization/hasType> "#{type}";
#                <http://mu.semte.ch/vocabularies/authorization/hasUsedGroup> #{allowed_group_set};
#                <http://mu.semte.ch/vocabularies/authorization/hasEsIndex> ?index
#     }
#   }
# SPARQL

  query_result = query  = <<SPARQL
  SELECT ?index WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?rights a <http://mu.semte.ch/vocabularies/authorization/AccessRights>;
               <http://mu.semte.ch/vocabularies/authorization/hasType> "#{type}";
               <http://mu.semte.ch/vocabularies/authorization/hasAllowedGroup> #{allowed_group_set};
               <http://mu.semte.ch/vocabularies/authorization/hasEsIndex> ?index
        FILTER NOT EXISTS {
           ?rights <http://mu.semte.ch/vocabularies/authorization/hasAllowedGroup> ?group.
            FILTER ( ?group NOT IN (#{allowed_group_set}) )
        }
    }
  }
SPARQL

  result = query_result.first
  query_result.first ? query_result.first["index"] : nil
end


def put_access_rights type, index, allowed_groups, used_groups
  uuid = generate_uuid()
  uri = "<http://mu.semte.ch/authorization/elasticsearch/indexes/#{uuid}>"
  
  allowed_group_set = allowed_groups.map { |g| "\"#{g}\"" }.join(",")
  used_group_set = used_groups.map { |g| "\"#{g}\"" }.join(",")

  query_result = query <<SPARQL
  INSERT DATA {
    GRAPH <http://mu.semte.ch/authorization> {
        #{uri} a <http://mu.semte.ch/vocabularies/authorization/AccessRights>;
               <http://mu.semte.ch/vocabularies/core/uuid> "#{uuid}";
               <http://mu.semte.ch/vocabularies/authorization/hasType> "#{type}";
               <http://mu.semte.ch/vocabularies/authorization/hasAllowedGroup> #{allowed_group_set};
               <http://mu.semte.ch/vocabularies/authorization/hasUsedGroup> #{used_group_set};
               <http://mu.semte.ch/vocabularies/authorization/hasEsIndex> "#{index}"
    }
  }
SPARQL
end


def current_index type_path
  allowed_groups, used_groups = current_groups
  type = settings.types[type_path]["type"]
  index = find_matching_access_rights type, allowed_groups, used_groups
end


# * to do: check if index exists before creating it
# * to do: how to name indexes?
def create_current_index client, type_path
  allowed_groups, used_groups = current_groups
  type = settings.types[type_path]["type"]
  index = type + "-" + allowed_groups.join("-") # placeholder
  put_access_rights type, index, allowed_groups, used_groups
  client.create_index index, settings.types[type_path]["mappings"]
  make_index client, type_path, index 
  index
end


def current_groups
  allowed_groups_s = request.env["HTTP_MU_AUTH_ALLOWED_GROUPS"]
  allowed_groups = allowed_groups_s ? JSON.parse(allowed_groups_s).map { |e| e["value"] } : []

  used_groups_s = request.env["HTTP_MU_AUTH_USED_GROUPS"]
  used_groups = used_groups_s ? JSON.parse(used_groups_s).map { |e| e["value"] } : []

  return allowed_groups.sort, used_groups.sort
end


# indexes all documents (or all authorized documents,
# if queries are routed through an authorization service)
# * to do: should do batch updates
def make_index client, type_path, index
  count_list = [] # for reporting

  type_def = settings.types[type_path]
  type = type_def["type"]
  rdf_type = type_def["rdf_type"]
  properties = type_def["properties"]

  count = count_documents rdf_type

  count_list.push( { type: type, count: count } )

  (0..(count/10)).each do |i|
    offset = i*settings.offset
    query_result = query <<SPARQL
    SELECT ?id WHERE {
      ?doc a #{rdf_type};
           <http://mu.semte.ch/vocabularies/core/uuid> ?id
    } LIMIT 100 OFFSET #{offset}
SPARQL

    query_result.each do |result|
      uuid = result[:id].to_s
      query_result = query make_property_query uuid, properties
      result = query_result.first

      document = {
        title: result[:title].to_s,
        description: result[:description].to_s,
      }
      client.put_document(index, uuid, document.to_json)
    end

  end

  { index: index, document_types: count_list }.to_json
end


# Currently supports ES methods that can be given a single value, e.g., match, term, prefix, fuzzy, etc.
# i.e., anything that can be written: { "query": { "METHOD" : { "field": "value" } } }
# not supported yet: everything else
# such as value, range, boost...
# Currently combined with { "bool": { "must": { ... } } } 
# * to do: range queries
# * to do: sort
# * to do: 
def construct_query
  filters = params["filter"].map do |field, v| 
    v.map do |method, val| 
      { method => { field => val } } 
    end
  end.first.first

  {
    query: {
      bool: {
        must: filters
      }
    }
  }

end


def format_results type, results
  { 
    data: JSON.parse(results)["hits"]["hits"].map do |result|
      {
        type: type,
        id: result["_id"],
        attributes: result["_source"]
      }
    end,
    links: { self: "http://application/" }
  }
end


post "/:type_path/index" do |type_path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  index = current_index type_path
  create_current_index client, type_path unless index
  make_index client, type_path, index
end


get "/:type_path/search" do |type_path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = settings.types[type_path]["type"]

  index = current_index type_path
  unless index 
    index = create_current_index client, type_path
    make_index client, type_path, index
    client.refresh_index index
  end

  es_query = construct_query

  format_results(type, client.search(index: index, query: es_query)).to_json
end


post "/:type_path/search" do |type_path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = settings.types[type_path]["type"]

  index = current_index type_path
  unless index 
    index = create_current_index client, type_path
    make_index client, type_path, index
    client.refresh_index index
  end

  es_query = @json_body

  format_results(type, client.search(index: index, query: es_query)).to_json
end
