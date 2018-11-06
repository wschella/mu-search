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

  # data is an array of json/hashes, ordered according to
  # https://www.elastic.co/guide/en/elasticsearch/reference/6.4/docs-bulk.html
  def bulk_update_document index, data
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/_bulk")
    req = Net::HTTP::Post.new(uri)

    body = ""
    data.each do |datum|
      body += datum.to_json + "\n"
    end

    log.info "DATA: #{body}"
    req.body = body
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

  def count index:, query_string: nil, query: nil, sort: nil
    if query_string
      uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/_count?q=#{query_string}&sort=#{sort}")
      req = Net::HTTP::Get.new(uri)
    else 
      uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/_count")
      req = Net::HTTP::Get.new(uri)
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
    predicate_s = predicate.is_a? String ? predicate : predicate.join("/")
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


def find_matching_access_rights type, allowed_groups, used_groups
  allowed_group_set = allowed_groups.map { |g| "\"#{g}\"" }.join(",")
  used_group_set = used_groups.map { |g| "\"#{g}\"" }.join(",")

  # simplified example
  query_result = query <<SPARQL
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
    data = []
    query_result = query <<SPARQL
    SELECT DISTINCT ?id WHERE {
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
      # client.put_document(index, uuid, document.to_json)
      data.push ({ index: { _id: uuid } })
      data.push document
    end
    client.bulk_update_document index, data
  end

  { index: index, document_types: count_list }.to_json
end


# Currently supports ES methods that can be given a single value, e.g., match, term, prefix, fuzzy, etc.
# i.e., any method that can be written: { "query": { "METHOD" : { "field": "value" } } }
# * not supported yet: everything else, e.g., value, range, boost...
# Currently combined using { "bool": { "must": { ... } } } 
# * to do: range queries
# * to do: sort
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


def format_results type, count, page, size, results
  last_page = count/size
  next_page = [page+1, last_page].min
  prev_page = [page-1, 0].max

  { 
    count: count,
    data: JSON.parse(results)["hits"]["hits"].map do |result|
      {
        type: type,
        id: result["_id"],
        attributes: result["_source"]
      }
    end,
    links: {
      self: "http://application/",
      first: "page[number]=0&page[size]=#{size}",
      last: "page[number]=#{last_page}&page[size]=#{size}",
      prev: "page[number]=#{prev_page}&page[size]=#{size}",
      next: "page[number]=#{next_page}&page[size]=#{size}"
    }
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

  if params["page"]
    page = params["page"]["number"] or 0
    size = params["page"]["size"] or 10
  else
    page = 0
    size = 10
  end

  es_query = construct_query

  count_result = JSON.parse(client.count index: index, query: es_query)
  count = count_result["count"]

  # add pagination parameters
  es_query["from"] = page * size
  es_query["size"] = size

  results = client.search index: index, query: es_query
  format_results( type, count, page, size, results).to_json
end


# Using raw ES search DSL, mostly for testing
# Need to think through several things, such as pagination
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

  count_query = es_query
  count_query.delete("from")
  count_query.delete("size")
  count_result = JSON.parse(client.count index: index, query: es_query)
  count = count_result["count"]

  format_results(type, count, 0, 10, client.search(index: index, query: es_query)).to_json
end
