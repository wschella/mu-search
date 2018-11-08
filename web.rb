require 'net/http'


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
    predicate_s = predicate.is_a?(String) ? predicate : predicate.join("/")
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
  rights = settings.rights[type][used_groups]
  rights and rights[:index]
end


def store_access_rights type, index, allowed_groups, used_groups
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

  new_rights_set = { uri: uri, index: index,
                     allowed_groups: allowed_groups, used_groups: used_groups }

  settings.rights[type][used_groups] =  new_rights_set
end


def load_access_rights type
  rights = {}

  query_result = query <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?rights a <http://mu.semte.ch/vocabularies/authorization/AccessRights>;
               <http://mu.semte.ch/vocabularies/authorization/hasType> "#{type}";
               <http://mu.semte.ch/vocabularies/authorization/hasEsIndex> ?index
    }
  }
SPARQL

  query_result.each do |result|
    uri = result["rights"].to_s
    allowed_groups_result = query <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> <http://mu.semte.ch/vocabularies/authorization/hasAllowedGroup> ?group
    }
  }
SPARQL
    allowed_groups = allowed_groups_result.map { |g| g["group"].to_s }

    used_groups_result = query <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> <http://mu.semte.ch/vocabularies/authorization/hasUsedGroup> ?group
    }
  }
SPARQL
    used_groups = used_groups_result.map { |g| g["group"].to_s }
  
    rights[used_groups] = { uri: uri, index: result["index"].to_s, allowed_groups: allowed_groups, used_groups: used_groups }
  end

  rights
end


def current_index type_path
  allowed_groups, used_groups = current_groups
  type = settings.type_defs[type_path]["type"] || settings.type_defs[type_path]["types"].join("-") # abstract this!
  index = find_matching_access_rights type, allowed_groups, used_groups
end


# * to do: check if index exists before creating it
# * to do: how to name indexes?
def create_current_index client, type_path
  allowed_groups, used_groups = current_groups

  # abstract this!
  type = settings.type_defs[type_path]["type"] || settings.type_defs[type_path]["types"].join("-")

  # placeholder
  index = type + "-" + allowed_groups.join("-")
  
  store_access_rights type, index, allowed_groups, used_groups
  client.create_index index, settings.type_defs[type_path]["mappings"]
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

  type_def = settings.type_defs[type_path]


  # Can we do this inversion at load-up?
  if type_def["type"]
    rdf_type = type_def["rdf_type"]
    types = [{
               type: type_def["type"],
               rdf_type: rdf_type,
               count: count_documents(rdf_type),
               properties: type_def["properties"]
              }]
  else
    types = type_def["types"].map do |type_name|
      source_type_def = settings.types[type_name]
      rdf_type = source_type_def["rdf_type"]

      { 
        type: type_name,
        rdf_type: rdf_type,
        count: count_documents(rdf_type),
        properties: Hash[
          type_def["properties"].map do |property|
            property_name = property["name"]
            [property_name, source_type_def["properties"][property_name]]
          end
        ]
      }
    end
  end

  # if type   
  #   rdf_type = type_def["rdf_type"]
  #   rdf_types = [rdf_type]
  #   count = count_documents rdf_type
  #   count_list.push( { type: type, count: count } )
  # else
  #   types = type_def["types"]
  #   types.each do |type_name|
  #     rdf_type = settings.types[type_name]["rdf_type"] 
  #     count = count_documents rdf_type
  #     count_list.push {type: type_name, count: count}
  #   end
  # end

  types.each do |type|
    rdf_type = type[:rdf_type]
    properties = type[:properties]

    count = type[:count]
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

      client.bulk_update_document index, data unless data.empty?
    end
  end

  { index: index, document_types: types }.to_json
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


configure do
  configuration = JSON.parse File.read('/config/config.json')
  set :types, Hash[
        configuration["types"].collect do |type_def|
          [type_def["type"], type_def]
        end
      ]

  # determines batch size for indexing documents (SPARQL OFFSET)
  set :offset, configuration["offset"]

  # invert definitions for easy lookup by path
  type_defs = {}
  rights = {}
  configuration["types"].each do |type_def|
    type_defs[type_def["on_path"]] = type_def
    type = type_def["type"] || type_def["types"].join("-")
    rights[type] = load_access_rights type
  end

  set :type_defs, type_defs

  set :rights, rights
end


get "/:type_path/index" do |type_path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  index = current_index type_path
  create_current_index client, type_path unless index
  make_index client, type_path, index
end


get "/:type_path/search" do |type_path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = settings.type_defs[type_path]["type"]

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
  type = settings.type_defs[type_path]["type"]

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
