require 'net/http'
require 'digest'


# A quick as-needed Elastic API, for use until
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


def find_matching_access_rights type, allowed_groups, used_groups
  index = settings.indexes[type][used_groups]
  index and index[:index]
end


def store_access_rights type, index, allowed_groups, used_groups
  uuid = generate_uuid()
  uri = "http://mu.semte.ch/authorization/elasticsearch/indexes/#{uuid}"
  
  allowed_group_set = allowed_groups.map { |g| "\"#{g}\"" }.join(",")
  used_group_set = used_groups.map { |g| "\"#{g}\"" }.join(",")

  query_result = query <<SPARQL
  INSERT DATA {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
               <http://mu.semte.ch/vocabularies/core/uuid> "#{uuid}";
               <http://mu.semte.ch/vocabularies/authorization/objectType> "#{type}";
               <http://mu.semte.ch/vocabularies/authorization/hasAllowedGroup> #{allowed_group_set};
               <http://mu.semte.ch/vocabularies/authorization/hasUsedGroup> #{used_group_set};
               <http://mu.semte.ch/vocabularies/authorization/indexName> "#{index}"
    }
  }
SPARQL

  index_authorizations = {
    uri: uri,
    index: index,
    allowed_groups: allowed_groups,
    used_groups: used_groups 
  }

  settings.indexes[type][used_groups] =  index_authorizations
end


def load_access_rights type
  rights = {}

  query_result = query <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?rights a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
               <http://mu.semte.ch/vocabularies/authorization/objectType> "#{type}";
               <http://mu.semte.ch/vocabularies/authorization/indexName> ?index
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
  
    rights[used_groups] = { 
      uri: uri,
      index: result["index"].to_s,
      allowed_groups: allowed_groups, 
      used_groups: used_groups 
    }
  end

  rights
end


def current_index path
  allowed_groups, used_groups = current_groups
  type = get_type path

  find_matching_access_rights type, allowed_groups, used_groups
end


def get_type path
  settings.type_paths[path]
end


# * to do: check if index exists before creating it
# * to do: how to name indexes?
def create_current_index client, path
  allowed_groups, used_groups = current_groups

  type = get_type path

  index = Digest::MD5.hexdigest (type + "-" + allowed_groups.join("-"))
  
  store_access_rights type, index, allowed_groups, used_groups
  client.create_index index, settings.type_definitions[type]["es_mappings"]
  index_documents client, path, index 
  index
end


def current_groups
  allowed_groups_s = request.env["HTTP_MU_AUTH_ALLOWED_GROUPS"]
  allowed_groups = allowed_groups_s ? JSON.parse(allowed_groups_s).map { |e| e["value"] } : []

  used_groups_s = request.env["HTTP_MU_AUTH_USED_GROUPS"]
  used_groups = used_groups_s ? JSON.parse(used_groups_s).map { |e| e["value"] } : []

  return allowed_groups.sort, used_groups.sort
end


def count_documents rdf_type
  query_result = query <<SPARQL
      SELECT (COUNT(?doc) AS ?count) WHERE {
        ?doc a <#{rdf_type}>
      }
SPARQL

  query_result.first["count"].to_i
end


def make_property_query uuid, properties
  select_variables_s = ""
  property_predicates = []

  properties.each do |key, predicate|
    select_variables_s += " ?#{key} " 
    predicate_s = predicate.is_a?(String) ? predicate : predicate.join("/")
    property_predicates.push "<#{predicate}> ?#{key}"
  end

  property_predicates_s = property_predicates.join("; ")

  <<SPARQL
    SELECT #{select_variables_s} WHERE { 
     ?doc  <http://mu.semte.ch/vocabularies/core/uuid> "#{uuid}";
           #{property_predicates_s}.
    }
SPARQL
end


def type_definition_by_path path
  settings.type_definitions[settings.type_paths[path]]
end


def is_multiple_type? type_definition
  type_definition["composite_types"].is_a?(Array)
end


def multiple_type_expand_subtypes types, properties
  types.map do |type|
    source_type_def = settings.type_definitions[type]
    rdf_type = source_type_def["rdf_type"]

    { 
      "type" => type,
      "rdf_type" => rdf_type,
      "properties" => Hash[
        properties.map do |property|
          property_name = property["name"]
          mapped_name = 
            if property["mappings"]
              property["mappings"][type] || property_name
            else 
              property_name
            end
          [property_name, source_type_def["properties"][mapped_name]]
        end
      ]
    }
  end
end


def index_documents client, path, index
  settings.index_status[index] = :updating

  count_list = [] # for reporting

  type_def = type_definition_by_path path

  if is_multiple_type?(type_def)
    types = multiple_type_expand_subtypes type_def["composite_types"], type_def["properties"]
  else
    types = [type_def]
  end

  types.each do |type|
    rdf_type = type["rdf_type"]

    count = count_documents rdf_type
    type["count"] = count
    properties = type["properties"]

    (0..(count/settings.batch_size)).each do |i|
      offset = i*settings.batch_size
      data = []
      query_result = query <<SPARQL
    SELECT DISTINCT ?id WHERE {
      ?doc a <#{rdf_type}>;
           <http://mu.semte.ch/vocabularies/core/uuid> ?id
    } LIMIT 100 OFFSET #{offset}
SPARQL

      query_result.each do |result|
        uuid = result[:id].to_s
        query_result = query make_property_query uuid, properties
        result = query_result.first

        document = Hash[
          properties.collect do |key, val|
            [key, result[key]]
          end
        ]

        data.push ({ index: { _id: uuid } })
        data.push document
      end

      client.bulk_update_document index, data unless data.empty?

    end
  end

  settings.index_status[index] = :valid
  
  { index: index, document_types: types }.to_json
end


# Currently supports ES methods that can be given a single value, e.g., match, term, prefix, fuzzy, etc.
# i.e., any method that can be written: { "query": { "METHOD" : { "field": "value" } } }
# * not supported yet: everything else, e.g., value, range, boost...
# Currently combined using { "bool": { "must": { ... } } } 
# * to do: range queries
# * to do: sort
def construct_es_query
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

  # determines batch size for indexing documents (SPARQL OFFSET)
  set :batch_size, (configuration["batch_size"] || 100)

  set :type_paths, Hash[
        configuration["types"].collect do |type_def|
          [type_def["on_path"], type_def["type"]]
        end
      ]

  set :type_definitions, Hash[
        configuration["types"].collect do |type_def|
          [type_def["type"], type_def]
        end
      ]

  type_defs = {}
  rights = {}
  configuration["types"].each do |type_def|
    type = type_def["type"]
    rights[type] = load_access_rights type
  end

  set :indexes, rights

  set :index_status, {}

  # properties lookup table for deltas
  rdf_properties = {}

  configuration["types"].each do |type_def|
    if type_def["composite_types"]
    # this needs to be done looping on composite_types first
    # to pick up implicit properties
    #
      # type_def["properties"].each do |name, property|
      #   if property["mappings"]
      #     property["mappings"].each do |source_type, source_property|
      #       rdf_property = settings.type_definitions[source_type]["properties"][source_property]
      #       rdf_properties[rdf_property] =  rdf_properties[rdf_property] || []
      #       rdf_properties[rdf_property].push type_def["type"]
      #     end
      #   end
    else
      type_def["properties"].each do |name, rdf_property|
        rdf_properties[rdf_property] =  rdf_properties[rdf_property] || []
        rdf_properties[rdf_property].push type_def["type"]
      end
    end
  end

  set :rdf_properties, rdf_properties

end


get "/:path/index" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  index = current_index path

  unless index
    index = create_current_index client, path 
  end

  index_documents client, path, index
end


get "/:path/search" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type path

  index = current_index path

  # wait if being updated

  if index and settings.index_status[index] == :invalid
    index_documents client, path, index
    client.refresh_index index
  end

  unless index 
    index = create_current_index client, path
    index_documents client, path, index
    client.refresh_index index
  end

  if params["page"]
    page = params["page"]["number"] or 0
    size = params["page"]["size"] or 10
  else
    page = 0
    size = 10
  end

  es_query = construct_es_query

  count_result = JSON.parse(client.count index: index, query: es_query)
  count = count_result["count"]

  # add pagination parameters
  es_query["from"] = page * size
  es_query["size"] = size

  results = client.search index: index, query: es_query

  format_results(type, count, page, size, results).to_json
end


# Using raw ES search DSL, mostly for testing
# Need to think through several things, such as pagination
post "/:path/search" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type path

  index = current_index path

  if index and settings.index_status[index] == :invalid
    index_documents client, path, index
    client.refresh_index index
  end

  unless index 
    index = create_current_index client, path
    index_documents client, path, index
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


post "/update" do
  deltas = @json_body
  inserts = deltas["delta"]["inserts"]
  deletes = deltas["delta"]["deletes"]

  inserts.each do |triple|
    s = triple["s"]
    p = triple["p"]
    possible_types = settings.rdf_properties[p]

    possible_types.each do |type|
      rdf_type = settings.type_definitions[type]["rdf_type"]

      if query "ASK WHERE { <#{s}> a <#{rdf_type}> }"
        indexes = settings.indexes[type]
        log.info "Invalidating #{indexes.length} indexes of document type: #{type}"
        # if automatic_updates flag, then update/remove specified document
        # else
        indexes.each { |key, index| settings.index_status[index[:index]] = :invalid }
      end
    end
  end

  {message: "Thanks for the update."}.to_json
end
