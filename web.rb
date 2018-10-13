require 'net/http'

# fake configuration
configure do
  set :properties, { 
        "title" => "<http://mu.semte.ch/vocabularies/core/title>",
        "description" => "<http://mu.semte.ch/vocabularies/core/description>" 
      }
end


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

  def create_index(index, mappings = nil)
    uri = URI("http://#{@host}:#{@port_s}/#{index}")
    req = Net::HTTP::Put.new(uri)
    if mappings
      req.body = { mappings: { _doc: { properties: mappings } } }.to_json
    end

    run(uri, req)
  end

  def get_document(index, id)
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{id}")
    req = Net::HTTP::Get.new(uri)
    run(uri, req)
  end

  def put_document(index, id, document)
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{id}")
    req = Net::HTTP::Put.new(uri)
    req.body = document
    run(uri, req)
  end

  def update_document(index, id, document)
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{id}/_update")
    req = Net::HTTP::Post.new(uri)
    req.body = document
    run(uri, req)
  end

  def delete_document(index, id)
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{id}")
    req = Net::HTTP::Delete.new(uri)
    run(uri, req)
  end

  def search(index:, query_string: nil, query: nil, sort: nil)
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


def make_property_query uuid
  select_variables = ""
  property_predicates = ""

  settings.properties.each do |key, predicate|
    select_variables += " ?#{key} " 
    property_predicates += "; #{predicate} ?#{key} "
  end

  <<SPARQL
    SELECT #{select_variables} WHERE { 
     ?doc  <http://mu.semte.ch/vocabularies/core/uuid> "#{uuid}" #{property_predicates}
    }
SPARQL
end


# indexes a single document in Elasticsearch
# properties need to be made configurable
def index_document client, uuid
  query_result = query make_property_query uuid
  result = query_result.first

  groups_query_result = query <<SPARQL
      SELECT ?gid WHERE {
        ?doc <http://mu.semte.ch/vocabularies/core/uuid> "#{uuid}";
             <http://mu.semte.ch/vocabularies/authorization/inGroup> ?group.
        ?group <http://mu.semte.ch/vocabularies/core/uuid> ?gid
      }
SPARQL

  groups = []
  groups_query_result.each do |group|
    groups.push(group[:gid].to_s)
  end

  document = {
    title: result[:title].to_s,
    description: result[:description].to_s,
    groups: groups,
    required_matches: 1
  }
  log.info document
  client.put_document('document', uuid, document.to_json)
end

# indexes all documents (or all authorized documents,
# if queries are routed through an authorization service)
# * needs pagination
get "/index" do
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  query_result = query <<SPARQL
    SELECT ?id WHERE {
      ?doc a <http://mu.semte.ch/vocabularies/core/Document>;
           <http://mu.semte.ch/vocabularies/core/uuid> ?id
    }
SPARQL

  query_result.each do |result|
    index_document client, result[:id].to_s 
  end

  { message: "all ok" }.to_json
end


get "/index/:uuid" do |uuid|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  index_document client, uuid
  
  { message: "all ok" }.to_json
end


def run_authorized_search query
  allowed_groups_s = request.env["HTTP_MU_AUTH_ALLOWED_GROUPS"]
  allowed_groups = allowed_groups_s ? JSON.parse(allowed_groups_s).map { |e| e["value"] } : nil

  used_groups_s = request.env["HTTP_MU_AUTH_USED_GROUPS"]
  used_groups = used_groups_s ? JSON.parse(used_groups_s).map { |e| e["value"] } : nil

  search_query = 
    {
      query: {
        bool: {
          must: [
            query,
            { terms_set: { groups: { terms: allowed_groups, minimum_should_match_field: "required_matches" } } }
          ]
        }
      }
    } 

  results = client.search index: 'document', query: search_query 
  results
end


post "/search" do
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  run_authorized_search @json_body["query"]
end


# * title is hard-coded
get "/search" do
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  run_authorized_search( { match: { title: params["q"] } } )
end


