require 'net/http'

# sample configuration
configure do
  set :step, 100
  set :properties, { 
        "<http://mu.semte.ch/vocabularies/core/Document>" => {
          "title" => "<http://mu.semte.ch/vocabularies/core/title>",
          "description" => "<http://mu.semte.ch/vocabularies/core/description>" 
        }
      }
end

# quick and as-needed Elastic api, for use until
# the conflict with Sinatra::Utils is resolved
# https://github.com/mu-semtech/mu-ruby-template/issues/16
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


def make_property_query uuid, property_map
  select_variables = ""
  property_predicates = ""

  property_map.each do |key, predicate|
    select_variables += " ?#{key} " 
    property_predicates += "; #{predicate} ?#{key} "
  end

  <<SPARQL
    SELECT #{select_variables} WHERE { 
     ?doc  <http://mu.semte.ch/vocabularies/core/uuid> "#{uuid}" #{property_predicates}
    }
SPARQL
end


def query_count type
  query_result = query <<SPARQL
      SELECT (COUNT(?doc) AS ?count) WHERE {
        ?doc a #{type}
      }
SPARQL

  query_result.first["count"].to_i
end


def get_all_access_rights
  query_result = query <<SPARQL
  SELECT ?allowedGroups ?usedGroups ?index WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
     ?rights a <accessRights>;
             <hasAllowedGroups> ?allowedGroups;
             <hasUsedGroups> ?usedGroups;
             <hasEsIndex> ?index
    }
  }
SPARQL
  
  query_result.map do |result|
    {
      allowed: result["allowedGroups"].to_s.split(","), 
      used: result["usedGroups"].to_s.split(","), 
      index: result["index"] 
    }
  end
end


# * make real URI and mu:uuid!
def put_access_rights index, allowed_groups, used_groups
  uri = "<#{index}>"

  query_result = query <<SPARQL
  INSERT DATA {
    GRAPH <http://mu.semte.ch/authorization> {
        #{uri} a <accessRights>;
            <hasAllowedGroups> "#{allowed_groups}";
            <hasUsedGroups> "#{used_groups}";
             <hasEsIndex> "#{index}"
    }
  }
SPARQL
end


# seems overly complicated... what am I missing?
# at this level, could probably be done directly in the triplestore
def has_same_access_rights allowed_one, used_one, allowed_two
  diff = allowed_one - allowed_two
  log.info "diffs: #{diff} // #{used_one} == #{used_one - diff} ?"
  diff == [] or (used_one - diff) == used_one
end


# seems overly complicated... what am I missing?
def find_matching_rights allowed_groups
  if allowed_groups
    get_all_access_rights.each do |ar|
      return ar[:index] if has_same_access_rights ar[:allowed], ar[:used], allowed_groups
    end

    nil
  end
end


def current_groups
  allowed_groups_s = request.env["HTTP_MU_AUTH_ALLOWED_GROUPS"]
  allowed_groups = allowed_groups_s ? JSON.parse(allowed_groups_s).map { |e| e["value"] } : nil

  used_groups_s = request.env["HTTP_MU_AUTH_USED_GROUPS"]
  used_groups = used_groups_s ? JSON.parse(used_groups_s).map { |e| e["value"] } : nil

  return allowed_groups.sort, used_groups.sort
end


def current_matching_access_rights
  allowed_groups, used_groups = current_groups
  find_matching_rights allowed_groups
end


def current_index client
  allowed_groups, used_groups = current_groups
  used_groups = used_groups or []
  allowed_groups = allowed_groups or []
  index = find_matching_rights allowed_groups
  new = nil

  unless index
    new = true
    index = allowed_groups.join("-")
    put_access_rights index, allowed_groups.join(","), used_groups.join(",")
    client.create_index index
    make_index client, index 
  end

  return index, new
end


# indexes all documents (or all authorized documents,
# if queries are routed through an authorization service)
# * should do batch updates
def make_index client, index
  count_list = [] # for reporting

  settings.properties.each do |type, property_map|
    count = query_count type
    count_list.push( { type: type, count: count } )

    (0..(count/10)).each do |i|
      offset = i*settings.step
      query_result = query <<SPARQL
    SELECT ?id WHERE {
      ?doc a #{type};
           <http://mu.semte.ch/vocabularies/core/uuid> ?id
    } LIMIT 100 OFFSET #{offset}
SPARQL

      query_result.each do |result|
        uuid = result[:id].to_s
        query_result = query make_property_query uuid, property_map
        result = query_result.first

        document = {
          title: result[:title].to_s,
          description: result[:description].to_s,
        }
        client.put_document(index, uuid, document.to_json)
      end

    end
  end

  { index: index, document_types: count_list }.to_json
end


get "/index" do
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  make_index client, current_index(client)
end


post "/search" do
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  index, new = current_index(client)
  if new
    make_index client, index
    client.refresh_index index
  end
  client.search index: index, query: @json_body
end


get "/search" do
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  index, new = current_index(client)
  log.info "Using index: #{index}"
  if new
    make_index client, index
    client.refresh_index index
  end
  client.search index: index, query: @json_body
end


