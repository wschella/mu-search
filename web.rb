require 'net/http'

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


# test
get "/test" do
  content_type 'application/json'

  client = Elastic.new(host: 'elasticsearch', port: 9200)

  # client.create_index 'user', { title: { type: "text" }, name: { type: "text" } }
  # client.put_document('user', 1, { name: "John Doe" }).body
  # client.get_document('people', 1).body
  client.search index: 'peopl*,user', query: { query: { match: { name: "John" } } }
end


# index documents
get "/index" do
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  query_result = query <<SPARQL
    SELECT ?id ?title ?desc WHERE {
      ?doc <http://mu.semte.ch/vocabularies/core/uuid> ?id; 
           <http://mu.semte.ch/vocabularies/core/title> ?title; 
           <http://mu.semte.ch/vocabularies/core/description> ?desc
    }
SPARQL

  query_result.each do |result|
    id = result[:id].to_s

    groups_query_result = query <<SPARQL
      SELECT ?gid WHERE {
        ?doc <http://mu.semte.ch/vocabularies/core/uuid> "#{result.id.to_s}";
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
    client.put_document('document', id, document.to_json)
  end


  "all ok"
end



# search
get "/search" do
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  # test query
  query_result = query <<SPARQL
    SELECT ?id WHERE {
      ?doc <http://mu.semte.ch/vocabularies/core/uuid> ?id
    } LIMIT 1
SPARQL

  search_query = 
    {
      query: {
        bool: {
          must: [
            {
              match: {
                title: "document"
              }
            },
            {
              terms_set: {
                groups: {
                  terms: [
                    "g02"
                  ],
                  minimum_should_match_field: "required_matches"
                }
              }
            }
          ]
        }
      }
    } 

  results = client.search index: 'document', query: search_query 
  # log.info JSON.parse(results)
  # JSON.parse(results)["hits"]["hits"]
  results
  
end
