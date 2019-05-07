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

  def up
    uri = URI("http://#{@host}:#{@port_s}/_cluster/health")
    req = Net::HTTP::Get.new(uri)

    begin
      result = JSON.parse run(uri, req)
      result["status"] == "yellow" or
        result["status"] == "green"
    rescue
      false
    end
  end

  def index_exists index
    uri = URI("http://#{@host}:#{@port_s}/#{index}")
    req = Net::HTTP::Head.new(uri)

    begin
      run(uri, req)
      true
    rescue
      false
    end
  end

  def create_index index, mappings = nil
    uri = URI("http://#{@host}:#{@port_s}/#{index}")
    req = Net::HTTP::Put.new(uri)

    req.body = {
      settings: {
        analysis: {
          analyzer: {
            dutchanalyzer: {
              tokenizer: "standard",
              filter: ["lowercase", "dutchstemmer"] } },
          filter: {
            dutchstemmer: {
              type: "stemmer",
              name: "dutch" } } } }
    }.to_json

    result = run(uri, req)
  end

  def delete_index index
    uri = URI("http://#{@host}:#{@port_s}/#{index}")
    req = Net::HTTP::Delete.new(uri)
    begin
      run(uri, req)
      log.info "Deleted #{index}"
      log.info "Status: #{index_exists index}"
    rescue
      if !client.index_exists index
        log.info "Index not deleted, does not exist: #{index}"
      else
        raise "Error deleting index: #{index}"
      end
    end
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
    req.body = document.to_json
    run(uri, req)
  end

  def update_document index, id, document
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{id}/_update")
    req = Net::HTTP::Post.new(uri)
    req.body = { "doc": document }.to_json
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

  def delete_by_query index, query, conflicts_proceed
    conflicts = conflicts_proceed ? 'conflicts=proceed' : ''
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/_delete_by_query?#{conflicts}")

    req = Net::HTTP::Post.new(uri)
    req.body = query.to_json
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

  def upload_attachment index, id, pipeline, document
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{id}?pipeline=#{pipeline}")
    req = Net::HTTP::Put.new(uri)
    req.body = document.to_json
    run(uri, req)
  end

  def create_attachment_pipeline pipeline, field
    uri = URI("http://#{@host}:#{@port_s}/_ingest/pipeline/#{pipeline}")
    req = Net::HTTP::Put.new(uri)
    req.body = {
      description: "Extract attachment information",
      processors: [
        {
          attachment: {
            field: field,
            indexed_chars: -1
          }
        },
        {
          remove: {
            field: field
          }
        }
      ]
    }.to_json
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
