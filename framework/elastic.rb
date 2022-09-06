# A quick as-needed Elastic API, for use until
# the conflict with Sinatra::Utils is resolved
# * does not follow the standard API
# * see: https://github.com/mu-semtech/mu-ruby-template/issues/16
class Elastic
  # Sets up the ElasticSearch instance
  def initialize(host: "localhost", port: 9200, logger:)
    @host = host
    @port = port
    @port_s = port.to_s
    @logger = logger
  end

  # Checks whether or not ElasticSearch is up
  #
  # Executes a health check and accepts either "green" or "yellow".
  def up?
    uri = URI("http://#{@host}:#{@port_s}/_cluster/health")
    req = Net::HTTP::Get.new(uri)
    begin
      resp = run(uri, req)
      if resp.is_a? Net::HTTPSuccess
        health = JSON.parse resp.body
        health["status"] == "yellow" or health["status"] == "green"
      else
        false
      end
    rescue
      false
    end
  end

  # Checks whether or not the supplied index exists.
  #   - index: string name of the index
  #
  # Executes a HEAD request. If that succeeds we can assume the index
  # exists.
  def index_exists?(index)
    uri = URI("http://#{@host}:#{@port_s}/#{index}")
    req = Net::HTTP::Head.new(uri)

    begin
      resp = run(uri, req)
      resp.is_a?(Net::HTTPSuccess) ? true : false
    rescue StandardError => e
      @logger.warn("ELASTICSEARCH") { "Failed to detect whether index #{index} exists. Assuming it doesn't." }
      @logger.warn("ELASTICSEARCH") { e.full_message }
      false
    end
  end

  # Creates an index in Elasticsearch
  #   - index: Index to be created
  #   - mappings: Optional pre-defined document mappings for the index,
  #     JSON object passed directly to Elasticsearch.
  #   - settings: Optional JSON object passed directly to Elasticsearch
  def create_index(index, mappings = nil, settings = nil)
    uri = URI("http://#{@host}:#{@port_s}/#{index}")
    req = Net::HTTP::Put.new(uri)
    req_body = {
      mappings: mappings,
      settings: settings
    }.to_json
    req.body = req_body
    resp = run(uri, req)

    if resp.is_a? Net::HTTPSuccess
      @logger.debug("ELASTICSEARCH") { "Successfully created index #{index}" }
      resp
    else
      @logger.error("ELASTICSEARCH") { "Failed to create index #{index}.\nPUT #{uri}\nRequest: #{req_body}\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
      if resp.is_a? Net::HTTPClientError # 4xx status code
        raise ArgumentError, "Failed to create index #{index}: #{req_body}"
      else
        raise "Failed to create index #{index}"
      end
    end
  end

  # Deletes an index from ElasticSearch
  #   - index: Name of the index to be removed
  #
  # Returns true when the index existed and is succesfully deleted.
  # Otherwise false.
  # Throws an error if the index exists but fails to be deleted.
  def delete_index(index)
    uri = URI("http://#{@host}:#{@port_s}/#{index}")
    req = Net::HTTP::Delete.new(uri)
    resp = run(uri, req)

    if resp.is_a? Net::HTTPNotFound
      @logger.debug("ELASTICSEARCH") { "Index #{index} doesn't exist and cannot be deleted." }
      false
    elsif resp.is_a? Net::HTTPSuccess
      @logger.debug("ELASTICSEARCH") { "Successfully deleted index #{index}" }
      true
    else
      @logger.error("ELASTICSEARCH") { "Failed to delete index #{index}.\nDELETE #{uri}\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
      raise "Failed to delete index #{index}"
    end
  end

  # Refreshes an ElasticSearch index, making documents available for
  # search.
  #   - index: Name of the index which will be refreshed.
  #
  # Returns whether the refresh succeeded
  #
  # When we store documents in ElasticSearch, they are not necessarily
  # available immediately. It requires a refresh of the index. This
  # operation happens once every second. When we build an index to
  # query it immediately, we should ensure to refresh the index before
  # querying.
  def refresh_index(index)
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_refresh")
    req = Net::HTTP::Post.new(uri)
    resp = run(uri, req)

    if resp.is_a? Net::HTTPSuccess
      @logger.debug("ELASTICSEARCH") { "Successfully refreshed index #{index}" }
      true
    else
      @logger.warn("ELASTICSEARCH") { "Failed to refresh index #{index}.\nPOST #{uri}\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
      false
    end
  end

  # Clear a given index by deleting all documents in the Elasticsearch index
  #   - index: Index name to clear
  # Note: this operation does not delete the index in Elasticsearch
  def clear_index(index)
    if index_exists? index
      resp = delete_documents_by_query index, { query: { match_all: {} } }
      if resp.is_a? Net::HTTPSuccess
        @logger.debug("ELASTICSEARCH") { "Successfully cleared index #{index}" }
        resp
      else
        @logger.error("ELASTICSEARCH") { "Failed to clear index #{index}.\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
        raise "Failed to clear index #{index}"
      end
    end
  end

  # Gets a single document from an index by its ElasticSearch id.
  # Returns nil if the document cannot be found.
  #   - index: Index to retrieve the document from
  #   - id: ElasticSearch ID of the document
  def get_document(index, id)
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{CGI::escape(id)}")
    req = Net::HTTP::Get.new(uri)
    resp = run(uri, req)
    if resp.is_a? Net::HTTPNotFound
      @logger.debug("ELASTICSEARCH") { "Document #{id} not found in index #{index}" }
      nil
    elsif resp.is_a? Net::HTTPSuccess
      JSON.parse resp.body
    else
      @logger.error("ELASTICSEARCH") { "Failed to get document #{id} from index #{index}.\nGET #{uri}\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
      raise "Failed to get document #{id} from index #{index}"
    end
  end

  # Inserts a new document in an Elasticsearch index
  #   - index: Index to store the document in.
  #   - id: Elasticsearch identifier to store the document under.
  #   - document: document contents to index (as a ruby json object)
  # Returns the inserted document
  # Raises an error on failure.
  def insert_document(index, id, document)
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{CGI::escape(id)}")
    req = Net::HTTP::Put.new(uri)
    req_body = document.to_json
    req.body = req_body
    resp = run(uri, req)

    if resp.is_a? Net::HTTPSuccess
      @logger.debug("ELASTICSEARCH") { "Inserted document #{id} in index #{index}" }
      JSON.parse resp.body
    else
      @logger.error("ELASTICSEARCH") { "Failed to insert document #{id} in index #{index}.\nPUT #{uri}\nRequest: #{req_body}\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
      raise "Failed to insert document #{id} in index #{index}"
    end
  end

  # Partially updates an existing document in Elasticsearch index
  #   - index: Index to update the document in
  #   - id: ElasticSearch identifier of the document
  #   - document: New document contents
  # Returns the updated document or nil if the document cannot be found.
  # Otherwise, raises an error.
  def update_document(index, id, document)
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{CGI::escape(id)}/_update")
    req = Net::HTTP::Post.new(uri)
    req_body = { "doc": document }.to_json
    req.body = req_body
    resp = run(uri, req)

    if resp.is_a? Net::HTTPNotFound
      @logger.info("ELASTICSEARCH") { "Cannot update document #{id} in index #{index} because it doesn't exist" }
      nil
    elsif resp.is_a? Net::HTTPSuccess
      @logger.debug("ELASTICSEARCH") { "Updated document #{id} in index #{index}" }
      JSON.parse resp.body
    else
      @logger.error("ELASTICSEARCH") { "Failed to update document #{id} in index #{index}.\nPOST #{uri}\nRequest: #{req_body}\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
      raise "Failed to update document #{id} in index #{index}"
    end
  end

  # Updates the document with the given id in the given index.
  # Inserts the document if it doesn't exist yet
  # - index: index to store document in
  # - id: elastic identifier to store the document under
  # - document: document contents (as a ruby json object)
  def upsert_document(index, id, document)
    @logger.debug("ELASTICSEARCH") { "Trying to update document with id #{id}" }
    updated_document = update_document index, id, document
    if updated_document.nil?
      @logger.debug("ELASTICSEARCH") { "Document #{id} does not exist yet, trying to insert new document" }
      insert_document index, id, document
    else
      updated_document
    end
  end

  # Deletes a document from an Elasticsearch index
  #   - index: Index to remove the document from
  #   - id: ElasticSearch identifier of the document
  # Returns true when the document existed and is succesfully deleted.
  # Otherwise false.
  # Throws an error if the document exists but fails to be deleted.
  def delete_document(index, id)
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/#{CGI::escape(id)}")
    req = Net::HTTP::Delete.new(uri)
    resp = run(uri, req)

    if resp.is_a? Net::HTTPNotFound
      @logger.debug("ELASTICSEARCH") { "Document #{id} doesn't exist in index #{index} and cannot be deleted." }
      false
    elsif resp.is_a? Net::HTTPSuccess
      @logger.debug("ELASTICSEARCH") { "Successfully deleted document #{id} in index #{index}" }
      true
    else
      @logger.error("ELASTICSEARCH") { "Failed to delete document #{id} in index #{index}.\nDELETE #{uri}\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
      raise "Failed to delete document #{id} in index #{index}"
    end
  end

  # Searches for documents in the given indexes
  #   - indexes: Array of indexes to be searched
  #   - query: Elasticsearch query JSON object in ruby format
  def search_documents(indexes:, query: nil)
    indexes_s = indexes.join(',')
    uri = URI("http://#{@host}:#{@port_s}/#{indexes_s}/_search")
    req_body = query.to_json
    @logger.debug("SEARCH") { "Searching Elasticsearch index(es) #{indexes_s} with body #{req_body}" }
    req = Net::HTTP::Post.new(uri)
    req.body = req_body
    resp = run(uri, req)

    if resp.is_a? Net::HTTPSuccess
      JSON.parse resp.body
    else
      @logger.error("SEARCH") { "Searching documents in index(es) #{indexes_s} failed.\nPOST #{uri}\nRequest: #{req_body}\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
      if resp.is_a? Net::HTTPClientError # 4xx status code
        raise ArgumentError, "Invalid search query #{req_body}"
      else
        raise "Something went wrong while searching documents in index(es) #{indexes_s}"
      end
    end
  end

  # Counts search results for documents in the given indexex
  #   - indexes: Array of indexes to be searched
  #   - query: Elasticsearch query JSON object in ruby format
  def count_documents(indexes:, query: nil)
    indexes_s = indexes.join(',')
    uri = URI("http://#{@host}:#{@port_s}/#{indexes_s}/_doc/_count")
    req_body = query.to_json
    @logger.debug("SEARCH") { "Count search results in index(es) #{indexes_s} for body #{req_body}" }
    req = Net::HTTP::Get.new(uri)
    req.body = req_body
    resp = run(uri, req)

    if resp.is_a? Net::HTTPSuccess
      data = JSON.parse resp.body
      data["count"]
    else
      @logger.error("SEARCH") { "Counting search results in index(es) #{indexes_s} failed.\nPOST #{uri}\nRequest: #{req_body}\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
      if resp.is_a? Net::HTTPClientError # 4xx status code
        raise ArgumentError, "Invalid search query #{req_body}"
      else
        raise "Something went wrong while counting search results in index(es) #{indexes_s}"
      end
    end
  end

  private

  # Sends a raw request to Elasticsearch
  #   - uri: URI instance representing the elasticSearch host
  #   - req: The request object
  #   - retries: Max number of retries
  #
  # Returns the HTTP response.
  #
  # Note: the method only logs limited info on purpose.
  # Additional logging about the error that occurred
  # is the responsibility of the consumer.
  def run(uri, req, retries = 6)
    def run_rescue(uri, req, retries, result = nil)
      if retries == 0
        if result.is_a? Exception
          @logger.warn("ELASTICSEARCH") { "Failed to run request #{uri}. Max number of retries reached." }
          raise result
        else
          @logger.info("ELASTICSEARCH") { "Failed to run request #{uri}. Max number of retries reached." }
          result
        end
      else
        @logger.info("ELASTICSEARCH") { "Failed to run request #{uri}. Request will be retried (#{retries} left)." }
        next_retries = retries - 1
        backoff = (6 - next_retries)**2
        sleep backoff
        run(uri, req, next_retries)
      end
    end

    req["content-type"] = "application/json"

    res = Net::HTTP.start(uri.hostname, uri.port) do |http|
      http.read_timeout = ENV["ELASTIC_READ_TIMEOUT"].to_i
      begin
        http.request(req)
      rescue Exception => e
        run_rescue(uri, req, retries, e)
      end
    end

    case res
    when Net::HTTPSuccess, Net::HTTPRedirection # response code 2xx or 3xx
      # Ruby doesn't use the encoding specified in HTTP headers (https://bugs.ruby-lang.org/issues/2567#note-3)
      content_type = res["CONTENT-TYPE"]
      if res.body && content_type && content_type.downcase.include?("charset=utf-8")
        res.body.force_encoding("utf-8")
      end
      res
    when Net::HTTPTooManyRequests
      run_rescue(uri, req, retries, res)
    else
      @logger.info("ELASTICSEARCH") { "Failed to run request #{uri}" }
      @logger.debug("ELASTICSEARCH") { "Request body (trimmed): #{req.body.to_s[0...1024]}" }
      @logger.debug("ELASTICSEARCH") { "Response: #{res.inspect}" }
      res
    end
  end

  # Deletes all documents which match a certain query
  #   - index: Index to delete the documents from
  #   - query: ElasticSearch query used for selecting documents
  #   - conflicts_proceed: boolean indicating whether to proceed deletion if
  #     other operations are currently occurring on the same document or not.
  #
  # For the query formal, see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html
  def delete_documents_by_query(index, query, conflicts_proceed: true)
    conflicts = conflicts_proceed ? 'conflicts=proceed' : ''
    uri = URI("http://#{@host}:#{@port_s}/#{index}/_doc/_delete_by_query?#{conflicts}")
    req = Net::HTTP::Post.new(uri)
    req.body = query.to_json
    run(uri, req)
  end
end
