module MuSearch
  class IndexBuilder

    def initialize(elastic_client:, number_of_threads:, logger:, batch_size:, max_batches:,  index_definitions:, index_id:, allowed_groups:, attachment_path_base: )
      @logger = logger
      @elastic_client = elastic_client
      @number_of_threads = number_of_threads
      @batch_size = batch_size
      @allowed_groups = allowed_groups
      @attachment_path_base = attachment_path_base
      if allowed_groups && allowed_groups.length > 0
        allowed_groups_object = allowed_groups.select { |group| group }
        sparql_options = { headers: { 'mu-auth-allowed-groups': allowed_groups_object.to_json } }
        @sparql_connection_pool = ConnectionPool.new(size: number_of_threads, timeout: 3) { ::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'], sparql_options) }
      else
        # assumes we're building the index for a request from a logged in user
        @sparql_connection_pool = ConnectionPool.new(size: number_of_threads, timeout: 3) {  SinatraTemplate::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT']) }
      end
      @index_definitions = index_definitions
      @index_id = index_id
      log.info "Index builder initialized"
    end

    def build
      @index_definitions.each do |type_def|
        log.info "Building index of type #{type_def["type"]}"
        rdf_type = type_def["rdf_type"]
        number_of_documents = count_documents(rdf_type)
        log.info "Found #{number_of_documents} documents to index of type #{rdf_type} with allowed groups #{@allowed_groups}"

        batches =
          if @max_batches and @max_batches != 0
            [@max_batches, number_of_documents/@batch_size].min
          else
            number_of_documents/@batch_size
          end
        batches = batches + 1
        log.info "Number of batches: #{batches}"
        Parallel.each( 1..batches, in_threads: @number_of_threads ) do |i|
          batch_start_time = Time.now
          log.info "Indexing batch #{i} of #{batches}"
          offset = ( i - 1 )*@batch_size
          @sparql_connection_pool.with do |sparql_client|
            document_builder = MuSearch::DocumentBuilder.new(
              sparql_client: sparql_client,
              attachment_path_base: @attachment_path_base,
              logger: @logger
            )
            q = " SELECT DISTINCT ?doc WHERE { ?doc a <#{rdf_type}>.  } LIMIT #{@batch_size} OFFSET #{offset} "
            log.debug "selecting documents for batch #{i}"
            query_result = sparql_client.query(q)
            log.debug "Discovered identifiers for this batch: #{query_result}"
            query_result.each do |result|
              document_id = result[:doc].to_s
              log.debug "Fetching document for #{document_id}"
              document, attachment_pipeline = document_builder.fetch_document_to_index(
                                                 uri: document_id,
                                                 properties: type_def["properties"])
              log.debug "Uploading document #{document_id} - batch #{i} - allowed groups #{@allowed_groups}"
              if attachment_pipeline
                @elastic_client.upload_attachment(@index_id, document_id, attachment_pipeline, document)
              else
                @elastic_client.put_document(@index_id, document_id, document)
              end
            rescue StandardError => e
              log.warn e
              log.warn "Failed to ingest document #{document_id}"
            end
          end
          log.info "Processed batch #{i} in #{(Time.now - batch_start_time).round} seconds"
        end
      end
    end

    private
    def log
      @logger
    end

    def count_documents(rdf_type)
      @sparql_connection_pool.with do |client|
        result = client.query("SELECT (COUNT(?doc) as ?count) WHERE { ?doc a #{SinatraTemplate::Utils.sparql_escape_uri(rdf_type)} }")
        documents_count = result.first["count"].to_i
        documents_count
      end
    end
  end
end
