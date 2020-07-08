module MuSearch
  class IndexBuilder

    def initialize(elastic_client:, number_of_threads:, logger:, index_definitions:, index_id:, allowed_groups:  )
      @logger = logger
      @elastic_client = elastic_client
      @number_of_threads = number_of_threads
      @batch_size = batch_size
      @max_batches = max_batches
      sparql_options = { headers: { 'mu-auth-allowed-groups': allowed_groups_object.to_json } }
      @sparql_connection_pool = ConnectionPool.new(size: number_of_threads, timeout: 3) { ::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'], sparql_options) }
      @tika_client = Tika.new(host: 'localhost', port: 9998)
      @index_definitions = index_definitions
      @index_id = index_id

    end

    def build
      @index_definitions.each do |type_def|
        log.info "Building index of type #{@type_def["type"]}"
        rdf_type = type_def["rdf_type"]
        number_of_document = count_documents(rdf_type)
        log.info "Found #{count} documents of type #{rdf_type} to index"

        batches =
          if @max_batches and @max_batches != 0
            [@max_batches, count/@number_of_batches].min
          else
            count/@batch_size
          end
        log.info "Number of batches: #{batches}"
        Parallel.each (0..batches, in_threads: @number_of_threads) do |i|
          batch_start_time = Time.now
          log.info "Indexing batch #{i} of #{batches}"
          offset = i*settings.batch_size
          @sparql_connection_pool.with do |sparql_client|
            q = " SELECT DISTINCT ?doc ?id WHERE { ?doc a <#{rdf_type}>.  } LIMIT #{settings.batch_size} OFFSET #{offset} "
            log.debug "selecting documents for batch #{i}"
            query_result = client.query(q)
            log.debug "Discovered identifiers for this batch: #{query_result}"
            query_result.each do |result|
              log.debug "Fetching document for #{document_id}"
              document = fetch_document_to_index(tika_client: @tika_client,
                                                 elastic_client: @elastic_client,
                                                 sparql_client: sparql_client
                                                 uri: document_id,
                                                 properties: type_def["properties"])
              log.debug "Uploading document #{document_id} - batch #{i} - allowed groups #{allowed_groups}"
              begin
                @elastic_client.bulk_update_document({ index: { _id: document_id } }, document)
              rescue StandardError => e
                log.warn e
                log.warn "Failed to ingest batch for id #{document_id}"
              end
            rescue StandardError => e
              log.warn e
              log.warn "Failed to fetch document or upload it or somesuch.  ID #{document_id} error #{e.inspect}"
            end
          end
          log.info "Processed batch in #{(Time.now - batch_start_time).round} seconds"
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
        log.info "Found #{documents_count} documents for #{allowed_groups}."
        documents_count
      end
    end

    # Retrieves a document to index from the available parameters.  Is
    # capable of coping with uri identification schemes, with
    # properties as configured in the user's config, as well as with
    # optional allowed_groups.
    #
    # This is your one-stop shop to fetch all info to index a document.
    #
    #   - uri: URI of the item to fetch
    #   - properties: Array of properties as configured in the user's
    #     configuration file.
    #   - allowed_groups: Optional setting allowing to scope down the
    #     retrieved contents by specific access rights.
    #   - attachment_path_base: base path to use for files
    def fetch_document_to_index tika_client: , sparql_client: , uri: nil, properties: nil, attachment_path_base: '.'
      # we include uuid because it may be used for folding
      unless properties.has_key?("uuid")
        properties["uuid"] = ["http://mu.semte.ch/vocabularies/core/uuid"]
      end
      key_value_tuples = properties.collect do |key, val|
        query = make_property_query(uri, key, val)
        results = sparql_client.query(query)

        if val.is_a? Hash
          # file attachment
          if val["attachment_pipeline"]
            key, value = parse_attachment(tika_client, results, key, attachment_path_base)
            [key, value]
          # nested object
          elsif val["rdf_type"]
            parse_nested_object(results, key, val['properties'])
          end
        else
          values = results.collect do |result|
            case result[key]
            when RDF::Literal::Integer
              result[key].to_i
            when RDF::Literal::Double
              result[key].to_f
            when RDF::Literal::Decimal
              result[key].to_f
            when RDF::Literal::Boolean
              result[key].to_s.downcase == 'true'
            when RDF::Literal::Time
              result[key].to_s
            when RDF::Literal::Date
              result[key].to_s
            when RDF::Literal::DateTime
              result[key].to_s
            when RDF::Literal
              result[key].to_s
            else
              result[key].to_s
            end
          end
          [key, denumerate(values)]
        end
      end

      document = Hash[key_value_tuples]
      return document
    end

  end
end
