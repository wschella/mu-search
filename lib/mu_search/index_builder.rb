require 'parallel'
require 'concurrent'

module MuSearch
  class IndexBuilder

    def initialize(logger:, elasticsearch:, tika:, type_name:, index_id:, allowed_groups:, search_configuration: )
      @logger = logger
      @elasticsearch = elasticsearch
      @tika = tika
      @type_name = type_name
      @index_id = index_id
      @configuration = search_configuration
      @number_of_threads = search_configuration[:number_of_threads]
      @batch_size = search_configuration[:batch_size]
      @max_batches = search_configuration[:max_batches]
      @attachment_path_base = search_configuration[:attachment_path_base]

      if allowed_groups && allowed_groups.length > 0
        allowed_groups_object = allowed_groups.select { |group| group }
        sparql_options = { headers: { 'mu-auth-allowed-groups': allowed_groups_object.to_json } }
        @sparql_connection_pool = ConnectionPool.new(size: number_of_threads, timeout: 3) { ::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'], sparql_options) }
      else
        # assumes we're building the index for a request from a logged in user
        @sparql_connection_pool = ConnectionPool.new(size: number_of_threads, timeout: 3) {  SinatraTemplate::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT']) }
      end
      @allowed_groups = allowed_groups

      type_def = @configuration[:type_definitions][type_name]
      if type_def["composite_types"] and type_def["composite_types"].length
        @index_definitions = expand_composite_type_definition type_def
      else
        @index_definitions = [type_def]
      end
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
              tika_client: @tika,
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
              document = document_builder.fetch_document_to_index(
                                                 uri: document_id,
                                                 properties: type_def["properties"])
              log.debug "Uploading document #{document_id} - batch #{i} - allowed groups #{@allowed_groups}"
              @elasticsearch.put_document(@index_id, document_id, document)
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

    # Expands the type definition of a composite type.
    # Returns an array of type definitions, one per simple type that constitutes the composite type
    # with the properties resolved based on the properties mapping of the composite type.
    #
    # See the README for an example of a composite type configuration.
    def expand_composite_type_definition composite_type_def
      simple_types = composite_type_def["composite_types"]
      simple_types.map do |simple_type|
        simple_type_def = @configuration[:type_definitions][simple_type]
        properties = composite_type_def["properties"].map do |composite_prop|
          property_name = composite_prop["name"]
          mapped_name = composite_prop["mappings"] && composite_prop["mappings"][simple_type]
          mapped_name = composite_prop["name"] if mapped_name.nil?
          property_def = simple_type_def["properties"][mapped_name]
          [property_name, property_def]
        end
        {
          "type" => simple_type,
          "rdf_type" => simple_type_def["rdf_type"],
          "properties" => Hash[properties]
        }
      end
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
