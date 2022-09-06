require 'parallel'
require 'concurrent'

module MuSearch
  class IndexBuilder

    def initialize(logger:, elasticsearch:, tika:, sparql_connection_pool:, search_index:, search_configuration:)
      @logger = logger
      @elasticsearch = elasticsearch
      @tika = tika
      @sparql_connection_pool = sparql_connection_pool
      @search_index = search_index

      @configuration = search_configuration
      @number_of_threads = search_configuration[:number_of_threads]
      @batch_size = search_configuration[:batch_size]
      @max_batches = search_configuration[:max_batches]
      @attachment_path_base = search_configuration[:attachment_path_base]

      type_def = @configuration[:type_definitions][search_index.type_name]
      if type_def["composite_types"] && type_def["composite_types"].length
        @index_definitions = expand_composite_type_definition type_def
      else
        @index_definitions = [type_def]
      end
    end

    # Index the documents for the configured type definition in batches.
    #
    # The properties are queried from the triplestore using the SPARQL connection pool
    # which is configured with the appropriate mu-auth-allowed-groups.
    #
    # If a document fails to index, a warning will be logged, but the indexing continues.
    # The other documents in the batch will still be indexed.
    def build
      # Note: @index_definitions will only contain multiple elements in case of a composite type.
      @index_definitions.each do |type_def|
        @logger.info("INDEXING") { "Building index of type #{type_def["type"]}" }
        rdf_type = type_def["rdf_type"]
        number_of_documents = count_documents(rdf_type)
        @logger.info("INDEXING") { "Found #{number_of_documents} documents to index of type #{rdf_type} with allowed groups #{@search_index.allowed_groups}" }
        batches =
          if @max_batches && (@max_batches != 0)
            [@max_batches, number_of_documents/@batch_size].min
          else
            number_of_documents/@batch_size
          end
        batches = batches + 1
        @logger.info("INDEXING") { "Number of batches: #{batches}" }

        Parallel.each(1..batches, in_threads: @number_of_threads) do |i|
          batch_start_time = Time.now
          @logger.info("INDEXING") { "Indexing batch #{i}/#{batches}" }
          failed_documents = []

          @sparql_connection_pool.with_authorization(@search_index.allowed_groups) do |sparql_client|
            document_builder = MuSearch::DocumentBuilder.new(
              tika: @tika,
              sparql_client: sparql_client,
              attachment_path_base: @attachment_path_base,
              logger: @logger
            )
            document_uris = get_documents_for_batch rdf_type, i
            document_uris.each do |document_uri|
              @logger.debug("INDEXING") { "Indexing document #{document_uri} in batch #{i}" }
              document = document_builder.fetch_document_to_index(
                uri: document_uri,
                properties: type_def["properties"])
              @elasticsearch.insert_document @search_index.name, document_uri, document
            rescue StandardError => e
              failed_documents << document_uri
              @logger.warn("INDEXING") { "Failed to index document #{document_uri} in batch #{i}" }
              @logger.warn { e.full_message }
            end
          end
          @logger.info("INDEXING") { "Processed batch #{i}/#{batches} in #{(Time.now - batch_start_time).round} seconds." }
          if failed_documents.length > 0
            @logger.warn("INDEXING") { "#{failed_documents.length} documents failed to index in batch #{i}." }
            @logger.debug("INDEXING") { "Failed documents: #{failed_documents}" }
          end
        end
      end
    end

    private

    # Expands the type definition of a composite type.
    # Returns an array of type definitions, one per simple type that constitutes the composite type
    # with the properties resolved based on the properties mapping of the composite type.
    #
    # See the README for an example of a composite type configuration.
    def expand_composite_type_definition(composite_type_def)
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
      @sparql_connection_pool.with_authorization(@search_index.allowed_groups) do |client|
        result = client.query("SELECT (COUNT(?doc) as ?count) WHERE { ?doc a #{SinatraTemplate::Utils.sparql_escape_uri(rdf_type)} }")
        documents_count = result.first["count"].to_i
        documents_count
      end
    end

    def get_documents_for_batch(rdf_type, batch_i)
      offset = (batch_i - 1) * @batch_size
      @sparql_connection_pool.with_authorization(@search_index.allowed_groups) do |client|
        result = client.query("SELECT DISTINCT ?doc WHERE { ?doc a <#{rdf_type}>.  } LIMIT #{@batch_size} OFFSET #{offset}")
        document_uris = result.map { |r| r[:doc].to_s }
        @logger.debug("INDEXING") { "Selected documents for batch #{batch_i}: #{document_uris}" }
        document_uris
      end
    end
  end
end
