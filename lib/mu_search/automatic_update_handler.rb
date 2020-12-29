require_relative 'update_handler'

module MuSearch
  ##
  # the automatic update handler is a service that executes updates or deletes on indexes.
  # when a document needs to be udpated the handler will fetch the required information from the triplestore
  # and insert that data into the correct index
  # when a document needs to be deleted it will verify that the document no longer exists in the triplestore
  # and if so remove it from the index
  # this handler takes the configured allowed_groups of an index into account
  class AutomaticUpdateHandler < MuSearch::UpdateHandler

    ##
    # creates an automatic update handler
    def initialize(elasticsearch:, tika:, search_configuration:, **args)
      @elasticsearch = elasticsearch
      @tika = tika
      @type_definitions = search_configuration[:type_definitions]
      @attachment_path_base = search_configuration[:attachment_path_base]
      super(search_configuration: search_configuration, **args)
    end

    ##
    # Update all documents relating to a particular uri and a series of
    # types.
    #
    #   - document_id: String URI of the entity which needs an update.
    #   - index_types: Array of index types where the document needs to be updated
    #   - update_type: Type of the update (:update or :delete)
    #
    # Note: since updates may have been queued for a while, the update type is not taken into account.
    #       The current state of the triplestore is taken as the source of truth.
    #       If the document exists and is accessible in the triplestore for a set of allowed groups,
    #            the document gets updated in the corresponding search index
    #       If the document doesn't exist (anymore) or is not accessible in the triplestore for a set of allowed groups,
    #            the document is removed from the corresponding search index
    def handler(document_id, index_types, update_type)
      index_types.each do |index_type|
        @logger.debug("UPDATE HANDLER") { "Updating document <#{document_id}> in indexes for type '#{index_type}'" }
        indexes = @index_manager.indexes[index_type]
        indexes.each do |_, index|
          authorized_client = MuSearch::SPARQL.authorized_client index.allowed_groups
          rdf_type = @type_definitions[index_type]["rdf_type"]
          if document_exists_for? authorized_client, document_id, rdf_type
            @logger.debug("UPDATE HANDLER") { "Document <#{document_id}> needs to be updated in index #{index.name} for '#{index_type}' and allowed groups #{index.allowed_groups}" }
            document_builder = DocumentBuilder.new(
              tika: @tika,
              sparql_client: authorized_client,
              attachment_path_base: @attachment_path_base,
              logger: @logger)
            properties = @type_definitions[index_type]["properties"]
            document = document_builder.fetch_document_to_index(uri: document_id, properties: properties)
            @elasticsearch.upsert_document index.name, document_id, document
          else
            @logger.debug("UPDATE HANDLER") { "Document <#{document_id}> not accessible or already removed in triplestore for allowed groups #{index.allowed_groups}. Removing document from Elasticsearch index #{index.name} as well." }
            begin
              @elasticsearch.delete_document index.name, document_id
            rescue
              # TODO check error and log warning if needed
              @logger.debug("UPDATE HANDLER") { "Failed to delete document #{document_id} from index #{index.name}" }
            end
          end
        end
      end
    end

    private

    def document_exists_for?(authorized_client, document_id, rdf_type)
      authorized_client.query("ASK { #{sparql_escape_uri(document_id)} a #{sparql_escape_uri(rdf_type)} . }")
    end
  end
end
