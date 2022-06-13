module MuSearch
  ###
  # The IndexManager keeps track of indexes and their state in:
  # - an in-memory cache @indexes, grouped per type
  # - Elasticsearch, using index.name as identifier
  # - triplestore
  ###
  class IndexManager

    attr_reader :indexes
    def initialize(logger:, elasticsearch:, tika:, sparql_connection_pool:, search_configuration:)
      @logger = logger
      @elasticsearch = elasticsearch
      @tika = tika
      @sparql_connection_pool = sparql_connection_pool
      @master_mutex = Mutex.new
      @configuration = search_configuration
      @indexes = {} # indexes per type

      initialize_indexes
    end

    # Fetches an array of indexes for the given type and allowed/used groups
    # Ensures all indexes exists and are up-to-date when the function returns
    # If no type is passed, indexes for all types are invalidated
    # If no allowed_groups are passed, all indexes are invalidated regardless of access rights
    #   - type_name: type to find index for
    #   - allowed_groups: allowed groups to find index for (array of {group, variables}-objects)
    #   - force_update: whether the index needs to be updated only when it's marked as invalid or not
    #
    # In case of additive indexes, returns one index per allowed group
    # Otherwise, returns an array of a single index
    # Returns an empty array if no index is found
    def fetch_indexes type_name, allowed_groups, force_update: false
      indexes_to_update = []
      type_names = type_name.nil? ? @indexes.keys : [type_name]

      @master_mutex.synchronize do
        type_names.each do |type_name|
          if allowed_groups
            if @configuration[:additive_indexes]
              additive_indexes = []
              allowed_groups.each do |allowed_group|
                index = get_matching_index type_name, [allowed_group]
                additive_indexes << index unless index.nil?
              end
              indexes_to_update += additive_indexes
              @logger.debug("INDEX MGMT") do
                index_names_s = additive_indexes.map { |index| index.name }.join(", ")
                "Fetched #{additive_indexes.length} additive indexes for type '#{type_name}' and allowed_groups #{allowed_groups}: #{index_names_s}"
              end
            else
              index = get_matching_index type_name, allowed_groups
              unless index.nil?
                indexes_to_update << index
                @logger.debug("INDEX MGMT") { "Fetched index for type '#{type_name}' and allowed_groups #{allowed_groups}: #{index.name}" }
              end
            end
          elsif @indexes[type_name] # fetch all indexes, regardless of access rights
            @indexes[type_name].each do |_, index|
              @logger.debug("INDEX MGMT") { "Fetched index for type '#{type_name}' and allowed_groups #{index.allowed_groups}: #{index.name}" }
              indexes_to_update << index
            end
          end
        end

        indexes_to_update.each do |index|
          index.status = :invalid if force_update
          update_index index
        end

        if indexes_to_update.any? { |index| index.status == :invalid }
          @logger.warn("INDEX MGMT") { "Not all indexes are up-to-date. Search results may be incomplete." }
        end
      end

      indexes_to_update
    end

    # Invalidate the indexes for the given type and allowed groups
    # If no type is passed, indexes for all types are invalidated
    # If no allowed_groups are passed, all indexes are invalidated regardless of access rights
    # - type_name: name of the index type to invalidate all indexes for
    # - allowed_groups: allowed groups to invalidate indexes for (array of {group, variables}-objects)
    #
    # Returns the list of indexes that are invalidated
    #
    # TODO correctly handle composite indexes
    def invalidate_indexes type_name, allowed_groups
      indexes_to_invalidate = []
      type_names = type_name.nil? ? @indexes.keys : [type_name]

      @master_mutex.synchronize do
        type_names.each do |type_name|
          if allowed_groups
            if @configuration[:additive_indexes]
              allowed_groups.each do |allowed_group|
                index = find_matching_index type_name, [allowed_group]
                indexes_to_invalidate << index unless index.nil?
              end
            else
              index = find_matching_index type_name, allowed_groups
              indexes_to_invalidate << index unless index.nil?
            end
          elsif @indexes[type_name] # invalidate all indexes, regardless of access rights
            @indexes[type_name].each do |_, index|
              indexes_to_invalidate << index
            end
          end
        end

        @logger.info("INDEX MGMT") do
          type_s = type_name.nil? ? "all types" : "type '#{type_name}'"
          allowed_groups_s = allowed_groups.nil? ? "all groups" : "allowed_groups #{allowed_groups}"
          index_names_s = indexes_to_invalidate.map { |index| index.name }.join(", ")
          "Found #{indexes_to_invalidate.length} indexes to invalidate for #{type_s} and #{allowed_groups_s}: #{index_names_s}"
        end

        indexes_to_invalidate.each do |index|
          @logger.debug("INDEX MGMT") { "Mark index #{index.name} as invalid" }
          index.mutex.synchronize { index.status = :invalid }
        end
      end

      indexes_to_invalidate
    end


    # Remove the indexes for the given type and allowed groups
    # If no type is passed, indexes for all types are removed
    # If no allowed_groups are passed, all indexes are removed regardless of access rights
    # - type_name: name of the index type to remove all indexes for
    # - allowed_groups: allowed groups to remove indexes for (array of {group, variables}-objects)
    #
    # Returns the list of indexes that are removed
    #
    # TODO correctly handle composite indexes
    def remove_indexes type_name, allowed_groups
      indexes_to_remove = []
      type_names = type_name.nil? ? @indexes.keys : [type_name]

      @master_mutex.synchronize do
        type_names.each do |type_name|
          if allowed_groups
            if @configuration[:additive_indexes]
              allowed_groups.each do |allowed_group|
                index = find_matching_index type_name, [allowed_group]
                indexes_to_remove << index unless index.nil?
              end
            else
              index = find_matching_index type_name, allowed_groups
              indexes_to_remove << index unless index.nil?
            end
          elsif @indexes[type_name] # remove all indexes, regardless of access rights
            @indexes[type_name].each do |_, index|
              indexes_to_remove << index
            end
          end
        end

        @logger.info("INDEX MGMT") do
          type_s = type_name.nil? ? "all types" : "type '#{type_name}'"
          allowed_groups_s = allowed_groups.nil? ? "all groups" : "allowed_groups #{allowed_groups}"
          index_names_s = indexes_to_remove.map { |index| index.name }.join(", ")
          "Found #{indexes_to_remove.length} indexes to remove for #{type_s} and #{allowed_groups_s}: #{index_names_s}"
        end

        indexes_to_remove.each do |index|
          @logger.debug("INDEX MGMT") { "Remove index #{index.name}" }
          index.mutex.synchronize do
            remove_index index allowed_groups
            index.status = :deleted
          end
        end
      end

      indexes_to_remove
    end


    private

    # Initialize indexes based on the search configuration
    # Ensures all configured eager indexes exist
    # and removes indexes found in the triplestore if index peristentce is disabled
    def initialize_indexes
      if @configuration[:persist_indexes]
        @logger.info("INDEX MGMT") { "Loading persisted indexes from the triplestore" }
        @configuration[:type_definitions].keys.each do |type_name|
          @indexes[type_name] = get_indexes_from_triplestore_by_type type_name
        end
      else
        @logger.info("INDEX MGMT") { "Removing indexes as they're configured not to be persisted. Set the 'persist_indexes' flag to 'true' to enable index persistence (recommended in production environment)." }
        remove_persisted_indexes
      end

      @logger.info("INDEX MGMT") { "Start initializing all configured eager indexing groups..." }
      @master_mutex.synchronize do
        total = @configuration[:eager_indexing_groups].length * @configuration[:type_definitions].keys.length
        count = 0
        @configuration[:eager_indexing_groups].each do |allowed_groups|
          @configuration[:type_definitions].keys.each do |type_name|
            count = count + 1
            unless @configuration[:persist_indexes]
              @logger.info("INDEX MGMT") { "Removing eager index for type '#{type_name}' and allowed_groups #{allowed_groups} since indexes are configured not to be persisted." }
              remove_index type_name, allowed_groups
            end
            index = ensure_index type_name, allowed_groups
            @logger.info("INDEX MGMT") { "(#{count}/#{total}) Eager index #{index.name} created for type '#{index.type_name}' and allowed_groups #{allowed_groups}. Current status: #{index.status}." }
            if index.status == :invalid
              @logger.info("INDEX MGMT") { "Eager index #{index.name} not up-to-date. Start reindexing documents." }
              index_documents index
              index.status = :valid
            end
          end
        end
        @logger.info("INDEX MGMT") { "Completed initialization of #{total} eager indexes" }
      end
    end

    # Get a single matching index for the given type and allowed groups.
    # Create a new one if none is found in the cache.
    #   - type_name: type to find index for
    #   - allowed_groups: allowed groups to find index for (array of {group, variables}-objects)
    def get_matching_index type_name, allowed_groups
      index = find_matching_index type_name, allowed_groups
      if index
        @logger.debug("INDEX MGMT") { "Found matching index in cache for type '#{type_name}' and allowed_groups #{allowed_groups}" }
      else
        @logger.info("INDEX MGMT") { "Didn't find matching index for type '#{type_name}' and allowed_groups #{allowed_groups} in cache. Going to fetch index from triplestore or create it if it doesn't exist yet. Configure eager indexes to avoid building indexes at runtime." }
        index = ensure_index type_name, allowed_groups
      end
      index
    end

    # Find a single matching index for the given type and allowed/used groups
    #   - type_name: type to find index for
    #   - allowed_groups: allowed groups to find index for (array of {group, variables}-objects)
    #   - used_groups: used groups to find index for (array of {group, variables}-objects)
    # Returns nil if no index is found
    #
    # TODO take used_groups into account when they are supported by mu-authorization
    def find_matching_index type_name, allowed_groups, used_groups = []
      @logger.debug("INDEX MGMT") { "Trying to find matching index in cache for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}" }
      group_key = serialize_authorization_groups allowed_groups
      index = @indexes.dig(type_name, group_key)
      index
    end

    # Ensure index exists in the triplestore, in Elasticsearch and
    # in the in-memory indexes cache of the IndexManager
    #
    # Returns the index with status :valid or :invalid depending
    # whether the index already exists in Elasticsearch
    def ensure_index type_name, allowed_groups, used_groups = []
      sorted_allowed_groups = sort_authorization_groups allowed_groups
      sorted_used_groups = sort_authorization_groups used_groups
      index_name = generate_index_name type_name, sorted_allowed_groups, sorted_used_groups

      # Ensure index exists in triplestore
      index_uri = find_index_in_triplestore_by_name index_name
      unless index_uri
        @logger.debug("INDEX MGMT") { "Create index #{index_name} in triplestore for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}" }
        index_uri = create_index_in_triplestore type_name, index_name, sorted_allowed_groups, sorted_used_groups
      end

      # Ensure index exists in the IndexManager
      index = find_matching_index type_name, allowed_groups, used_groups
      unless index
        @logger.debug("INDEX MGMT") { "Add index #{index_name} to cache for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}" }
        index = MuSearch::SearchIndex.new(
          uri: index_uri,
          name: index_name,
          type_name: type_name,
          allowed_groups: sorted_allowed_groups,
          used_groups: sorted_used_groups)
        @indexes[type_name] = {} unless @indexes.has_key? type_name
        group_key = serialize_authorization_groups sorted_allowed_groups
        @indexes[type_name][group_key] = index
      end

      # Ensure index exists in Elasticsearch
      unless @elasticsearch.index_exists? index_name
        @logger.debug("INDEX MGMT") { "Creating index #{index_name} in Elasticsearch for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}" }
        index.status = :invalid
        type_definition = @configuration[:type_definitions][type_name]
        if type_definition
          mappings = type_definition["mappings"] || {}
          mappings["properties"] = {} if mappings["properties"].nil?
          # uuid must be configured as keyword to be able to collapse results
          mappings["properties"]["uuid"] = { type: "keyword" }
          mappings["properties"]["uri"] = { type: "keyword" }
          # TODO deep merge custom and default settings
          settings = type_definition["settings"] || @configuration[:default_index_settings] || {}
          @elasticsearch.create_index index_name, mappings, settings
        else
          raise "No type definition found in search config for type '#{type_name}'. Unable to create Elasticsearch index."
        end
      end
      index
    end

    # Updates an existing index if it's current state is invalid
    # I.e. clear all documents in the Elasticsearch index
    # and index the documents again.
    # The Elasticsearch index is never completely removed.
    #   - index: SearchIndex to update
    # Returns the index.
    def update_index index
      if index.status == :invalid
        index.mutex.synchronize do
          @logger.info("INDEX MGMT") { "Updating index #{index.name}" }
          index.status = :updating
          begin
            @elasticsearch.clear_index index.name
            index_documents index
            @elasticsearch.refresh_index index.name
            index.status = :valid
            @logger.info("INDEX MGMT") { "Index #{index.name} is up-to-date" }
          rescue StandardError => e
            index.status = :invalid
            @logger.error("INDEX MGMT") { "Failed to update index #{index.name}." }
            @logger.error("INDEX MGMT") { e.full_message }
          end
        end
      end
      index
    end

    # Indexes documents in the given SearchIndex.
    # I.e. index documents for a specific type in the given Elasticsearch index
    # taking the authorization groups into account. Documents are indexed in batches.
    #   - index: SearchIndex to index documents in
    def index_documents index
      search_configuration = @configuration.select do |key|
        [:number_of_threads, :batch_size, :max_batches,
         :attachment_path_base, :type_definitions].include? key
      end
      builder = MuSearch::IndexBuilder.new(
        logger: @logger,
        elasticsearch: @elasticsearch,
        tika: @tika,
        sparql_connection_pool: @sparql_connection_pool,
        search_index: index,
        search_configuration: search_configuration)
      builder.build
    end

    # Removes the index for the given type_name and allowed/used groups
    # from the triplestore, Elasticsearch and
    # the in-memory indexes cache of the IndexManager.
    # Does not yield an error if index doesn't exist
    def remove_index type_name, allowed_groups, used_groups = []
      sorted_allowed_groups = sort_authorization_groups allowed_groups
      sorted_used_groups = sort_authorization_groups used_groups
      index_name = generate_index_name type_name, sorted_allowed_groups, sorted_used_groups

      # Remove index from IndexManager
      if @indexes.has_key? type_name
        @indexes[type_name].delete_if { |_, value| value.name == index_name }

        # Remove index from triplestore and Elasticsearch
        remove_index_by_name index_name
      end
    end

    # Removes the index from the triplestore and Elasticsearch
    # Does not yield an error if index doesn't exist
    def remove_index_by_name index_name
      @logger.debug("INDEX MGMT") { "Removing index #{index_name} from triplestore" }
      remove_index_from_triplestore index_name

      if @elasticsearch.index_exists? index_name
        @logger.debug("INDEX MGMT") { "Removing index #{index_name} from Elasticsearch" }
        @elasticsearch.delete_index index_name
      end
    end

    # Removes all persisted indexes from the triplestore as well as from Elasticsearch
    #
    # NOTE this method does not check the current search configuration.
    #      It only removes indexes found in the triplestore and removes those.
    def remove_persisted_indexes
      result = @sparql_connection_pool.sudo_query <<SPARQL
SELECT ?name WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
               <http://mu.semte.ch/vocabularies/authorization/indexName> ?name
    }
  }
SPARQL
      index_names = result.map { |r| r.name }
      index_names.each do |index_name|
        remove_index_by_name index_name
        @logger.info("INDEX MGMT") { "Remove persisted index #{index_name} in triplestore and Elasticsearch" }
      end
    end

    # Stores a new index in the triplestore
    #
    #   - type_name: Type of the objects stored in the index
    #   - index_name: Unique name of the index (also used as id in Elasticsearch)
    #   - allowed_groups: allowed groups of the index (array of {group, variables}-objects)
    #   - used_groups: used groups of the index (array of {group, variables}-objects)
    #
    # TODO cleanup internal model used for storing indexes in triplestore
    def create_index_in_triplestore type_name, index_name, allowed_groups, used_groups
      uuid = generate_uuid()
      uri = "http://mu.semte.ch/authorization/elasticsearch/indexes/#{uuid}"  # TODO update base URI

      def groups_term groups
        groups.map { |g| sparql_escape_string g.to_json }.join(",")
      end

      allowed_group_statement = allowed_groups.empty? ? "" : "search:hasAllowedGroup #{groups_term(allowed_groups)} ; "
      used_group_statement = used_groups.empty? ? "" : "search:hasUsedGroup #{groups_term(used_groups)} ; "

      query_result = @sparql_connection_pool.sudo_update <<SPARQL
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX search: <http://mu.semte.ch/vocabularies/authorization/>
  INSERT DATA {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> a search:ElasticsearchIndex ;
               mu:uuid "#{uuid}" ;
               search:objectType "#{type_name}" ;
               #{allowed_group_statement}
               #{used_group_statement}
               search:indexName "#{index_name}" .
    }
  }
SPARQL
      uri
    end

    # Removes the index with given name from the triplestore
    #
    #   - index_name: name of the index to remove
    def remove_index_from_triplestore index_name
      @sparql_connection_pool.sudo_update <<SPARQL
DELETE {
  GRAPH <http://mu.semte.ch/authorization> {
    ?s ?p ?o .
  }
}
WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?s a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex> ;
           <http://mu.semte.ch/vocabularies/authorization/indexName> #{sparql_escape_string index_name} ;
           ?p ?o .
    }
}
SPARQL
    end

    # Find index by name in the triplestore
    # Returns nil if none is found
    def find_index_in_triplestore_by_name index_name
      result = @sparql_connection_pool.sudo_query <<SPARQL
SELECT ?index WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex> ;
               <http://mu.semte.ch/vocabularies/authorization/indexName> #{sparql_escape_string index_name} .
    }
  } LIMIT 1
SPARQL
      result.map { |r| r.index }.first
    end

    # Gets indexes for the given type name from the triplestore
    #
    # - type_name: name of the index type as configured in the search config
    #
    # Note: there may be multiple indexes for one type.
    #       One per (combination of) allowed groups
    def get_indexes_from_triplestore_by_type type_name
      indexes = {}

      query_result = @sparql_connection_pool.sudo_query  <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex> ;
                 <http://mu.semte.ch/vocabularies/authorization/objectType> "#{type_name}" ;
                 <http://mu.semte.ch/vocabularies/authorization/indexName> ?index_name .
    }
  }
SPARQL

      query_result.each do |result|
        uri = result["index"].to_s
        index_name = result["index_name"].to_s

        allowed_groups_result = @sparql_connection_pool.sudo_query  <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> <http://mu.semte.ch/vocabularies/authorization/hasAllowedGroup> ?group
    }
  }
SPARQL
        allowed_groups = allowed_groups_result.map { |g| JSON.parse g["group"].to_s }

        used_groups_result = @sparql_connection_pool.sudo_query  <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> <http://mu.semte.ch/vocabularies/authorization/hasUsedGroup> ?group
    }
  }
SPARQL
        used_groups = used_groups_result.map { |g| JSON.parse g["group"].to_s }

        group_key = serialize_authorization_groups allowed_groups

        indexes[group_key] = MuSearch::SearchIndex.new(
          uri: uri,
          name: index_name,
          type_name: type_name,
          allowed_groups: allowed_groups,
          used_groups: used_groups)
      end

      indexes
    end

    # Generate a unique name for an index based on the given type and allowed/used groups
    def generate_index_name type_name, sorted_allowed_groups, sorted_used_groups
      groups = sorted_allowed_groups.map do |group|
        # order keys of each group object alphabetically to ensure unique json serialization
        Hash[ group.sort_by { |key, _| key } ].to_json
      end
      Digest::MD5.hexdigest (type_name + "-" + groups.join("-"))
    end

  end
end
