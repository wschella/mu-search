module MuSearch
  class IndexManager

    include SinatraTemplate::Utils

    attr_reader :master_mutex
    def initialize(logger:, elasticsearch:, tika:, search_configuration:)
      @logger = logger
      @elasticsearch = elasticsearch
      @tika = tika
      @master_mutex = Mutex.new
      @configuration = search_configuration
      @indexes = {} # indexes per type
    end

    # Updates and returns an array of indexes for the given type and allowed/used groups
    # Ensures all indexes exists and are up-to-date when the function returns
    #   - type_name: type to find index for
    #   - allowed_groups: allowed groups to find index for (array of {group, variables}-objects)
    #   - used_groups: used groups to find index for (array of {group, variables}-objects)
    #
    # In case of additive indexes, returns one index per allowed group
    # Otherwise, returns an array of a single group
    # Returns an empty array if no index is found
    def update_indexes_for_type_and_groups type_name, allowed_groups, used_groups
      def update_index type_name, allowed_groups, used_groups
        index = find_matching_index type_name, allowed_groups, used_groups
        if index
          log.debug "[Index mgmt] Found matching index in cache"
        else
          log.info "[Index mgmt] Didn't find matching index for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups} in cache. Going to fetch index from triplestore or create it if it doesn't exist yet."
          index = ensure_index type_name, allowed_groups, used_groups
        end
        if index.status == :invalid
          index.mutex.sychronize do
            log.info "[Index mgmt] Updating index #{index.name}"
            index.status = :updating
            begin
              @elasticsearch.clear_index index.name
              # TODO move index_documents to IndexManager
              index_documents @elasticsearch, @tika, type_name, index.name, allowed_groups
              @elasticsearch.refresh_index index.name
              index.status = :valid
            rescue
              # TODO log error
              index.status = :invalid
            end
          end
        end
        index
      end

      @master_mutex.synchronize do
        if @configuration[:additive_indexes]
          indexes = allowed_groups.map do |allowed_group|
            update_index type_name, [allowed_group], used_groups
          end
          index_names = indexes.map { |index| index.name }
          logger.debug "[Index mgmt] Fetched and updated #{indexes.length} additive indexes for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}: #{index_names.join(", ")}"
        else
          index = update_index type_name, allowed_groups, used_groups
          logger.debug "[Index mgmt] Fetched and updated index for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}: #{index.name}"
          indexes = [index]
        end
      end

      indexes
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
        log.debug "[Index mgmt] Create index #{index_name} in triplestore for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}"
        index_uri = create_index_in_triplestore type_name, index_name, sorted_allowed_groups, sorted_used_groups
      end

      # Ensure index exists in the IndexManager
      index = find_matching_index type_name, allowed_groups, used_groups
      unless index
        log.debug "[Index mgmt] Add index #{index_name} to cache for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}"
        index = ::SearchIndex.new({
                                    uri: index_uri,
                                    name: index_name,
                                    type_name: type_name,
                                    allowed_groups: sorted_allowed_groups,
                                    used_groups: sorted_used_groups
                                  })
        @indexes[type_name] = {} unless @indexes.has_key? type_name
        group_key = serialize_authorization_groups sorted_allowed_groups
        @indexes[type_name][group_key] = index
      end

      # Ensure index exists in Elasticsearch
      unless @elasticsearch.index_exists index_name
        log.debug "[Index mgmt] Creating index #{index_name} in Elasticsearch for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}"
        index.status = :invalid
        type_definition = @configuration[:types].find { |type_def| type_def.type == type_name }
        if type_definition
          mappings = type_definition["mappings"] || {}
          settings = type_definition["settings"] || @configuration[:default_index_settings] # TODO merge custom and default settings
          @elasticsearch.create_index index_name, mappings, settings
        else
          raise "No type definition found in search config for type '#{type_name}'. Unable to create Elasticsearch index."
        end
      end
      index
    end

    # Removes the index from the triplestore, Elasticsearch and
    # the in-memory indexes cache of the IndexManager.
    # Does not yield an error if index doesn't exist
    def remove_index type_name, allowed_groups, used_groups = []
      sorted_allowed_groups = sort_authorization_groups allowed_groups
      sorted_used_groups = sort_authorization_groups used_groups
      index_name = generate_index_name type_name, sorted_allowed_groups, sorted_used_groups

      # Remove index from IndexManager
      @indexes[type_name] = {}

      # Remove index from triplestore and Elasticsearch
      remove_index_by_name index_name
    end

    # Loads all indexes for the configured types
    # in the in-memory index cache of the IndexManager
    #
    # NOTE this method does not check the existance of an index in Elasticsearch
    def load_configured_indexes
      @configuration[:types].each do |type_def|
        type_name = type_def["type"]
        @indexes[type_name] = get_indexes_from_triplestore_by_type type_name
      end
    end

    # Destroys all persisted indexes from the triplestore as well as from Elasticsearch
    #
    # NOTE this method does not check the current search configuration.
    #      It only removes indexes found in the triplestore and destroys those.
    def destroy_persisted_indexes
      index_names = get_index_names_from_triplestore
      index_names.each do |index_name|
        remove_index_by_name index_name
        log.info "[Index mgmt] Destroyed index #{index_name} in triplestore and Elasticsearch"
      end
    end


    private

    def log
      @logger
    end


    # Find a single matching index for the given type and allowed/used groups
    #   - type_name: type to find index for
    #   - allowed_groups: allowed groups to find index for (array of {group, variables}-objects)
    #   - used_groups: used groups to find index for (array of {group, variables}-objects)
    # Returns nil if no index is found
    #
    # TODO take used_groups into account when they are supported by mu-authorization
    def find_matching_index type_name, allowed_groups, used_groups = []
      log.debug "[Index mgmt] Find matching index in cache for type '#{type_name}', allowed_groups #{allowed_groups} and used_groups #{used_groups}"
      group_key = serialize_authorization_groups allowed_groups
      index = @indexes[type_name] && @indexes[type_name][group_key]
      index
    end

    # Removes the index from the triplestore and Elasticsearch
    # Does not yield an error if index doesn't exist
    def remove_index_by_name
      # Remove index from triplestore
      remove_index_from_triplestore index_name
      log.debug "[Index mgmt] Removing index #{index_name} from triplestore"

      # Remove index from Elasticseach
      if @elasticsearch.index_exists index_name
        log.debug "[Index mgmt] Removing index from Elasticsearch: #{index_name}"
        @elasticsearch.delete_index index_name
      end
    end

    # Get all index names from the triplestore
    def get_index_names_from_triplestore
      result = MuSearch::SPARQL::sudo_query <<SPARQL
SELECT ?name WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
               <http://mu.semte.ch/vocabularies/authorization/indexName> ?name
    }
  }
SPARQL
      result.map { |r| r.id }
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

      def groups_term group
        groups.map { |g| sparql_escape_string g.to_json }.join(",")
      end

      allowed_group_statement = allowed_groups.empty? ? "" : "search:hasAllowedGroup #{groups_term(allowed_groups)} ; "
      used_group_statement = used_groups.empty? ? "" : "search:hasUsedGroup #{groups_term(used_groups)} ; "

      query_result = direct_query  <<SPARQL
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX search: <http://mu.semte.ch/vocabularies/authorization/>
  INSERT DATA {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> a search:ElasticsearchIndex> ;
               mu:uuid "#{uuid}" ;
               search:objectType "#{type}" ;
               #{allowed_group_statement}
               #{used_group_statement}
               search:indexName "#{index}" .
    }
  }
SPARQL
      uri
    end

    # Removes the index with given name from the triplestore
    #
    #   - index_name: name of the index to remove
    def remove_index_from_triplestore index_name
      MuSearch::SPARQL::sudo_query <<SPARQL
DELETE {
  GRAPH <http://mu.semte.ch/authorization> {
    ?s ?p ?o
  }
} WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?s a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex> ;
           <http://mu.semte.ch/vocabularies/authorization/indexName> #{sparql_escape_string index_name} ;
           ?p ?o
    }
  }
SPARQL
    end

    # Find index by name in the triplestore
    # Returns nil if none is found
    def find_index_in_triplestore_by_name index_name
      result = MuSearch::SPARQL::sudo_query <<SPARQL
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

      query_result = MuSearch::SPARQL::sudo_query  <<SPARQL
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

        allowed_groups_result = MuSearch::SPARQL::sudo_query  <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> <http://mu.semte.ch/vocabularies/authorization/hasAllowedGroup> ?group
    }
  }
SPARQL
        allowed_groups = allowed_groups_result.map { |g| JSON.parse g["group"].to_s }

        used_groups_result = MuSearch::SPARQL::sudo_query  <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> <http://mu.semte.ch/vocabularies/authorization/hasUsedGroup> ?group
    }
  }
SPARQL
        used_groups = used_groups_result.map { |g| JSON.parse g["group"].to_s }

        group_key = serialize_authorization_groups allowed_groups

        indexes[group_key] = MuSearch::SearchIndex.new({
                                                         uri: uri,
                                                         name: index_name,
                                                         type_name: type_name,
                                                         allowed_groups: allowed_groups,
                                                         used_groups: used_groups
                                                       })
      end

      indexes
    end

    def generate_index_name type_name, sorted_allowed_groups, sorted_used_groups
      # TODO does .to_json always return same serialization, independent of the order of the keys (group vs variables first)?
      Digest::MD5.hexdigest (type_name + "-" + sorted_allowed_groups.map { |g| g.to_json }.join("-"))
    end

  end
end
