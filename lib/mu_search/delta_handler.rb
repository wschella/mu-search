require 'set'

module MuSearch
  ##
  # the delta handler is a service that parses deltas and triggers
  # the necessary updates via the (index) update handler.
  # Assumes that it is safe to remove objects for which the type was removed
  # updates documents for deltas that match the configured property paths
  # NOTE: in theory the handler has a pretty good idea what has changed
  #       it may be possible to have finer grained updates on es documents than we currently have
  class DeltaHandler

    ##
    # creates a delta handler
    #
    # raises an error if an invalid search config is provided
    def initialize(logger:, sparql_connection_pool:, search_configuration:, update_handler: )
      @logger = logger
      @sparql_connection_pool = sparql_connection_pool
      type_definitions = search_configuration[:type_definitions]
      @type_to_config_map = map_type_to_config(type_definitions)
      @property_to_config_map = map_property_to_config(type_definitions)
      @update_handler = update_handler
    end

    ##
    # Parses the given delta and triggers the update of affected documents
    # Assumes delta format v0.0.1
    # TODO add support for other delta formats
    def handle_deltas(deltas)
      @logger.info("DELTA") { "Received delta update #{deltas}" }
      if deltas.is_a?(Array)
        @logger.debug("DELTA") { "Delta contains #{deltas.length} changesets" }
        deltas.each do |changeset|
          changeset["inserts"].uniq.each do |triple|
            @logger.debug("DELTA") { "Handling inserted triple #{triple.inspect}" }
            search_configs = applicable_index_configurations_for_triple triple
            type_names = search_configs.map { |config| "'#{config[:type_name]}'" }
            @logger.debug("DELTA") { "Triple affects all search indexes for #{type_names.join(', ')}" }
            search_configs.each do |config|
              subjects = find_root_subjects_for_triple(triple, config)
              if subjects.length
                type_name = config[:type_name]
                @logger.info("DELTA") { "Found #{subjects.length} subjects for '#{type_name}' that needs to be updated." }
                subjects.each { |subject| @update_handler.add_update(subject, type_name) }
              end
            end
          end

          changeset["deletes"].uniq.each do |triple|
            @logger.debug("DELTA") { "Handling deleted triple #{triple.inspect}" }
            # for deletes the delete of a type triggers the delete of the document,
            # all other changes are considered an update
            search_configs = applicable_index_configurations_for_triple triple
            search_configs.each do |config|
              if triple["predicate"]["value"] == RDF.type.to_s
                type_name = config[:type_name]
                subject = triple["subject"]["value"]
                @logger.info("DELTA") { "#{subject} will be removed from indexes for '#{type_name}'." }
                @update_handler.add_delete(subject, type_name)
              else
                subjects = find_root_subjects_for_triple(triple, config, false)
                if subjects.length
                  type_name = config[:type_name]
                  @logger.info("DELTA") { "Found #{subjects.length} subjects for '#{type_name}' that need to be updated." }
                  subjects.each { |subject| @update_handler.add_update(subject, type_name) }
                end
              end
            end
          end
        end
      else
        @logger.error("DELTA") { "Received delta does not seem to be in v0.0.1 format. Mu-search currently only supports delta format v0.0.1 " }
      end
    end

    private

    ##
    # Find index configs that are impacted by the given triple,
    # i.e. the object is an rdf:Class that is configured as search index
    #      or the predicate is included in one of the property (paths) of a search index.
    # Returns a set of impacted search configs.
    # Each config contains keys :type_name, :rdf_type, :rdf_properties
    def applicable_index_configurations_for_triple(triple)
      predicate = triple["predicate"][ "value"]
      if predicate == RDF.type.to_s
        rdf_type = triple["object"][ "value"]
        @type_to_config_map[rdf_type]
      else
        @property_to_config_map[predicate] + @property_to_config_map["^#{predicate}"]
      end
    end

    ##
    # Finds the root subjects related to the given triple for a given search config
    # - triple: changed triple received in delta message
    # - config: search config for a type affected by the changed triple
    # - is_addition: whether the triple is inserted or deleted
    #
    # Returns an array of subject URIs as strings.
    # Returns an empty array if no subjects are found.
    def find_root_subjects_for_triple(triple, config, is_addition = true)
      # NOTE: current logic assumes rdf:type is never part of the property path
      if triple["predicate"]["value"] == RDF.type.to_s
        [ triple["subject"]["value"] ]
      else
        find_subjects_for_property(triple, config, is_addition)
      end
    end

    ##
    # Finds the subjects related to the given triple
    # with the configured rdf_type via the configured property path
    # - triple: changed triple received in delta message
    # - config: search config for a type affected by the changed triple
    # - is_addition: whether the triple is inserted or deleted
    #
    # Returns an array of subject URIs as strings.
    # Returns an empty array if no subjects are found.
    # TODO: this needs some form of cache
    def find_subjects_for_property(triple, config, is_addition)
      predicate = triple["predicate"]["value"]
      object_type = triple["object"]["type"]
      subjects = []
      nb_of_hops = config[:rdf_properties].length
      config[:rdf_properties].each_with_index do |property, i|
        if [predicate, "^#{predicate}"].include?(property)
          is_inverse = property.start_with? "^"
          if i < nb_of_hops - 1 and !is_inverse and object_type != "uri"
            # we are not at the end of the path and the object is a literal
            @logger.debug("DELTA") { "Discarding path because object is not a URI, but #{object_type}" }
          else
            subjects.concat( query_for_subjects_to_triple(triple, config, i, is_inverse, is_addition) )
          end
        end
      end
      subjects
    end

    ##
    # Queries the triplestore to find subjects related to the given triple
    # with the configured rdf_type via the configured property path
    # - triple: changed triple received in delta message
    # - config: search config for a type affected by the changed triple
    # - i: index number of the triple predicate in the property path of the search config
    # - is_inverse: whether the triple's predicate is included as inverse predicate in the property path of the search config
    # - is_addition: whether the triple is inserted or deleted
    #
    # Returns an array of subject URIs as strings.
    # Returns an empty array if no subjects are found.
    def query_for_subjects_to_triple(triple, config, i, is_inverse, is_addition)
      rdf_type = config[:rdf_type]
      rdf_properties = config[:rdf_properties]
      property_path_to_target = rdf_properties.take(i) # path from start to the triple, excluding the triple itself
      property_path_from_target = rdf_properties.slice(i + 1, rdf_properties.length - i) # path from the triple until the end

      subject_value = triple["subject"]["value"]
      predicate_value = triple["predicate"]["value"]
      object_value = triple["object"]["value"]
      object_type = triple["object"]["type"]

      # escaping values for useage in the SPARQL query
      path_to_target_term = MuSearch::SPARQL::make_predicate_string(property_path_to_target)
      path_from_target_term = MuSearch::SPARQL::make_predicate_string(property_path_from_target)
      object_term = object_type == "uri" ? sparql_escape_uri(object_value) : %(""#{object_value.sparql_escape}"")

      # based on the direction of the predicate, determine the target to which the property_path leads
      target_subject_term = is_inverse ? sparql_escape_uri(object_value) : sparql_escape_uri(subject_value)
      target_object_term = is_inverse ? sparql_escape_uri(subject_value) : object_term

      sparql_query = "SELECT DISTINCT ?s WHERE {\n"
      sparql_query += "\t #{sparql_escape_uri(subject_value)} #{sparql_escape_uri(predicate_value)} #{object_term} . \n" if is_addition
      sparql_query += "\t ?s a #{sparql_escape_uri(rdf_type)} . \n"
      # Check path from start to the triple
      if property_path_to_target.length == 0
        # triple is at the root. We only need to check if it has the correct rdf_type
        sparql_query += "\t VALUES ?s { #{target_subject_term} } . \n"
      else
        sparql_query += "\t ?s #{path_to_target_term} #{target_subject_term} . \n"
      end
      # Check path from the triple to the end
      if is_addition && property_path_from_target.length > 0
        sparql_query += "\t #{target_object_term} #{path_from_target_term} ?foo. \n"
      end
      sparql_query += "}"

      @sparql_connection_pool.sudo_query(sparql_query).map { |result| result["s"].to_s }
    end

    ##
    # parses the search configuration and returns a map that links rdf types to their related indexes
    # TODO add correct handling for composite_types
    # TOOD add correct handling for nested_types
    def map_type_to_config(type_definitions)
      type_map = Hash.new{ |hash, key| hash[key] = Set.new } # has a set as default value for each key
      type_definitions.reject{ |type, config| config.has_key?("composite_types")}.each do |type, config|
        rdf_type = config["rdf_type"]
        type_map[rdf_type] << { type_name: type, rdf_type: rdf_type, rdf_properties: [ RDF.type.to_s ] }
      end
      type_map
    end

    ##
    # parses the search configuration and returns a map that links rdf predicates to their related indexes
    # TODO add correct handling for composite_types
    # TOOD add correct handling for nested_types
    def map_property_to_config(type_definitions)
      property_map = Hash.new{ |hash, key| hash[key] = Set.new } # has a set as default value for each key
      type_definitions.reject{ |type, config| config.has_key?("composite_types")}.each do |type, config|
        config["properties"].each do |key, value|
          value = value["via"] if value.kind_of?(Hash) and !value["via"].nil?
          if value.kind_of?(Array)
            value.each do |property|
              property_map[property] << { type_name: type, rdf_type: config["rdf_type"], rdf_properties: value }
            end
          else
            property_map[value] << { type_name: type, rdf_type: config["rdf_type"], rdf_properties: [ value ] }
          end
        end
      end
      property_map
    end
  end
end
