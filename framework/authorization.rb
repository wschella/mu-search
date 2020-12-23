# Singleton collection of all indexes
#
# Provides operations on individual indexes.
class Indexes
  attr_accessor :indexes
  include Singleton
  include SinatraTemplate::Utils

  # The index_definition contains the following information:
  # - index: name of the index
  # - uri: uri of the index in the triplestore
  # - allowed_groups: Groups to which this index applies
  # - used_groups: Currently unused

  # Sets up class variables
  def initialize
    @indexes = {}
    @mutexes = {}
    @status = {}
  end

  # Yields all indexes for the supplied type.
  def get_indexes type
    return @indexes[type]
  end

  # def all_names
  #   @indexes.values.reduce([]) do |result, indexes|
  #     result + indexes.values.reduce([]) { |r, definition| l + [definition[:index]] }
  #   end
  # end

  # def all_authorized allowed_groups, used_groups
  #   @indexes.values.reduce([]) do |result, indexes|
  #     if indexes[allowed_groups]
  #       result + [indexes[allowed_groups][:index]]
  #     else
  #       result
  #     end
  #   end
  # end

  # Yields all known types for which we have indexes.
  def types
    @indexes.keys
  end

  # Sets up a new index
  #
  #   - type: Type for which the index should be created
  #   - allowed_groups: Groups for which this index will hold.  This
  #     should be an array.  The array will contain a sole element in
  #     the case of additive indexes.
  #   - index_definition: Description of the index (see above)
  def add_index type, allowed_groups, used_groups, index_definition
    indexes[type] = {} unless @indexes[type]
    @indexes[type][allowed_groups] = index_definition
    @mutexes[index_definition[:index]] = Mutex.new
  end

  # Sets the status of an index
  #
  # TODO: document allowed values for status
  def set_status index, status
    @status[index] = status
  end

  # Deletes the status of an index
  #
  # TODO: further describe why one would execute this and what the
  # effect would be.
  #
  # TODO: why does this delete the status, but not the index from the
  # indexes hash?
  def delete_status index
    @status.delete index
  end

  # Yields the status of an index
  #
  # TODO: describe possible values and their definition together with
  # Indexes#set_status
  def status index
    @status[index]
  end

  # Invalidates all indexes
  def invalidate_all
    # TODO: Is the following code more readable or not?  Remove current
    # code or commented code.
    #
    # @indexes.flat_map do |_, indexes|
    #   indexes.map do |_, index_definition|
    #     index = index_definition[:index]
    #     @status[index] = :invalid
    #     index
    #   end
    # end

    indexes_invalidated = []

    @indexes.each do |type, indexes|
      indexes.each do |groups, index_definition|
        index = index_definition[:index]
        @status[index] = :invalid
        indexes_invalidated.push index_definition[:index]
      end
    end
    indexes_invalidated
  end

  # Invalidates all indexes authorized by allowed_groups
  #
  # TODO: research use of this method and document intended use.  It
  # might work incorrectly with additive indexes.
  def invalidate_all_authorized allowed_groups, used_groups
    indexes_invalidated = []

    @indexes.each do |type, indexes|
      index_definition = indexes[allowed_groups]
      if index_definition
        index = index_definition[:index]
        @status[index] = :invalid
        indexes_invalidated.push index_definition[:index]
      end
    end
    indexes_invalidated
  end

  # Invalidates all indexes for a given type
  #
  # TODO: research use of this method and document indeded use.
  def invalidate_all_by_type type
    indexes_invalidated = []
    indexes = @indexes[type]

    if indexes
      @indexes.each do |type, indexes|
        indexes.each do |groups, index_definition|
          index = index_definition[:index]
          @status[index] = :invalid
          indexes_invalidated.push index_definition[:index]
        end
      end
    end
    indexes_invalidated
  end


  # Retrieves the index for the supplied type and allowed_groups.  A
  # single index is returned, if you intend to search for the
  # allowed_groups separately you have to send multiple requests.
  #
  #   - type: The type to search the index for
  #   - allowed_groups: allowed_groups for which the index should be
  #     retrieved.  This is an array of N elements.
  #
  # TODO: verify type should be a string
  #
  # TODO: cope with used_groups when they are supported by the Delta
  # service.  [Note that used_groups are currently NOT used in
  # lookup...  maybe a confusion in the specs?]
  def find_matching_index type, allowed_groups, used_groups
    log.debug "FIND_MATCHING_INDEX for type #{type} and allowed_groups #{allowed_groups}"
    normalized_groups = allowed_groups.sort_by{ |group| group["name"] + group["variables"].join("") }
    index = @indexes[type] && @indexes[type][normalized_groups]
    index
  end

  # Looks up an index definition by name (hash).
  def lookup_index_by_name index_name
    @indexes.each do |type, indexes|
      indexes.each do |allowed_groups, index|
        if index[:index] == index_name
          return index
        end
      end
    end
  end

end


# Fully removes an index from the system.  Includes removal from
# ElasticSearch, the Index singleton and the triplestore.
#
#   - client: ElasticSearch client
#   - index: Index which is to be removed
def destroy_index client, index, type, groups
  if client.index_exists index
    log.info "Deleting index: #{index}"
    client.delete_index index
  end

  if settings.additive_indexes
    log.debug "DESTROY_INDEX assumes additive indexes"
    groups.map do |group|
      Indexes.instance.indexes[type].delete [group]
    end
  else
    Indexes.instance.indexes[type].delete groups
  end

  Indexes.instance.delete_status index

  remove_persisted_index index
end


# Destroys all existing indexes
#
# TODO: Why does this method need to delete more stuff from the Indexes
# singleton?  I'm confused how this relates.
#
# TODO: should be inside Indexes, but uses *settings* (side-note: I
# don't see settings being used here just yet)
def destroy_existing_indexes client
  Indexes.instance.indexes.map do |type, indexes|
    indexes.map do |groups, index|
      destroy_index client, index[:index], type, groups
      index[:index]
    end
  end.flatten
end


# Fully destroys all indexes to which the current user has access.
#
#   - client: ElasticSearch client
#   - allowed_groups: Array of groups to which the current user has
#     access.
#   - used_groups: Unused
#
# TODO: why does this method consume used_groups.  I have yet to
# discover why this is relevant for this call.  Might be an oversight
# because used_groups is not functional yet.
def destroy_authorized_indexes client, allowed_groups, used_groups
  Indexes.instance.indexes.map do |type, indexes|
    index = indexes[allowed_groups]
    if index
      destroy_index client, index[:index], type, groups
      index[:index]
    end
  end
end


# Stores a newly created index in the triplestore
#
# Captures information and stores it in the triplestore.
#
#   - type: Type of the objects stored in the index
#   - index: Name of the index as used in ElasticSearch
#   - Allowed Groups for this index
#   - Used groups for this index (currently not reasoned on)
def store_index type, index, allowed_groups, used_groups
  uuid = generate_uuid()
  uri = "http://mu.semte.ch/authorization/elasticsearch/indexes/#{uuid}"

  def group_statement predicate, groups
    if groups.empty?
      ""
    else

      group_set = groups.map { |g| sparql_escape_string g.to_json }.join(",")
      " <#{predicate}> #{group_set}; "
    end
  end

  allowed_group_statement = group_statement "http://mu.semte.ch/vocabularies/authorization/hasAllowedGroup", allowed_groups
  used_group_statement = group_statement "http://mu.semte.ch/vocabularies/authorization/hasUsedGroup", used_groups

  query_result = direct_query  <<SPARQL
  INSERT DATA {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
               <http://mu.semte.ch/vocabularies/core/uuid> "#{uuid}";
               <http://mu.semte.ch/vocabularies/authorization/objectType> "#{type}";
               #{used_group_statement}
               #{allowed_group_statement}
               <http://mu.semte.ch/vocabularies/authorization/indexName> "#{index}"
    }
  }
SPARQL
end


def get_request_index_names type
  indexes = get_request_indexes type # TODO use find_matching_indexes

  indexes ? indexes.map { |index| index[:index] } : []
end


def get_matching_index type, allowed_groups, used_groups
  index = get_matching_index type, allowed_groups, used_groups
  if index then index[:index] else nil end
end


# TODO: remove commented code unless useful
#
#   direct_query <<SPARQL
# SELECT ?index_name WHERE {
#     GRAPH <http://mu.semte.ch/authorization> {
#         ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
#                <http://mu.semte.ch/vocabularies/authorization/indexName> ?index_name
#     }
#   }
# SPARQL

# Removes a persistent index from the triplestore
#
#   - index_name: string name of the index we whish to remove.
#
# TODO: use sparql_escape_string to escape the index's name.
def remove_persisted_index index_name
  direct_query <<SPARQL
DELETE {
  GRAPH <http://mu.semte.ch/authorization> {
    ?index ?p ?o
  }
} WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
               <http://mu.semte.ch/vocabularies/authorization/indexName> "#{index_name}";
                ?p ?o
    }
  }
SPARQL
end
