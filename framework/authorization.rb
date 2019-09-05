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
    index = @indexes[type] && @indexes[type][allowed_groups]
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


  # Yields the mutex for the supplied index
  def mutex index
    @mutexes[index]
  end

  # Creates a new mutex for the supplied index
  #
  # TODO: discover where this is used and documents its intended use.
  def new_mutex index
    @mutexes[index] = Mutex.new
  end
end


# Removes an index from Elasticsearch
#
#   - client: ElasticSearch client
#   - index: Index name to be removed
#
# TODO: Why does this not use client.delete_index
#
# TODO: Should we move these methods in a separate module so they're
# easy to find and disambiguate?  They seem to be high-level.
def clear_index client, index
  if client.index_exists index
    client.delete_by_query(index, { query: { match_all: {} } }, true)
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
  @indexes.map do |type, indexes|
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


# Invalides indexes of the supplied subject for the given type.
#
# TODO: describe this method further after seeing where it is being
# used.  I don't understand what the goal of this method is as I don't
# see how the type filtering helps.
def invalidate_indexes s, type
  @indexes[type].each do |key, index|
    allowed_groups = index[:allowed_groups]
    rdf_type = settings.type_definitions[type]["rdf_type"]
    if is_authorized s, rdf_type, allowed_groups
      Indexes.instance.mutex(index[:index]).synchronize do
        Indexes.instance.set_status index[:index], :invalid
      end
    else
      log.info "Not Authorized, nothing doing."
    end
  end
end

# Loads all indexes for the supplied types
#
# Searches the triplestore for all indexes of these types and loads
# the metadata into mu-search so they can be used later.
#
#   - types: Type definitions which will be loaded
#
# TODO: describe where these types come from.  I'm a bit lost as they
# aren't strings (this probably makes sense, I just don't know where
# they come from yet).
def load_persisted_indexes types
  types.each do |type_def|
    type = type_def["type"]
    Indexes.instance.indexes[type] = load_indexes type
  end
end

# Destroys all persisted indexes
#
# Queries the database to find all currently persisted indexes and
# removes each of them from ElasticSearch and from the triplestore.
#
# NOTE this method does not try to remove indexes from the Indexes
# singleton.  It is inteded to be called during setup only.
def destroy_persisted_indexes client
  get_persisted_index_names().each do |result|
    index_name = result['index_name']
    if client.index_exists index_name
      log.info "Deleting persisted index: #{index_name}"
      client.delete_index index_name
    end

    remove_persisted_index index_name
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


# Searches the triplestore for names of persisted indexes
#
# The name of a persisted index is the name used in ElasticSearch.
def get_persisted_index_names
  direct_query <<SPARQL
SELECT ?index_name WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
               <http://mu.semte.ch/vocabularies/authorization/indexName> ?index_name
    }
  }
SPARQL
end


# Gets the request indexes for a particular type and fetches the
# allowed_groups from the request.
#
# In case of additive indexes, one index is returned per allowed
# group.  Otherwise an array of a single group is returned.
#
# In case an index could not be found, a falsey value is returned.
#
#
# TODO: further abstract this function.  The current API is strange as
# it may not yield a full set of indexes.  Working with the indexes
# then requires calling get_request_groups again to see what needs to
# be created.  We should have a single higher-level abstraction to
# work with and/or revamp this function so it's more solid.
#
# Also used in config, calling find_matching_index directly
# Note that used_groups are currently NOT used in lookup...
# maybe a confusion in the specs?
def get_request_indexes type
  allowed_groups = get_allowed_groups
  used_groups = []

  log.debug "GET_REQUEST_INDEXES allowed_groups #{allowed_groups}"
  log.debug "GET_REQUEST_INDEXES used_groups #{used_groups}"

  if settings.additive_indexes
    log.debug "GET_REQUEST_INDEXES assumes additive indexes"
    indexes = allowed_groups.map do |group|
      Indexes.instance.find_matching_index type, [group], used_groups
    end
    indexes.select { |x| x }
  else
    log.debug "GET_REQUEST_INDEXES assumes non-additive indexes"
    index = Indexes.instance.find_matching_index(type, allowed_groups, used_groups)
    if index then [index] else [] end
  end
end


def get_request_index_names type
  indexes = get_request_indexes type
  indexes ? indexes.map { |index| index[:index] } : []    
end


def get_matching_index_name type, allowed_groups, used_groups
  log.debug "Searching matching indexes for type #{type} with allowed groups #{allowed_groups}"
  Indexes.instance.find_matching_index(type, allowed_groups, used_groups)
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

# Sorts an array of groups
#
#  - groups: Array to sort
#
# TODO: The variables array is already a sorted array.  Sorting their
# values may yield strange results.  It is likely better not to sort
# them. -- Really? Note that groups are used for lookup of indexes.
# Sorting guaranties uniqueness.
#
# TODO: cope with dashes in the variables.  Perhaps we should escape
# these characters in the variables so we don't have conflicts.
def sort_groups groups
  groups.sort_by { |g| g["name"] + "-" + g["variables"].sort.join("-") }
end

# Returns the allowed_groups from the request
def get_allowed_groups
  allowed_groups_s = request.env["HTTP_MU_AUTH_ALLOWED_GROUPS"]
  allowed_groups =
    allowed_groups_s ? JSON.parse(allowed_groups_s) : []

  return sort_groups(allowed_groups)
end

# Ensures indexes exist for the current request in the TripleStore in
# ElasticSearch and in the Indexes singleton.
#
# Fetches the allowed_groups from the request and interprets it based
# on the additive_indexes setting.
#
#  - client: ElasticSearch client used for answering the request.
#  - type: Type of documents for which the indexes need to be built.
#
# TODO: think about naming.  Perhaps something like
# ensure_indexes_for_request would be better.  Perhaps different base
# constructs would remove the need for a function like this.
def create_request_indexes client, type
  allowed_groups = get_allowed_groups
  used_groups = []

  if settings.additive_indexes
    allowed_groups.map do |group|
      create_index_full client, type, [group], used_groups
    end
  else
    [create_index_full( client, type, allowed_groups, used_groups)]
  end
end

# Ensures a specific index exists in ElasticSearch, in the
# triplestore, and in the internal Indexes singleton.
#
# If an index exists beforehand, it is not recreated.
#
#   - client: ElasticSearch client in which the index will be created
#   - type: Type of the content stored in the index
#   - allowed_groups: Set of groups to which this index applies
#
# TODO: When we talk about allowed_groups here, we likely need to query
# the triplestore for contents using the allowed_groups in the
# request, but setup the index using the used_groups returned from teh
# triplestore.  That way we have the exact information on which the
# request was built and we can reuse indexes maximally.  In case of
# additive indexes these should overlap.
#
# * TODO: check if index exists before creating it <-- DONE?
# * TODO: how to name indexes? <-- DONE?
def create_index_full client, type, allowed_groups, used_groups
  index = Digest::MD5.hexdigest (type + "-" + allowed_groups.map { |g| g.to_json }.join("-"))

  uri =  store_index type, index, allowed_groups, used_groups

  index_definition =   {
    index: index,
    uri: uri,
    allowed_groups: allowed_groups,
    used_groups: used_groups
  }

  # Although index may exist in Elasticsearch, it may have been lost in the 
  # triplestore and the Indexes singleton. Add it, just to be sure.
  Indexes.instance.add_index type, allowed_groups, used_groups, index_definition

  if client.index_exists index
    log.info "Index not created, already exists: #{index}"
    return index_definition
  else
    mappings = settings.type_definitions[type]["mappings"]
    index_settings = settings.type_definitions[type]["settings"] || settings.default_index_settings

    unless mappings
      mappings = { "properties" => {} }
    end

    mappings["properties"]["uuid"] = { type: "keyword" }

    begin
      client.create_index index, mappings, index_settings
    rescue StandardError => e
      log.warn "Error (create_index): #{e.inspect}"
      raise "Error creating index: #{index}"
    end

    index_definition
  end
end

# Creates an index and returns the index's name.
def create_index client, type, allowed_groups, used_groups
  index = create_index_full client, type, allowed_groups, used_groups
  index[:index]
end


# Retrieves the indexes for the current request in a safe manner,
# ensuring everything exists and is up-to-date when this function 
# returns.
#
#   - client: ElasticSearch instance used for storing the indexes.
#   - type: Type of documents requested
#
# TODO: I'm not entirely certain I have fully understood this
# function.  The guarantees and/or intentions should be described more
# in depth as it's used in many places.
#
# TODO: Whilst staring it this code I think it could be optimized by
# creating a function that will create the index upon request and
# update it.  Something that just guarantees a single index is an top
# shape.  This method could call that method for all indexes and be
# done with it.  Similar functionality, hopefully easier to
# understand.  This is poken without caring about the mutexes, so
# perhaps the solution here is an optimal one.
#
# WARNING: a quick bug fix has introduced some naming ambiguity
# between indexes and indexs_name(s)
def get_or_create_indexes client, type
  # I doubt this takes care of additive indexes <-- this was from
  # experimentation, this code itself seems correct...

  def sync client, type
    log.debug "GET_OR_CREATE_INDEXES: sync type #{type}"

    settings.master_mutex.synchronize do
      # yield all available indexes
      indexes = get_request_indexes type

      log.debug "GET_OR_CREATE_INDEXES: sync indexes #{indexes}"
      if indexes.all? and !indexes.empty?
        log.debug "GET_OR_CREATE_INDEXES: sync get all indexes"

        update_statuses = indexes.map do |index|
          index_name = index[:index]
          if Indexes.instance.status(index_name) == :invalid
            Indexes.instance.set_status index_name, :updating
            true
          else
            false
          end
        end

        return indexes, update_statuses
      else
        log.debug "GET_OR_CREATE_INDEXES: sync did not get all indexes"

        indexes = create_request_indexes client, type

        update_statuses = indexes.map do |index|
          if index.is_a? String # Hack! This means index already exists
            false
          else
            index_name = index[:index]
            if Indexes.instance.status(index_name) == :valid
              false
            else
              Indexes.instance.set_status index_name, :updating
              true
            end
          end
        end

        return indexes, update_statuses
      end
    end
  end

  indexes, update_statuses = sync client, type

  log.debug "GET_OR_CREATE_INDEXES Sync gave indexes #{indexes}"
  log.debug "GET_OR_CREATE_INDEXES Sync gave statuses #{update_statuses}"

  indexes.zip(update_statuses).each do |index, update_index|
    unless index.is_a? String # Hack! see above
      index_name = index[:index]
      if update_index
        Indexes.instance.mutex(index_name).synchronize do
          begin
            clear_index client, index_name
            index_documents client, type, index_name, index[:allowed_groups]
            client.refresh_index index_name
            Indexes.instance.set_status index_name, :valid
          rescue
            Indexes.instance.set_status index_name, :invalid
          end
        end
      end
    end
  end

  resulting_indexes = indexes.map do |index|
    if index.is_a? String # hack
      index
    else
      index[:index]
    end
  end

  log.debug "GET_OR_CREATE_INDEXES yields indexes #{resulting_indexes}"
  resulting_indexes
end


# Loads indexes from the triplestore into the Indexes singleton for
# the supplied type.
#
# This handles the full setup needed for a single type.
#
# If indexes exist in the database and in ElasticSearch, we can
# retrieve them and load them in the internal model.  From there on
# they can be used to search on or to update.
def load_indexes type
  indexes = {}

  query_result = direct_query  <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
                 <http://mu.semte.ch/vocabularies/authorization/objectType> "#{type}";
                 <http://mu.semte.ch/vocabularies/authorization/indexName> ?index_name
    }
  }
SPARQL

  query_result.each do |result|
    uri = result["index"].to_s
    allowed_groups_result = direct_query  <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> <http://mu.semte.ch/vocabularies/authorization/hasAllowedGroup> ?group
    }
  }
SPARQL
    allowed_groups = allowed_groups_result.map { |g| JSON.parse g["group"].to_s }

    used_groups_result = direct_query  <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> <http://mu.semte.ch/vocabularies/authorization/hasUsedGroup> ?group
    }
  }
SPARQL
    used_groups = used_groups_result.map { |g| JSON.parse g["group"].to_s }

    index_name = result["index_name"].to_s

    indexes[allowed_groups] = {
      uri: uri,
      index: index_name,
      allowed_groups: allowed_groups,
      used_groups: used_groups
    }

    Indexes.instance.new_mutex index_name
  end

  indexes
end
