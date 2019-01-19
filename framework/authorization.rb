class Indexes
  attr_accessor :indexes
  include Singleton
  def initialize
    @indexes = {}
    @mutexes = {}
    @status = {}
  end

  def get_indexes type
    @indexes[type]
  end

  def add_index type, allowed_groups, used_groups, index_definition
    indexes[type] = {} unless @indexes[type]
    @indexes[type][allowed_groups] = index_definition
    @mutexes[index_definition[:index]] = Mutex.new
  end

  def set_status index, status
    @status[index] = status
  end

  def status index
    @status[index]
  end

  def invalidate_all 
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

  def find_matching_index type, allowed_groups, used_groups
    index = @indexes[type] && @indexes[type][allowed_groups]
    index and index[:index]
  end

  def mutex index
    @mutexes[index]
  end

  def new_mutex index
    @mutexes[index] = Mutex.new
  end
end


def clear_index client, index
  if client.index_exists index
    client.delete_by_query(index, { query: { match_all: {} } })
  end
end


# should be inside Indexes, but uses *settings*
def destroy_existing_indexes client
  Indexes.instance.indexes.each do |type, indexes|
    indexes.each do |groups, index|
      index_name = index[:index]
      if client.index_exists index_name
        log.info "Deleting #{index_name}"
        client.delete_index index_name
      end
      remove_index index_name
    end
  end
end


def invalidate_indexes s, type
  Indexes.instance.indexes[type].each do |key, index| 
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


def load_persisted_indexes types
  types.each do |type_def|
    type = type_def["type"]
    Indexes.instance.indexes[type] = load_indexes type
  end
end

def destroy_persisted_indexes client
  get_persisted_index_names().each do |result|
    index_name = result['index_name']
    if client.index_exists index_name
      log.info "Deleting #{index_name}"
      client.delete_index index_name
    end
    remove_index index_name
  end
end


def store_index type, index, allowed_groups, used_groups
  uuid = generate_uuid()
  uri = "http://mu.semte.ch/authorization/elasticsearch/indexes/#{uuid}"

  def group_statement predicate, groups
    if groups.empty?
      ""
    else
      group_set = groups.map { |g| sparql_escape_uri g }.join(",")
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


def get_request_index type
  allowed_groups, used_groups = get_request_groups
  Indexes.instance.find_matching_index type, allowed_groups, used_groups
end


def store_index type, index, allowed_groups, used_groups
  uuid = generate_uuid()
  uri = "http://mu.semte.ch/authorization/elasticsearch/indexes/#{uuid}"

  def group_statement predicate, groups
    if groups.empty?
      ""
    else
      group_set = groups.map { |g| sparql_escape_uri g }.join(",")
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


def remove_index index_name
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




def get_request_groups
  allowed_groups_s = request.env["HTTP_MU_AUTH_ALLOWED_GROUPS"]
  allowed_groups = 
    allowed_groups_s ? JSON.parse(allowed_groups_s).map { |e| e["value"] } : []

  used_groups_s = request.env["HTTP_MU_AUTH_USED_GROUPS"]
  used_groups = used_groups_s ? JSON.parse(used_groups_s).map { |e| e["value"] } : []

  return allowed_groups.sort, used_groups.sort
end


# * to do: check if index exists before creating it
# * to do: how to name indexes?
def create_request_index client, type, allowed_groups = nil, used_groups = nil
  unless allowed_groups
    allowed_groups, used_groups = get_request_groups
  end

  index = Digest::MD5.hexdigest (type + "-" + allowed_groups.join("-"))
  uri =  store_index type, index, allowed_groups, used_groups    

  index_definition =   {
    index: index,
    uri: uri,
    allowed_groups: allowed_groups,
    used_groups: used_groups 
  }
  
  
  Indexes.instance.add_index type, allowed_groups, used_groups, index_definition

  begin
    client.create_index index, settings.type_definitions[type]["es_mappings"]
  rescue
    if client.index_exists index
      log.info "Index not created, already exists: #{index}"
      # Is this an error??
    else
      raise "Error creating index: #{index}"
    end
  end

  index
end


def get_index_safe client, type
  def sync client, type
    settings.master_mutex.synchronize do
      index = get_request_index type
      if index
        if Indexes.instance.status(index) == :invalid
          Indexes.instance.set_status index, :updating
          return index, true
        else
          return index, false
        end
      else
        index = create_request_index client, type
        Indexes.instance.set_status index, :updating
        return index, true
      end
    end
  end

  index, update_index = sync client, type
  if update_index
    Indexes.instance.mutex(index).synchronize do
      begin
        clear_index client, index
        index_documents client, type, index
        client.refresh_index index
        Indexes.instance.set_status index, :valid
      rescue
        Indexes.instance.set_status index, :invalid
      end
    end
  end

  index
end


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
      allowed_groups = allowed_groups_result.map { |g| g["group"].to_s }

      used_groups_result = direct_query  <<SPARQL
  SELECT * WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        <#{uri}> <http://mu.semte.ch/vocabularies/authorization/hasUsedGroup> ?group
    }
  }
SPARQL
      used_groups = used_groups_result.map { |g| g["group"].to_s }

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
