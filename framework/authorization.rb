def find_matching_index type, allowed_groups, used_groups
  index = settings.indexes[type] && settings.indexes[type][allowed_groups]
  index and index[:index]
end


def destroy_existing_indexes client
  get_index_names().each do |result|
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


def get_index_names 
  direct_query <<SPARQL
SELECT ?index_name WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
               <http://mu.semte.ch/vocabularies/authorization/indexName> ?index_name
    }
  }
SPARQL
end


def load_indexes type
  index = {}

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

    index[allowed_groups] = { 
      uri: uri,
      index: index_name,
      allowed_groups: allowed_groups, 
      used_groups: used_groups 
    }

    settings.mutex[index_name] = Mutex.new
  end

  index
end


def get_request_index type
  allowed_groups, used_groups = get_request_groups
  find_matching_index type, allowed_groups, used_groups
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


def get_index_names 
  direct_query <<SPARQL
SELECT ?index_name WHERE {
    GRAPH <http://mu.semte.ch/authorization> {
        ?index a <http://mu.semte.ch/vocabularies/authorization/ElasticsearchIndex>;
               <http://mu.semte.ch/vocabularies/authorization/indexName> ?index_name
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
  log.info "Creating index: #{index} of type #{type} + #{allowed_groups}"
  uri =  store_index type, index, allowed_groups, used_groups    

  index_definition =   {
    index: index,
    uri: uri,
    allowed_groups: allowed_groups,
    used_groups: used_groups 
  }
  
  settings.indexes[type] = {} unless settings.indexes[type]
  settings.indexes[type][allowed_groups] = index_definition
  settings.mutex[index_definition[:index]] = Mutex.new

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
        if settings.index_status[index] == :invalid
          settings.index_status[index] = :updating
          return index, true
        else
          return index, false
        end
      else
        index = create_request_index client, type
        settings.index_status[index] = :updating
        return index, true
      end
    end
  end

  index, update_index = sync client, type
  if update_index
    settings.mutex[index].synchronize do
      begin
        index_documents client, type, index
        client.refresh_index index
        settings.index_status[index] = :valid
      rescue
        settings.index_status[index] = :invalid
      end
    end
  end

  index
end

