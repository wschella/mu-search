require 'net/http'
require 'digest'
require 'set'
require 'request_store'
require 'thin'
require 'listen'
require 'singleton'

require_relative 'framework/elastic.rb'
require_relative 'framework/sparql.rb'
require_relative 'framework/authorization.rb'
require_relative 'framework/indexing.rb'
require_relative 'framework/search.rb'
require_relative 'framework/update.rb'



def configure_settings client, is_reload = nil
  configuration = JSON.parse File.read('/config/config.json')

  set :db, SinatraTemplate::SPARQL::Client.new('http://db:8890/sparql', {})
  
  set :master_mutex, Mutex.new

  set :dev, (ENV['RACK_ENV'] == 'development')

  set :batch_size, (ENV['BATCH_SIZE'] || configuration["batch_size"] || 100)

  set :persist_indexes, ENV['PERSIST_INDEXES'] || configuration["persist_indexes"]

  set :common_terms_cutoff_frequency, (ENV['COMMON_TERMS_CUTOFF_FREQUENCY'] || configuration["common_terms_cutoff_frequency"] || 0.001)

  set :automatic_index_updates,  
      (ENV["AUTOMATIC_INDEX_UPDATES"] || configuration["automatic_index_updates"])

  set :type_paths, Hash[
        configuration["types"].collect do |type_def|
          [type_def["on_path"], type_def["type"]]
        end
      ]

  set :type_definitions, Hash[
        configuration["types"].collect do |type_def|
          [type_def["type"], type_def]
        end
      ]

  while !client.up
    log.info "Waiting for ES"
    sleep 1
  end
  while !sparql_up
    log.info "Waiting for Virtuoso"
    sleep 1
  end

  if settings.persist_indexes
    log.info "Loading persisted indexes"
    load_persisted_indexes configuration["types"]
  else
    destroy_persisted_indexes client
  end

  # properties and types lookup-tables for updates
  # { <rdf_property> => [type, ...] }
  rdf_properties, rdf_types = configure_properties_types_lookup_tables configuration
  set :rdf_properties, rdf_properties
  set :rdf_types, rdf_types

  # Eager Indexing
  eager_indexing_groups = configuration["eager_indexing_groups"] || []

  # if configuration["eager_indexing_sparql_query"]
  #   query_result = query configuration["eager_indexing_sparql_query"]
  #   eager_indexing_groups += query_result.map { |key, value| value.to_s }
  # end

  unless eager_indexing_groups.empty?
    settings.master_mutex.synchronize do
      eager_indexing_groups.each do |groups|
        settings.type_definitions.keys.each do |type|
          index = Indexes.instance.find_matching_index type, groups, groups 
          index = index || create_request_index(client, type, groups, groups)
          clear_index client, index
          index_documents client, type, index, groups
        end
      end
    end
  end
end

configure do
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  configure_settings client

  if settings.dev
    listener = Listen.to('/config/') do |modified, added, removed|
      if modified.include? '/config/config.json'
        log.info 'Reloading configuration'
        destroy_existing_indexes client
        configure_settings client, true
      end
    end

    listener.start
  end
end


def get_type_from_path path
  settings.type_paths[path]
end


post "/:path/invalidate" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type_from_path path
  allowed_groups, used_groups = get_request_groups

  if path == '_all'
    settings.master_mutex.synchronize do
      indexes_invalidated =
        if allowed_groups.empty?
          Indexes.instance.invalidate_all
        else
          Indexes.instance.invalidate_all_authorized allowed_groups, used_groups
        end 
      { indexes: indexes_invalidated, status: "invalid" }.to_json
    end
  else
    settings.master_mutex.synchronize do
      if allowed_groups.empty?
        type = get_type_from_path path
        indexes_invalidated =
          Indexes.instance.invalidate_all_by_type type
        { indexes: indexes_invalidated, status: "invalid" }.to_json
      else
        index = get_request_index type
        Indexes.instance.mutex(index).synchronize do
          Indexes.instance.set_status index, :invalid
        end
        { index: index, status: "invalid" }.to_json
      end
    end
  end
end


delete "/:path" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type_from_path path
  allowed_groups, used_groups = get_request_groups

  if path == '_all'
    settings.master_mutex.synchronize do
      indexes_deleted =
        if allowed_groups.empty?
          destroy_existing_indexes client
        else
          destroy_authorized_indexes client, allowed_groups, used_groups
        end 
      { indexes: indexes_deleted, status: "deleted" }.to_json
    end
  else
    settings.master_mutex.synchronize do
      if allowed_groups.empty?
        type = get_type_from_path path

        indexes_deleted =
          Indexes.instance.get_indexes(type).map do |groups, index|
          destroy_index client, index[:index]
          Indexes.instance.indexes[type].delete(groups)
          index[:index]
        end

        { indexes: indexes_deleted, status: "invalid" }.to_json
      else
        index = get_request_index type
        Indexes.instance.mutex(index).synchronize do
          destroy_index client, index
          Indexes.instance.indexes[type].delete(allowed_groups)
        end

        { index: index, status: "deleted" }.to_json
      end
    end
  end
end


post "/:path/index" do |path|
  content_type 'application/json'
  client = Elastic.new host: 'elasticsearch', port: 9200
  allowed_groups, used_groups = get_request_groups
  # This method shouldn't be necessary... 
  # something wrong with how I'm using synchronize
  # and return values.
  def sync client, type
    settings.master_mutex.synchronize do
      index = get_request_index type

      unless index
        index = create_request_index client, type
      end

      Indexes.instance.set_status index, :updating
      return index
    end
  end

  # yes, rename this please
  def go client, index, type, allowed_groups = nil
    Indexes.instance.mutex(index).synchronize do
      clear_index client, index
      report = index_documents client, type, index, allowed_groups
      Indexes.instance.set_status index, :valid
      report
    end
  end

  report = 
    if !allowed_groups.empty?
      if path == '_all'
        Indexes.instance.types.map do |type|
          index = sync client, type
          go client, index, type
        end
      else
        type = get_type_from_path path
        index = sync client, type
        go client, index, type
      end
    else
      if path == '_all'
        report = 
          Indexes.instance.indexes.map do |type, indexes|
            indexes.map do |groups, index|
              go client, index[:index], type, groups
            end
          end
        report.reduce([], :concat)
      else
        type = get_type_from_path path
        if Indexes.instance.get_indexes(type)
          Indexes.instance.get_indexes(type).map do |groups, index|
            go client, index[:index], type, groups
          end
        end
      end
    end
  report.to_json
end


get "/:path/search" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type_from_path path

  index = get_index_safe client, type
  log.info "Searching index: #{index}"

  es_query = construct_es_query
  count_query = es_query.clone

  sort_statement = es_query_sort_statement

  if sort_statement
    es_query['sort'] = sort_statement
  end

  if params["page"]
    page = (params["page"]["number"] && params["page"]["number"].to_i) || 0
    size = (params["page"]["size"] && params["page"]["size"].to_i) || 10
  else
    page = 0
    size = 10
  end

  es_query["from"] = page * size
  es_query["size"] = size

  while Indexes.instance.status index == :updating
    sleep 0.5
  end

  count_result = JSON.parse(client.count index: index, query: count_query)
  count = count_result["count"]
  results = client.search index: index, query: es_query

  format_results(type, count, page, size, results).to_json
end


# Raw ES Query DSL
# Need to think through several things, such as pagination
post "/:path/search" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type_from_path path

  index = get_index_safe client, type

  es_query = @json_body

  count_query = es_query.clone
  count_query.delete("from")
  count_query.delete("size")
  count_result = JSON.parse(client.count index: index, query: es_query)
  count = count_result["count"]

  format_results(type, count, 0, 10, client.search(index: index, query: es_query)).to_json
end


post "/update" do
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  # [[d, s, p, o], ...] where d is :- or :+ for deletes/inserts respectively
  deltas = parse_deltas @json_body
  # Tabulate first, to avoid duplicate updates
  # { <uri> => [type, ...] } or { <uri> => false } if document should be deleted
  if deltas
    if settings.automatic_index_updates
      docs_to_update, docs_to_delete = tabulate_updates deltas
      docs_to_update.each { |s, types| update_document_all_types client, s, types }
      docs_to_delete.each { |s, types| delete_document_all_types client, s, types }
    else
      invalidate_updates deltas
    end
  end

  { message: "Thanks for all the updates." }.to_json
end


post "/settings/automatic_updates" do
  settings.automatic_index_updates = true
end 


delete "/settings/automatic_updates" do
  settings.automatic_index_updates = false
end 


post "/settings/persist_indexes" do
  settings.persist_indexes = true
end 


delete "/settings/persist_indexes" do
  settings.persist_indexes = false
end 

