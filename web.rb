require 'net/http'
require 'digest'
require 'set'
require 'request_store'
require 'listen'
require 'singleton'
require 'base64'

require_relative 'framework/elastic.rb'
require_relative 'framework/sparql.rb'
require_relative 'framework/authorization.rb'
require_relative 'framework/indexing.rb'
require_relative 'framework/search.rb'
require_relative 'framework/update.rb'

before do
  request.path_info.chomp!('/')
end

# Applies basic configuration from environment variables and from
# configuration file (environment variables win).  Ensures
# elasticsearch is up and the database is set up.
#
# Called from the configure block.
#
# TODO is_reload is supplied but does not seem to be used.
#
# TODO This seems to contain both configuration as well as setup.  It
# would make sense to split both.
def configure_settings client, is_reload = nil
  configuration = JSON.parse File.read('/config/config.json')

  set :master_mutex, Mutex.new

  set :dev, (ENV['RACK_ENV'] == 'development')

  set :batch_size, (ENV['BATCH_SIZE'] || configuration["batch_size"] || 100)

  set :max_batches, (ENV['MAX_BATCHES'] || configuration["max_batches"])

  set :persist_indexes, ENV['PERSIST_INDEXES'] || configuration["persist_indexes"]

  set :additive_indexes, ENV['ADDITIVE_INDEXES'] || configuration["additive_indexes"]

  raw = ENV['ENABLE_RAW_DSL_ENDPOINT'] || configuration["enable_raw_dsl_endpoint"]

  set :raw_dsl_endpoint, ['true','True','TRUE'].include?(raw)

  set :default_index_settings, configuration["default_settings"]

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
          index = get_matching_index_name type, groups, []
          index_name = (index and index[:index]) || create_index(client, type, groups, [])

          unless settings.persist_indexes and index and client.index_exists index_name
            log.info "Clearing index for type #{type} - #{index_name}."
            clear_index client, index_name
            index_documents client, type, index_name, groups
            Indexes.instance.set_status index_name, :valid
          else
            log.info "Using persisted index: #{index_name}"
          end
        end
      end
    end
  end
end

# Configures the system and makes sure everything is up.  Heavily
# relies on configure_settings.
#
# TODO: get attachment pipeline names from configuration
configure do
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  while !client.up
    log.info "...waiting for elasticsearch..."
    sleep 1
  end

  # TODO provide more explicit abstraction to send out sudo calls. All
  # calls that go through this endpoint (rather than the one provided
  # by the mu-ruby-template seem to be mu-auth-sudo calls so it's easy
  # to replace.
  set :db, SinatraTemplate::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'], { headers: { 'mu-auth-sudo': 'true' } } )
  while !sparql_up do
    log.info "...waiting for SPARQL endpoint..."
    sleep 1
  end

  # hardcoded pipeline names (for now)
  client.create_attachment_pipeline "attachment", "data"
  client.create_attachment_array_pipeline "attachment_array", "data"

  configure_settings client


  if settings.dev
    listener = Listen.to('/config/') do |modified, added, removed|
      if modified.include? '/config/config.json'
        log.info 'Reloading configuration'
        destroy_existing_indexes client
        configure_settings client, true
        log.info '== Configuration reloaded'
      end
    end

    listener.start
  end
end


# Provides the type which matches the given path based on the supplied
# configuration.
def get_type_from_path path
  settings.type_paths[path]
end

# Invalidates the indexes resulting them to be updated in a next
# search query.
post "/:path/invalidate" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type_from_path path
  allowed_groups = get_allowed_groups
  used_groups = []

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
        index_names = get_request_index_names type

        index_names.each do |index|
          Indexes.instance.mutex(index).synchronize do
            Indexes.instance.set_status index, :invalid
          end
        end

        { indexes: indexes, status: "invalid" }.to_json
      end
    end
  end
end

# Deletes the indexes for :path requiring them to be fully recreated
# the next time we make a search.
delete "/:path/delete" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type_from_path path
  allowed_groups = get_allowed_groups
  used_groups = []

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

        indexes_deleted = Indexes.instance.get_indexes(type).map do |groups, index|
          destroy_index client, index[:index], groups
          index[:index]
        end

        { indexes: indexes_deleted, status: "deleted" }.to_json
      else
        index_names = get_request_index_names type

        index_names.each do |index|
          Indexes.instance.mutex(index).synchronize do
            destroy_index client, index, type, allowed_groups
          end
        end

        { indexes: index_names, status: "deleted" }.to_json
      end
    end
  end
end


# TODO document the POST mothed on :path/index
post "/:path/index" do |path|
  content_type 'application/json'
  client = Elastic.new host: 'elasticsearch', port: 9200
  allowed_groups = get_allowed_groups
  used_groups = []

  # This method shouldn't be necessary...
  # something wrong with how I'm using synchronize
  # and return values.
  def sync client, type
    settings.master_mutex.synchronize do
      index_names = get_request_index_names type

      unless !index_names.empty?
        indexes = create_request_indexes client, type
        index_names = indexes.map { |index| index[:index] }
      end

      index_names.each do |index|
        Indexes.instance.set_status index, :updating
      end

      return index_names
    end
  end

  def index_index client, index, type, allowed_groups = nil
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
          index_names = sync client, type
          index_names.each do |index|
            index_index client, index, type
          end
        end
      else
        type = get_type_from_path path
        index_names = sync client, type
        index_names.each do |index|
          index_index client, index, type
        end
      end
    else
      if path == '_all'
        report = Indexes.instance.indexes.map do |type, indexes|
          indexes.map do |groups, index|
            index_index client, index[:index], type, groups
          end
        end
        report.reduce([], :concat)
      else
        type = get_type_from_path path
        if Indexes.instance.get_indexes(type)
          Indexes.instance.get_indexes(type).map do |groups, index|
            index_index client, index[:index], type, groups
          end
        end
      end
    end
  report.to_json
end


# Performs a regular search.
#
# Creates new indexes, updates older ones and keeps constructed ones
# as necessary.  This is the standard entrypoint for your mu-search
# queries.
#
# Check Readme.md for more information.
#
# TODO move this method up as it's the most common entrypoint
#
# TODO fleshen out functionality with respect to existing indexes
get "/:path/search" do |path|
  log.debug "SEARCH Got allowed groups #{request.env["HTTP_MU_AUTH_ALLOWED_GROUPS"]}"

  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type_from_path path
  collapse_uuids = (params["collapse_uuids"] == "t")

  log.debug "SEARCH Found type #{type}"

  index_names = get_or_create_indexes client, type

  log.debug "SEARCH Found indexes #{index_names}"

  # TOOD: Not sure how this could ever be empty.  We should at least
  # be able to build some indexes if we have received some groups.
  # This is a method of last resort which currently does the job.
  return [].to_json if index_names.length == 0

  index_string = index_names.join(',')

  log.debug "SEARCH Searching index(es): #{index_string}"

  es_query = construct_es_query type

  log.debug "SEARCH ElasticSearch query: #{es_query}"
  log.debug "SEARCH ElasticSearch query as json: #{es_query.to_json}"

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

  if collapse_uuids
    es_query["collapse"] = { field: "uuid" }
    es_query["aggs"] = { "type_count" => { "cardinality" => { "field" => "uuid" } } }
  end

  # hard-coded example
  # question: how to specify which fields are included/excluded?
  # or should we simply exclude all attachment fields?
  es_query["_source"] = {
    excludes: ["data","attachment"]
  }

  # while Indexes.instance.status index == :updating

  while index_names.map { |index| Indexes.instance.status index == :updating }.any?
    sleep 0.5
  end

  log.debug "All indexes are up to date"
  log.debug "Running ES query: #{es_query.to_json}"

  results = JSON.parse(client.search index: index_string, query: es_query)


  log.debug "Got native results: #{results}"

  count = 
    if collapse_uuids
      results["aggregations"]["type_count"]["value"]
    else
      count_result = JSON.parse(client.count index: index_string, query: count_query)
      count_result["count"]
    end

  log.debug "Got #{count} results"

  format_results(type, count, page, size, results).to_json
end


# Raw ES Query DSL
# Need to think through several things, such as pagination
if settings.raw_dsl_endpoint
  post "/:path/search" do |path|
    content_type 'application/json'
    client = Elastic.new(host: 'elasticsearch', port: 9200)
    type = get_type_from_path path
    index_names = get_or_create_indexes client, type

    return [].to_json if index_names.length == 0

    index_string = index_names.join(',')

    es_query = @json_body

    count_query = es_query.clone
    count_query.delete("from")
    count_query.delete("size")
    count_result = JSON.parse(client.count index: index_string, query: es_query)
    count = count_result["count"]

    format_results(type, count, 0, 10, client.search(index: index_string, query: es_query)).to_json
  end
end

# Processes an update from the delta system.  Consumes the genesis
# delta format and invalidates the necessary indexes.
#
# TODO it seems this invalidates the full index, rather than trying to
# add the respective document to all related indexes when
# automatic_index_updates is set.  We may lack information to handle
# invalidation when paths are being used to index contents.
post "/update" do
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  # [[d, s, p, o], ...] where d is :- or :+ for deletes/inserts respectively
  log.debug "Received delta update #{@json_body}"
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

# Enables the automatic_updates setting on a live system
post "/settings/automatic_updates" do
  settings.automatic_index_updates = true
end


# Disables the automatic_updates setting on a live system
delete "/settings/automatic_updates" do
  settings.automatic_index_updates = false
end


# Enables the persistent indexes setting on a live system
post "/settings/persist_indexes" do
  settings.persist_indexes = true
end


# Disables the persistent indexes setting on a live system
delete "/settings/persist_indexes" do
  settings.persist_indexes = false
end
