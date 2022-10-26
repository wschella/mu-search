# Query builder for Elasticsearch queries
#
# This is your one-stop shop for the construction of search queries
# and the mapping of search query params to Elasticsearch Query DSL
class ElasticQueryBuilder
  attr_reader :page_number, :page_size, :sort, :collapse_uuids

  def initialize(logger:, type_definition:, filter:, page:, sort:, highlight:, collapse_uuids:, search_configuration:)
    @logger = logger
    @type_def = type_definition
    @filter = filter
    @page_number = page && page["number"] ? page["number"].to_i : 0
    @page_size = page && page["size"] ? page["size"].to_i : 10
    @sort = sort
    @highlight = highlight
    @collapse_uuids = true? collapse_uuids
    @configuration = search_configuration
  end

  # Build an Elasticseach query to search documents
  # Returns the Elasticsearch query as Ruby JSON object
  # Raises an ArgumentError for invalid search parameters
  def build_search_query
    @es_query = {}
    build_filter
      .build_sort
      .build_pagination
      .build_highlight
      .build_collapse
      .build_source_fields
    @es_query
  end

  # Build an Elasticseach query to count search results
  # Returns the Elasticsearch query as Ruby JSON object
  # Raises an ArgumentError for invalid search parameters
  def build_count_query
    @es_query = {}
    build_filter
      .build_collapse
    @es_query
  end

  # Constructs an Elasticsearch query
  # based on the filter parameters and type definition
  def build_filter
    if @filter && !@filter.empty?
      filters = @filter.map { |key, value| construct_es_query_term key, value }
      if filters.length == 1
        @es_query["query"] = filters.first
      else
        @es_query["query"] =
          {
            bool: {
              must: filters
            }
          }
      end
    end
    self
  end

  # Converts a param like "sort[:mode:field]=order"
  # to an array of objects like { <field>: { "order": <order>, "mode": <mode> }
  #
  # Order must be one of "asc", "desc"
  # Mode must be one of "min", "max", "sum", "avg", "median"
  #
  # Multiple sort fields are supported by providing multiple sort params
  # E.g. sort[title]=asc&sort[modified]=desc
  def build_sort
    if @sort
      sort = @sort.map do |key, order|
        mode, fields = split_filter key
        ensure_single_field_for "sort", fields do |field|
          if mode.nil?
            { field => order }
          else
            { field => { order: order, mode: mode } }
          end
        end
      end
      @es_query["sort"] = sort
    end
    self
  end

  def build_pagination
    @es_query["from"] = @page_number * @page_size
    @es_query["size"] = @page_size
    self
  end

  # Constructs elastic search highlight configuration.
  # An object of the shape:
  # { fields: { "field1": {}, "field2": {}, ...}}
  # One can use "*" as a field name to highlight all fields.
  # https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html
  def build_highlight
    if @highlight and !@filter.empty? and @highlight[":fields:"]
      @es_query["highlight"] = {
        fields:  @highlight[":fields:"].split(",").map{|field| [field, {}]}.to_h
      }
    end
    self
  end

  def build_collapse
    if @collapse_uuids
      @es_query["collapse"] = { field: "uuid" }
      @es_query["aggs"] = {
        "type_count" => {
          "cardinality" => {
            "field" => "uuid"
          }
        }
      }
    end
    self
  end

  # Excludes fields containing file contents
  # from the _source field in the search results
  #
  # TODO correctly handle nested objects
  # TODO correctly handle composite types
  def build_source_fields
    props = @type_def["properties"]
    if props.is_a?(Array)
      props.each { |p| filter_file_fields p }
    elsif props.is_a?(Hash)
      filter_file_fields props
    end
    self
  end

  def filter_file_fields(p)
      file_fields = p.select do |key, val|
        val.is_a?(Hash) && val["attachment_pipeline"]
      end
      @es_query["_source"] = {
        excludes: file_fields.keys
      }
  end

  private

  # Mapping of filter params to the Elasticseach Query DSL
  # See https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
  # - filter_key: key of the filter param (e.g. :fuzzy:title,description)
  # - value: value of the filter param
  def construct_es_query_term(filter_key, value)
    flag, fields = split_filter filter_key

    case flag
    when nil, "phrase", "phrase_prefix"
      multi_match_fields = fields == ["_all"] ? nil : fields
      {
        multi_match: { query: value, type: flag, fields: multi_match_fields }.compact
      }
    when "fuzzy"
      # Using `nil` instead of `*.*` to match all fields when no fields are specified, because `nil` will only match
      # all possible fields while `*.*` will match all (also conflicting, i.e. non keyword or text fields) fields.
      multi_match_fields = fields == ["_all"] ? nil : fields
      {
        multi_match: { query: value, fields: multi_match_fields, fuzziness: "AUTO" }.compact
      }
    when "term", "prefix", "wildcard", "regexp"
      ensure_single_field_for flag, fields do |field|
        {
          flag => { field => value }
        }
      end
    when "terms"
      ensure_single_field_for flag, fields do |field|
        {
          terms: { field => value.split(",") }
        }
      end
    when "fuzzy_phrase"
      ensure_single_field_for flag, fields do |field|
        clauses = value.split(" ").map do |word|
          {
            span_multi: {
              match: {
                fuzzy: {
                  field => { value: word, fuzziness: "AUTO" }
                }
              }
            }
          }
        end
        {
          span_near: { in_order: true, slop: 2, clauses: clauses }
        }
      end
    when "gte", "lte", "gt", "lt"
      ensure_single_field_for flag, fields do |field|
        {
          range: {
            field => { flag => value }
          }
        }
      end
    when "gte,lte", "gt,lte", "gt,lt", "gte,lt", "lte,gte", "lte,gt", "lt,gt", "lt,gte"
      ensure_single_field_for flag, fields do |field|
        flags = flag.split(",")
        values = value.split(",")
        if values.length == 2
          {
            range: {
              field => {
                flags[0] => values[0],
                flags[1] => values[1]
              }
            }
          }
        else
          raise ArgumentError, "Expected 2 comma-separated values for filter flag #{flag}, but received #{value}"
        end
      end
    when "has"
      ensure_single_field_for flag, fields do |field|
        if true? value
          {
            exists: { field: field }
          }
        else
          {}
        end
      end
    when "has-no"
      ensure_single_field_for flag, fields do |field|
        if true? value
          {
            bool: {
              must_not: {
                exists: {
                  field: field
                }
              }
            }
          }
        else
          {}
        end
      end
    when "query"
      ensure_single_field_for flag, fields do |field|
        {
          query_string: { default_field: field, query: value }
        }
      end
    when "sqs"
      all_fields = fields == ["_all"]
      {
        simple_query_string: {
          query: value,
          fields: all_fields ? nil : fields,
          default_operator: "and",
          all_fields: all_fields
        }.compact
      }
    when /common(,[0-9.]+){,2}/
      ensure_single_field_for "common", fields do |field|
        flag, cutoff, min_match = flag.split(",")
        cutoff = cutoff or @configuration[:common_terms_cutoff_frequency]
        term = {
          common: {
            field => { query: value, cutoff_frequency: cutoff }
          }
        }
        term["minimum_should_match"] = min_match if min_match
        term
      end
    else
      raise ArgumentError, "Unsupported filter flag :#{flag}:"
    end
  end

  # Utility to split the optional modifier from a filter key.
  # Modifiers prefix a filter key and are surrounded with ':'
  #   - key: the filter key to split
  #
  # E.g. ":avg:score" => ["avg", ["score"]]
  #      ":fuzzy:title,description" => ["fuzzy", ["title", "description"]]
  #
  # Returns a tuple of the modifier and fields
  def split_filter(filter_key)
    modifier = nil
    fields_s = filter_key

    match = /(?:\:)([^ ]+)(?::)([\w,.^*]*)/.match filter_key
    if match
      modifier = match[1]
      fields_s = match[2]
    end

    fields = fields_s.split(",")

    [modifier, fields]
  end

  # Ensure fields contains only a single element and yields it.
  # Raises an error if fields contains multiple elements.
  # - name: name of the filter, only used for error output
  # - fields: the parsed fields
  def ensure_single_field_for(name, fields)
    if fields && fields.length == 1
      yield fields.first
    else
      raise ArgumentError, "Param #{name} only supports exactly one field, but received #{fields}"
    end
  end

  def true?(obj)
    !obj.nil? &&
      (["true", "t"].include?(obj.to_s.downcase) || obj.to_s == "1")
  end
end
