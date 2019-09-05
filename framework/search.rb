# This file contains helpers for processing the public search api and
# converting it into ElasticSearch.

# Filter for splitting the optional modifier from a search string.
#
#   - filter: the query parameter received as the search key.
#
# Yields two values: the modifier and the search property.
def split_filter filter
  match = /(?:\:)([^ ]+)(?::)([\w,.]*)/.match(filter)
  if match
    return match[1], match[2]
  else
    return nil, filter
  end
end


# Converts the sort statement from the params into an ElasticSearch
# sort statement.
#
# Yields an elasticSearch filter in the form of a Ruby native json
# object.
def es_query_sort_statement
  params['sort'] && params['sort'].collect do |field, val|
    flag, field = split_filter field
    unless flag
      { field => val }
    else
      { field => { order: val, mode: flag } }
    end
  end
end

# Yields the attachment's field if a field contains an attachment, or
# the field itself if it did not.
#
# Can be seen as a filter to pass a field through to translate to the
# attachment field if necessary.
#
# TODO: This is named strangely.  Perhaps we should turn this into a
# name that only indicates that we're translating the publicly visible
# field name into the internal one.
def translate_attachment_field type, field
  properties = settings.type_definitions[type]["properties"]
  if properties[field].is_a? Hash and properties[field]["attachment_pipeline"]
    ["attachment.content", "#{field}.attachment.content"]
  else
    field
  end
end

# Constructs an ElasticSearch query in ruby JSON format based on the
# supplied filter parameters.
#
#   - type: Type of field which was queried.
def construct_es_query type
  filters = params['filter'] && params['filter'].map do |field, val|
    term = construct_es_query_term type, field, val
    term
  end.flatten

 if filters.length == 1
    { query: filters.first }
  else
    {
      query: {
        bool: {
          must: filters
        }
      }
    }
  end
end

# Abstraction for coping with filters that require a single field to
# be provided.
#
# name should be the name of your filter (for error output), fields
# should be the parsed fields (nil or an array).
def ensuring_single_field_for name, fields
  if fields && fields.length == 1
    yield fields.first
  else
    log.error("#{name} match works on exactly one field, received #{fields}")
    { }
  end
end

# Constructs an ElasticSearch query path for the given field and
# value.
#
# TODO: I don't know what this means.  Please describe.
def construct_es_query_path field, val
  if val.is_a? Hash
    val.map do |k, v|
      construct_es_query_path "#{field}.#{k}",  v
    end
  else
    { match: { field => val } }
  end
end

# Parses a filter argument as received by the construct_es_query_term
# function and converts it into the correct fields as used by
# elasticsearch.
#
# Emits flag (the thing between colons), and fields.  If no flag was
# given, it will be nil, the same holds for zero fields.
def parse_filter_argument( filter_argument, type )
  flag, fields_string = split_filter filter_argument
  fields_arr = fields_string
                 .split( "," )
                 .map { |field| translate_attachment_field( type, field ) }
                 .flatten()
  fields = fields_arr.length > 0 ? fields_arr : nil

  return flag, fields
end

# Constructs an ElasticSearch query term
#
# TODO: I don't really grok what this means either in detail either.
# I see things I recognize, but didn't connect the dots yet.
def construct_es_query_term type, filter_argument, val
  flag, fields = parse_filter_argument( filter_argument, type )

  case flag
  when nil
    { multi_match:
        { query: val, fields: fields == ['_all'] ? nil : fields }.compact
    }
  when 'fuzzy_phrase'
    make_fuzzy_phrase_match fields, val
  when 'fuzzy_match'
    make_fuzzy_match fields, val
  when 'term', 'fuzzy', 'prefix', 'wildcard', 'regexp'
    ensuring_single_field_for 'term fuzzy prefix wildcard or regexp', fields do |field|
      { flag => { field => val } }
    end
  when 'phrase', 'phrase_prefix'
    ensuring_single_field_for 'phrase and phrase_prefix', fields do |field|
      { 'match_' + flag => { field => val } }
    end
  when "terms"
    ensuring_single_field_for 'terms', fields do |field|
      { terms: { field => val.split(',') } }
    end
  when 'gte', 'lte', 'gt', 'lt'
    ensuring_single_field_for 'gte lte gt and lt', fields do |field|
      { range: { first_field => { flag => val } } }
    end
  when 'lte,gte', 'lt,gt', 'lt,gte', 'lte,gt'
    ensuring_single_field_for 'range combinations', fields do |field|
      flags = flag.split(',')
      vals = val.split(',')
      { range: { field => { flags[0] => vals[0], flags[1] => vals[1] } } }
    end
  when 'query'
    ensuring_single_field_for 'query', fields do |field|
      { query_string: { default_field: field, query: val } }
    end
  when 'sqs'
    {
      simple_query_string: {
        query: val,
        fields: fields,
        default_operator: "and",
        all_fields: fields ? nil : true
      }.compact
    }
  when /common(,[0-9.]+){,2}/
    ensuring_single_field_for 'common', fields do |field|
      flag, cutoff, min_match = flag.split(',')
      cutoff = cutoff or settings.common_terms_cutoff_frequency
      term = { common: { field => { query: val, cutoff_frequency: cutoff } } }
      if min_match
        term['minimum_should_match'] = min_match
      end
      term
    end
  end
end

# Formats the ElasticSearch resurts in a JSONAPI like manner.
#
#   - type: Type of instance which was searched for.
#   - count: Total amount of results that are available
#   - page: Currently requested page (in terms of pagination)
#   - size: Amount of results on a single page.
#   - results: Actual results in the form of the ElasticSearch
#     response.
#
# TODO: it would be nice if we could somehow return the matched
# section of the document.  In a perfect world, combined with some
# context around it.
def format_results type, count, page, size, results
  last_page = count/size
  next_page = [page+1, last_page].min
  prev_page = [page-1, 0].max

  query_string = request.query_string.gsub(/&?page\[(number|size)\]=[0-9]+/, '')
  uri =  request.path + '?' + query_string

  def join *elements
    elements.reduce('') { |cumm, e| e == '' ? cumm : (cumm == '' ? e : cumm + '&' + e) }
  end

  def page_string uri, page, size
    size_specd = params["page"] && params["page"]["size"]
    size_string = (size_specd && "page[size]=#{size}") || ''
    zero = page == 0
    page_number_string = zero ? '' : "page[number]=#{page}"
    join uri, page_number_string, size_string
  end

  {
    count: count,
    data: results["hits"]["hits"].map do |result|
      {
        type: type,
        id: result["_id"],
        attributes: result["_source"]
      }
    end,
    links: {
      self: page_string(uri, page, size),
      first: page_string(uri, 0, size),
      last:  page_string(uri, last_page, size),
      prev:  page_string(uri, prev_page, size),
      next:  page_string(uri, next_page, size)
    }
  }
end


# Constructs a fuzzy phrase match as described at
# https://stackoverflow.com/questions/38816955/elasticsearch-fuzzy-phrases#38823174
#
# TODO: this type of search does not seem to work at the moment.  We
# should verify how to get this running with our version of
# ElasticSearch as it may be a common way of searching.
def make_fuzzy_phrase_match fields, value
  ensuring_single_field_for 'fuzzy phrase match', fields do |field|
    { 
      "span_near" => {
        "in_order" => true,
        "slop" => 2,
        "clauses" => value.split(" ").map do |word|
          { "span_multi" => {
              "match" => {
                "fuzzy" => {
                  "#{field}" => {
                    "fuzziness" => "AUTO",
                    "value" => word
                  }
                }
              }
            }
          }
        end
      } }
  end
end

# Fuzzy matcher trying to match within a sentence.  This essentially
# accepts multiple words and verifies that they exist within the
# document using a fuzzy search.
#
# This is merely a try to figure out how to create some form of usable
# matching.
def make_fuzzy_match fields, value
  ensuring_single_field_for 'fuzzy match', fields do |field|
    {
      "match" => {
        "#{field}" => {
          "query" => value,
          "operator" => "and",
          "fuzziness" => "AUTO"
        }
      }
    }
  end
end
