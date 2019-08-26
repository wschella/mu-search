# This file contains helpers for processing the public search api and
# converting it into ElasticSearch.

# Filter for splitting the optional modifier from a search string.
#
#   - filter: the query parameter received as the search key.
#
# Yields two values: the modifier and the search property.
def split_filter filter
  match = /(?:\:)([^ ]+)(?::)(\w+)/.match(filter)
  if match
    return match[1], match[2]
  else
    return nil, filter
  end
end

# Splits fields when multiple were given.
#
# It is possible to supply multiple fields in some queries.  They are
# then split by a comma.  THis function splits the fields.
#
# TODO: I find it strange that this requires there to be more than one
# field.  Perhaps I'm not interpreting this correctly.  I would assume
# this to yield field.split(',') but it seems something else is
# expected.  Perhaps the consuming code would better check on this
# being more than one element long.
def split_fields field
  fields = field.split(',')
  if fields.length > 1
    fields
  else
    nil
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
def attachment_field type, field
  properties = settings.type_definitions[type]["properties"]
  if properties[field].is_a? Hash and properties[field]["attachment_pipeline"]
    "attachment.content"
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

# Constructs an ElasticSearch query term
#
# TODO: I don't really grok what this means either in detail either.
# I see things I recognize, but didn't connect the dots yet.
def construct_es_query_term type, field, val
  if field == '_all'
    { multi_match: { query: val } }
  else
    flag, field = split_filter field
    fields = split_fields field

    fields = fields ? fields.map { |f| attachment_field type, f } : nil
    field = attachment_field type, field

    if val.is_a? Hash
      t = construct_es_query_path field, val
      t
    elsif not flag
      if fields
        { multi_match: { query: val, fields: fields } }
      else
        { match: { field => val } }
      end
    else
      case flag
      when 'fuzzy_phrase'
        make_fuzzy_phrase_match field, val
      when 'fuzzy_match'
        make_fuzzy_match field, val
      when 'term', 'fuzzy', 'prefix', 'wildcard', 'regexp'
        { flag => { field => val } }
      when 'phrase', 'phrase_prefix'
        { 'match_' + flag => { field => val } }
      when "terms"
        { terms: { field => val.split(',') } }
      when 'gte', 'lte', 'gt', 'lt'
        { range: { field => { flag => val } } }
      when 'lte,gte', 'lt,gt', 'lt,gte', 'lte,gt'
        flags = flag.split(',')
        vals = val.split(',')
        { range: { field => { flags[0] => vals[0], flags[1] => vals[1] } } }
      when 'query'
        { query_string: { default_field: field, query: val } }
      when /common(,[0-9.]+){,2}/
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
def make_fuzzy_phrase_match field, value
  { "in_order" => true,
    "slop" => 2,
    "span_near" => {
      "clauses" => value.split(" ").map do |word|
        { "span_multi" => {
            "match" => {
              "fuzzy" => {
                "#{field}" => {
                  "fuzziness" => 2,
                  "value" => word } } } } }
      end
    } }
end

# Fuzzy matcher trying to match within a sentence.  This essentially
# accepts multiple words and verifies that they exist within the
# document using a fuzzy search.
#
# This is merely a try to figure out how to create some form of usable
# matching.
def make_fuzzy_match field, value
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
