def split_filter filter
  match = /(?:\:)(\w+)(?::)(\w+)/.match(filter)
  if match
    return match[1], match[2]
  else
    return nil, filter
  end
end


def split_fields field
  fields = field.split(',')
  if fields.length > 1
    fields
  else
    nil
  end
end


# Currently supports ES methods that can be given a single value, e.g., match, term, prefix, fuzzy, etc.
# i.e., any method that can be written: { "query": { "METHOD" : { "field": "value" } } }
# * not supported yet: everything else, e.g., value, range, boost...
# Currently combined using { "bool": { "must": { ... } } } 
# * to do: range queries
# * to do: sort
def construct_es_query
  filters = params["filter"].map do |field, val| 
    if field == '_all'
      { multi_match: { query: val, fields: ['*'] } }
    else
      flag, field = split_filter field
      fields = split_fields field

      unless flag
        if fields
          { multi_match: { query: val, fields: fields } }
        else
          { match: { field => val } }
        end
      else
        case flag
        when 'term', 'fuzzy', 'prefix', 'wildcard', 'regexp'
          { flag => { field => val } }
        when "terms"
          { terms: { field => val.split(',') } }
        when 'gte', 'lte', 'gt', 'lt'
          { range: { field => { flag => val } } }
        when 'lte,gte', 'lt,gt', 'lt,gte', 'lte,gt'
          flags = flag.split(',')
          vals = val.split(',')
          { range: { field => { flags[0] => vals[0], flags[1] => vals[1] } } }
        when "query"
          { query_string: { default_field: field, query: val } }
        end
      end
    end
  end

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


def format_results type, count, page, size, results
  last_page = count/size
  next_page = [page+1, last_page].min
  prev_page = [page-1, 0].max

  { 
    count: count,
    data: JSON.parse(results)["hits"]["hits"].map do |result|
      {
        type: type,
        id: result["_id"],
        attributes: result["_source"]
      }
    end,
    links: {
      self: "http://application/",
      first: "page[number]=0&page[size]=#{size}",
      last: "page[number]=#{last_page}&page[size]=#{size}",
      prev: "page[number]=#{prev_page}&page[size]=#{size}",
      next: "page[number]=#{next_page}&page[size]=#{size}"
    }
  }
end
