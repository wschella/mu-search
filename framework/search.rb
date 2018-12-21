def split_filter filter
  match = /(?:\:)([^ ]+)(?::)(\w+)/.match(filter)
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


def construct_es_query
  filters = params['filter'] && params['filter'].map do |field, val| 
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
    data: JSON.parse(results)["hits"]["hits"].map do |result|
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
