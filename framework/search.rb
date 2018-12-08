

# Currently supports ES methods that can be given a single value, e.g., match, term, prefix, fuzzy, etc.
# i.e., any method that can be written: { "query": { "METHOD" : { "field": "value" } } }
# * not supported yet: everything else, e.g., value, range, boost...
# Currently combined using { "bool": { "must": { ... } } } 
# * to do: range queries
# * to do: sort
def construct_es_query
  filters = params["filter"].map do |field, v| 
    v.map do |method, val| 
      { method => { field => val } } 
    end
  end.first.first

  {
    query: {
      bool: {
        must: filters
      }
    }
  }

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
