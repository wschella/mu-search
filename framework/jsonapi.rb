# Formats Elasticsearch search results in a JSONAPI like manner.
#   - type: Type of instance which was searched for.
#   - count: Total amount of results that are available
#   - page: Currently requested page (in terms of pagination)
#   - size: Amount of results on a single page
#   - results: Actual results in the form of the ElasticSearch
#     response.
#
# TODO: it would be nice if we could somehow return the matched
# section of the document.  In a perfect world, combined with some
# context around it.
def format_search_results(type, count, page, size, results)
  last_page = count / size
  next_page = [page + 1, last_page].min
  prev_page = [page - 1, 0].max

  query_string = request.query_string.gsub(/&?page\[(number|size)\]=[0-9]+/, '')
  uri = request.path + '?' + query_string

  def join(*elements)
    elements.reduce('') { |cumm, e| e == '' ? cumm : (cumm == '' ? e : cumm + '&' + e) }
  end

  def page_string(uri, page, size)
    size_specd = params["page"] && params["page"]["size"]
    size_string = (size_specd && "page[size]=#{size}") || ''
    zero = page == 0
    page_number_string = zero ? '' : "page[number]=#{page}"
    join uri, page_number_string, size_string
  end

  {
    count: count,
    data: results["hits"]["hits"].map do |result|
      uuid = result.dig("_source", "uuid") || result["_id"]
      {
        type: type,
        id: uuid,
        attributes: result["_source"].merge({ uri: result["_id"] }),
        highlight: result["highlight"]
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
