require 'net/http'
require 'json'

ELASTIC = 'http://localhost:8888'
SPARQL = 'http://localhost:4027/sparql'

def elastic(path, allowed_groups, test = nil)
  uri = URI(ELASTIC + path)
  req = Net::HTTP::Get.new(uri)
  allowed_groups_object = allowed_groups.map { |group| { value: group } }
  req['MU_AUTH_ALLOWED_GROUPS'] = allowed_groups_object.to_json

  res = Net::HTTP.start(uri.hostname, uri.port) { |http|
    http.request(req)
  }

  case res
  when Net::HTTPSuccess, Net::HTTPRedirection
    result = JSON.parse(res.body)
    if test
      result == test
    else
      result
    end
  else
    res.value
  end
end

def sparql(allowed_groups, query)
  uri = URI SPARQL
  req = Net::HTTP::Get.new(uri)
  allowed_groups_object = allowed_groups.map { |group| { value: group } }

  req['MU_AUTH_ALLOWED_GROUPS'] = allowed_groups_object.to_json
  req.body = query

  res = Net::HTTP.start(uri.hostname, uri.port) { |http|
    http.request(req)
  }

  case res
  when Net::HTTPSuccess, Net::HTTPRedirection
    JSON.parse(res.body)
  else
    res.value
  end
end

def run_test(value)
  result = yield
  if value == result
    puts "SUCCESS"
  else
    raise "\n*** ERROR ***\nExpected: #{value}\nReceived: #{result}\n*************"
  end
end

def automatic_updates(val)
  uri = URI(ELASTIC + '/settings/automatic_updates')
  req = val ? req = Net::HTTP::Post.new(uri) : req = Net::HTTP::Delete.new(uri)

  res = Net::HTTP.start(uri.hostname, uri.port) { |http|
    http.request(req)
  }
end

def persist_indexes(val)
  uri = URI(ELASTIC + '/settings/persist_indexes')
  req = val ? req = Net::HTTP::Post.new(uri) : req = Net::HTTP::Delete.new(uri)

  res = Net::HTTP.start(uri.hostname, uri.port) { |http|
    http.request(req)
  }
end
