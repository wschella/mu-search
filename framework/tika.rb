# A quick as-needed Tika API
#
# (Existing Tika gems seem not to work out of the box
# because of dependency problems.)
class TikaServer
  # Sets up the Tika instance
  def initialize(port: 9998)
    @host = 'localhost'
    @port_s = port.to_s
    IO.popen("java -jar /app/bin/tika-server-1.23.jar --host=localhost --port=#{@port_s}")
    log.info "Starting Tika server on port #{@port}..."
  end

  def up
    uri = URI("http://#{@host}:#{@port_s}/tika")
    log.info "checking http://#{@host}:#{@port_s}/tika"
    req = Net::HTTP::Get.new(uri)
    begin
      Net::HTTP.start(uri.hostname, uri.port) do |http|
        http.request(req)
      end
    rescue Exception => e
      log.info "An error of type #{e.class} happened, message is #{e.message}"
      false
    end
  end
end

class Tika
  include SinatraTemplate::Utils

  # Sets up the Tika instance
  def initialize(host: 'localhost', port: 9998)
    @host = host
    @port = port
    @port_s = port.to_s
  end

  # Sends a raw request to Tika
  #
  #   - req: The request object
  #
  # Responds with the body on success, or the failure value on
  # failure.
  def run(uri, req, retries = 6)

    def run_rescue(uri, req, retries, result = nil)
      if retries == 0
        log.error "Failed to run request #{uri}\n result: #{result.inspect}"
        log.debug "Request body for #{uri} was: #{req.body.to_s[0...1024]}"
        if result.kind_of?(Exception)
          raise result
        end
        result
      else
        log.debug "Failed to run request #{uri} retrying (#{retries} left)"
        next_retries = retries - 1
        backoff = (6 - next_retries) ** 2
        sleep backoff
        run(uri, req, next_retries)
      end
    end

    res = Net::HTTP.start(uri.hostname, uri.port) do |http|

      begin
        http.request(req)
      rescue Exception => e
        run_rescue(uri, req, retries, e)
      end
    end

    case res
    when Net::HTTPSuccess, Net::HTTPRedirection
      # Ruby doesn't use the encoding specified in HTTP headers (https://bugs.ruby-lang.org/issues/2567#note-3)
      content_type = res['CONTENT-TYPE']
      log.info "Tika: received type #{content_type}"
      if res.body and content_type and content_type.downcase.include?("charset=utf-8")
        res.body.force_encoding('UTF-8')
      end
      log.debug "Succeeded to run #{req.method} request for #{uri}\n Request body: #{req.body.to_s[0...1024]}\n Response body: #{res.body.to_s[0...1024]}"
      if req.method == "HEAD"
        res
      else
        res.read_body
      end
    when Net::HTTPTooManyRequests
      run_rescue(uri, req, retries, res)
    else
      log.warn "#{req.method} request on #{uri} resulted in response: #{res}"
      log.debug "Request body for #{uri} was: #{req.body.to_s[0...1024]}\n Response: #{res.inspect}"
      res
    end
  end

  def process_document name, document
    uri = URI("http://#{@host}:#{@port_s}/tika")
    req = Net::HTTP::Put.new(uri)
    req['Content-type'] = "application/pdf"
    req['Accept'] = "text/plain"
    req.body = document

    result = run(uri, req)
    # TODO: check could be better, but on success run returns the body
    if result.kind_of?(Net::HTTPResponse) and !result.kind_of?(Net::HTTPSuccess)
      raise "failed to process document #{name}, response was #{result.code} #{result.msg}"
    else
      result
    end
  end

  def extract_metadata name, document
    uri = URI("http://#{@host}:#{@port_s}/meta")
    req = Net::HTTP::Put.new(uri)
    # req['Content-type'] = "application/pdf"
    req['Accept'] = "application/json"
    req.body = document

    result = run(uri, req)
    # TODO: check could be better, but on success run returns the body
    if result.kind_of?(Net::HTTPResponse) and !result.kind_of?(Net::HTTPSuccess)
      raise "failed to process document #{name}, response was #{result.code} #{result.msg}"
    else
      result
    end
  end

end
