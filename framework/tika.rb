class Tika
  def initialize(host: "tika", port: 9998, logger:)
    @host = host
    @port = port
    @port_s = port.to_s
    @logger = logger
  end

  # Extracts text from the given blob with the given mime_type using Tika.
  #
  # Returns the text content as string on success
  # Returns nil on failure due to 'known' errors (eg. encrypted files)
  # Raises an exception on unexpected errors
  def extract_text(file_path, blob)
    mime_type = determine_mime_type file_path, blob

    uri = URI("http://#{@host}:#{@port_s}/tika")
    req = Net::HTTP::Put.new(uri)
    req["Accept"] = "text/plain"
    req["Content-Type"] = mime_type unless mime_type.nil?
    req.body = blob
    resp = run(uri, req)

    if resp.is_a? Net::HTTPSuccess
      resp.body
    elsif resp.is_a? Net::HTTPUnprocessableEntity
      @logger.warn("TIKA") { "Tika returned [#{resp.code} #{resp.msg}] to extract text for file #{file_path}. The file may be encrypted. Check the Tika logs for additional info." }
      nil
    else
      @logger.error("TIKA") { "Failed to extract text for file #{file_path}.\nPUT #{uri}\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
      raise "Tika returned [#{resp.code} #{resp.msg}] to extract text for file #{file_path}. Check the Tika logs for additional info."
    end
  end

  # Extracts metadata from the given blob using Tika.
  #
  # Returns the metadata as JSON on success
  # Returns nil on failure due to 'known' errors (eg. encrypted files)
  # Raises an exception on unexpected errors
  def extract_metadata(file_path, blob)
    uri = URI("http://#{@host}:#{@port_s}/meta")
    req = Net::HTTP::Put.new(uri)
    req["Accept"] = "application/json"
    req.body = blob
    resp = run(uri, req)

    if resp.is_a? Net::HTTPSuccess
      resp.body
    elsif resp.is_a? Net::HTTPUnprocessableEntity
      @logger.warn("TIKA") { "Tika returned [#{resp.code} #{resp.msg}] to extract metadata for file #{file_path}. The file may be encrypted. Check the Tika logs for additional info." }
      nil
    else
      @logger.error("TIKA") { "Failed to extract metadata for file #{file_path}.\nPUT #{uri}\nResponse: #{resp.code} #{resp.msg}\n#{resp.body}" }
      raise "Tika returned [#{resp.code} #{resp.msg}] to extract text for file #{file_path}. Check the Tika logs for additional info."
    end
  end

  private

  # Determine the mimetype of the given file name and content using Tika
  #
  # Returns a string indicating the mimetype on success.
  # Returns nil on error
  def determine_mime_type(file_path, blob)
    uri = URI("http://#{@host}:#{@port_s}/detect/stream")
    req = Net::HTTP::Put.new(uri)
    req["Content-Disposition"] = "attachment; filename=#{File.basename(file_path)}"
    req.body = blob
    resp = run(uri, req)

    if resp.is_a? Net::HTTPSuccess
      resp.body
    else
      @logger.warn("TIKA") { "Unable to determine mimetype of #{file_path}. Tika returned [#{resp.code} #{resp.msg}]." }
      nil
    end
  end

  # Sends a raw request to Tika with retries on failure
  #
  #   - uri: The URI to send the request to
  #   - req: The request object
  #   - retries: Max number of retries
  #
  # Returns the HTTP response.
  #
  # Note: the method only logs limited info on purpose.
  # Additional logging about the error that occurred
  # is the responsibility of the consumer.
  def run(uri, req, retries = 6)
    def run_rescue(uri, req, retries, result = nil)
      if retries == 0
        if result.is_a? Exception
          @logger.warn("TIKA") { "Failed to run request #{uri}. Max number of retries reached." }
          raise result
        else
          @logger.info("TIKA") { "Failed to run request #{uri}. Max number of retries reached." }
          result
        end
      else
        @logger.info("TIKA") { "Failed to run request #{uri}. Request will be retried (#{retries} left)." }
        next_retries = retries - 1
        backoff = (6 - next_retries)**2
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
    when Net::HTTPSuccess, Net::HTTPRedirection # response code 2xx or 3xx
      # Ruby doesn't use the encoding specified in HTTP headers (https://bugs.ruby-lang.org/issues/2567#note-3)
      content_type = res["CONTENT-TYPE"]
      if res.body && content_type && content_type.downcase.include?("charset=utf-8")
        res.body.force_encoding("utf-8")
      end
      res
    when Net::HTTPTooManyRequests
      run_rescue(uri, req, retries, res)
    else
      @logger.info("TIKA") { "Failed to run request #{uri}" }
      @logger.debug("TIKA") { "Response: #{res.code} #{res.msg}\n#{res.body}" }
      res
    end
  end
end
