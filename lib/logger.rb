# Monkeypatching Ruby Logger class to take log scope into account
# See also https://ruby-doc.org/stdlib-2.7.0/libdoc/logger/rdoc/Logger.html#method-i-add
class Logger
  def add(severity, message = nil, progname = nil)
    severity ||= UNKNOWN
    if @logdev.nil? or severity < level
      return true
    end
    if progname.nil?
      progname = @progname
    end
    if message.nil?
      if block_given?
        message = yield
      else
        message = progname
        progname = @progname
      end
    end
    # Monkey patch: taking scope into account
    unless in_scope progname
      return true
    end
    @logdev.write(
      format_message(format_severity(severity), Time.now, progname, message))
    true
  end

  def in_scope progname
    if progname
      @scopes = {} if @scopes.nil?
      if @scopes[progname].nil?
        scope = progname.to_s.upcase().gsub(/\s+/, "_")
        env_var = "LOG_SCOPE_#{scope}"
        @scopes[progname] = true? ENV[env_var]
      end
      @scopes[progname]
    else
      true
    end
  end

  def true? obj
    !obj.nil? && (obj.to_s.downcase == "true" || obj.to_s == "1")
  end
end
