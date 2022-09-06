# Monkeypatching Ruby Logger class to take log scope into account
# See also https://ruby-doc.org/stdlib-2.7.0/libdoc/logger/rdoc/Logger.html#method-i-add
class Logger
  def add(severity, message = nil, progname = nil)
    severity ||= UNKNOWN
    if @logdev.nil?
      return true
    end
    if progname.nil?
      progname = @progname
    end
    # Monkey patch: take log level per scope into account
    unless in_scope? progname, severity
      return true
    end
    if message.nil?
      if block_given?
        message = yield
      else
        message = progname
        progname = @progname
      end
    end
    @logdev.write(
      format_message(format_severity(severity), Time.now, progname, message))
    true
  end

  def in_scope?(progname, severity)
    if progname
      severity >= scope_log_level(progname)
    else
      severity >= level
    end
  end

  def scope_log_level(progname)
    @scope_levels = {} if @scope_levels.nil?
    if @scope_levels[progname].nil? # put value in scope_levels cache
      scope = progname.to_s.upcase.gsub(/\s+/, "_")
      env_var = "LOG_SCOPE_#{scope}"
      @scope_levels[progname] = string_to_log_level ENV[env_var]
    end
    @scope_levels[progname]
  end

  def string_to_log_level(level_s)
    level_const = level # fallback to default general log level
    if level_s
      level_s = level_s.upcase
      if ['UNKNOWN', 'FATAL', 'ERROR', 'WARN', 'INFO', 'DEBUG'].include? level_s
        level_const = Kernel.const_get("Logger::#{level_s}")
      end
    end
    level_const
  end
end
