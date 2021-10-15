require 'consul/async/utilities'
require 'consul/async/stats'
require 'em-http'
require 'json'
require 'async'
require 'async/http/internet/instance'
require 'consul/async/debug'

module Consul
  module Async
    # The class configuring Consul endpoints
    # It allows to translate configuration options per specific endpoint/path
    class ConsulConfiguration
      attr_reader :base_url, :token, :retry_duration, :min_duration, :wait_duration, :max_retry_duration, :retry_on_non_diff,
                  :missing_index_retry_time_on_diff, :missing_index_retry_time_on_unchanged, :debug, :enable_gzip_compression,
                  :fail_fast_errors, :max_consecutive_errors_on_endpoint, :tls_cert_chain, :tls_private_key, :tls_verify_peer
      def initialize(base_url: 'http://localhost:8500',
                     debug: { network: false },
                     token: nil,
                     retry_duration: 10,
                     min_duration: 0.1,
                     retry_on_non_diff: 5,
                     wait_duration: 600,
                     max_retry_duration: 600,
                     missing_index_retry_time_on_diff: 15,
                     missing_index_retry_time_on_unchanged: 60,
                     enable_gzip_compression: true,
                     paths: {},
                     max_consecutive_errors_on_endpoint: 10,
                     fail_fast_errors: 1,
                     tls_cert_chain: nil,
                     tls_private_key: nil,
                     tls_verify_peer: true)
        @base_url = base_url
        @token = token
        @debug = debug
        @enable_gzip_compression = enable_gzip_compression
        @retry_duration = retry_duration
        @min_duration = min_duration
        @wait_duration = wait_duration
        @max_retry_duration = max_retry_duration
        @retry_on_non_diff = retry_on_non_diff
        @missing_index_retry_time_on_diff = missing_index_retry_time_on_diff
        @missing_index_retry_time_on_unchanged = missing_index_retry_time_on_unchanged
        @paths = paths
        @max_consecutive_errors_on_endpoint = max_consecutive_errors_on_endpoint
        @fail_fast_errors = fail_fast_errors
        @tls_cert_chain = tls_cert_chain
        @tls_private_key = tls_private_key
        @tls_verify_peer = tls_verify_peer
      end

      def ch(path, symbol)
        sub = @paths[path.to_sym]
        if sub && sub[symbol]
          ::Consul::Async::Debug.puts_debug "Overriding #{symbol}:=#{sub[symbol]} for #{path}"
          sub[symbol]
        else
          method(symbol).call
        end
      end

      def create(path, agent: nil)
        return self unless @paths[path.to_sym]

        base_url = ch(path, :base_url)
        if agent
          agent = "http://#{agent}" unless agent.start_with? 'http', 'https'
          base_url = agent
        end
        ConsulConfiguration.new(base_url: base_url,
                                debug: ch(path, :debug),
                                token: ch(path, :token),
                                retry_duration: ch(path, :retry_duration),
                                min_duration: ch(path, :min_duration),
                                retry_on_non_diff: ch(path, :retry_on_non_diff),
                                wait_duration: ch(path, :wait_duration),
                                max_retry_duration: ch(path, :max_retry_duration),
                                missing_index_retry_time_on_diff: ch(path, :missing_index_retry_time_on_diff),
                                missing_index_retry_time_on_unchanged: ch(path, :missing_index_retry_time_on_unchanged),
                                enable_gzip_compression: enable_gzip_compression,
                                paths: @paths,
                                max_consecutive_errors_on_endpoint: @max_consecutive_errors_on_endpoint,
                                fail_fast_errors: @fail_fast_errors,
                                tls_cert_chain: ch(path, :tls_cert_chain),
                                tls_private_key: ch(path, :tls_private_key),
                                tls_verify_peer: ch(path, :tls_verify_peer))
      end
    end

    # This keep track of answer from Consul
    # It also keep statistics about result (x_consul_index, stats...)
    class ConsulResult
      attr_reader :data
      # @return [HttpResponse]
      attr_reader :http
      attr_reader :x_consul_index, :last_update, :stats, :retry_in
      def initialize(data, modified, http, x_consul_index, stats, retry_in, fake: false)
        @data = data
        @modified = modified
        @http = http
        @x_consul_index = x_consul_index
        @last_update = Time.now.utc
        @stats = stats
        @retry_in = retry_in
        @fake = fake
      end

      def fake?
        @fake
      end

      def modified?
        @modified
      end

      def mutate(new_data)
        @data = new_data.dup
        @data_json = nil
      end

      def json
        @data_json = JSON.parse(data) if @data_json.nil?
        @data_json
      end
    end
    # Basic Encapsulation of HTTP response from Consul
    # It supports empty responses to handle first call is an easy way
    class HttpResponse
      attr_reader :response_header, :response, :error

      # @param [Protocol::HTTP::Response, nil] response
      # @param [Exception, nil] error
      def initialize(response = nil, error = nil, override_nil_response = nil)
        @response_header = response&.headers&.dup&.freeze
        @response = response&.body? ? response&.body&.join&.freeze : override_nil_response
        @error = error&.dup&.freeze
      end
    end
    # This class represents a specific path in Consul HTTP API
    # It also stores x_consul_index and keep track on updates of API
    # So, it basically performs all the optimizations to keep updated with Consul internal state.
    class ConsulEndpoint
      # @return [ConsulConfiguration]
      attr_reader :conf
      attr_reader :path, :x_consul_index, :stats, :last_result, :enforce_json_200, :start_time, :default_value, :query_params
      def initialize(conf, path, enforce_json_200 = true, query_params = {}, default_value = '[]', agent = nil)
        @conf = conf.create(path, agent: agent)
        @default_value = default_value
        @path = path
        @x_consul_index = 0
        @s_callbacks = []
        @e_callbacks = []
        @enforce_json_200 = enforce_json_200
        @start_time = Time.now.utc
        @consecutive_errors = 0
        @query_params = query_params
        @stopping = false
        @stats = EndPointStats.new
        @last_result = ConsulResult.new(default_value, false, HttpResponse.new, 0, stats, 1, fake: true)
        on_response { |result| @stats.on_response result }
        on_error { |http| @stats.on_error http }
        _enable_network_debug if conf.debug && conf.debug[:network]
        Async { |task| fetch(task) }
      end

      def inspect
        "<ConsulEndpoint:#{object_id} #{path} idx:#{x_consul_index}>"
      end

      def _enable_network_debug
        on_response do |result|
          state = result.x_consul_index.to_i < 1 ? '[WARN]' : '[ OK ]'
          stats = result.stats
          warn "[DBUG]#{state}#{result.modified? ? '[MODFIED]' : '[NO DIFF]'}" \
          "[s:#{stats.successes},err:#{stats.errors}]" \
          "[#{stats.body_bytes_human.ljust(8)}][#{stats.bytes_per_sec_human.ljust(9)}]"\
          " #{path.ljust(48)} idx:#{result.x_consul_index}, next in #{result.retry_in} s"
        end
        on_error { |error| warn "[ERROR]: #{path}: #{error.inspect}" }
      end

      def on_response(&block)
        @s_callbacks << block
      end

      def on_error(&block)
        @e_callbacks << block
      end

      def ready?
        @ready
      end

      def terminate
        @stopping = true
        @task&.stop
      end

      private

      def build_request(consul_index)
        res = {
          head: {
            'Accept' => 'application/json',
            'X-Consul-Index' => consul_index,
            'X-Consul-Token' => conf.token
          },
          path: path,
          query: {
            wait: "#{conf.wait_duration}s",
            index: consul_index,
            stale: 'stale'
          },
          keepalive: true,
          callback: method(:on_response)
        }
        res[:head]['accept-encoding'] = 'identity' unless conf.enable_gzip_compression
        @query_params.each_pair do |k, v|
          res[:query][k] = v
        end
        res
      end

      # @param [::Protocol::HTTP::Response] http
      def find_x_consul_index(http)
        http.headers['x-consul-index']&.first
      end

      def _compute_retry_in(retry_in)
        retry_in / 2 + Consul::Async::Utilities.random.rand(retry_in)
      end

      # rubocop:disable Style/ClassVars
      def _last_429
        @@_last_429 ||= { count: 0 }
      end
      # rubocop:enable Style/ClassVars

      # @param [Protocol::HTTP::Response, nil] response
      # @param [Exception, nil] error
      def _handle_error(response, consul_index, error = nil)
        retry_in = _compute_retry_in([600, conf.retry_duration + 2**@consecutive_errors].min)
        if response&.status == 429
          _last_429
          retry_in = 60 + Consul::Async::Utilities.random.rand(180) if retry_in < 60
          _last_429[:time] = Time.now.utc
          _last_429[:count] += 1
          if (_last_429[:count] % 10) == 1
            if _last_429[:count] == 1
              ::Consul::Async::Debug.puts_error "Rate limiting detected on Consul side (HTTP 429)!\n\n" \
                                                "******************************* CONFIGURATION ISSUE DETECTED *******************************\n" \
                                                "* Too many simultaneous connections for Consul agent #{conf.base_url}\n" \
                                                "* You should tune 'limits.http_max_conns_per_client' to a higher value.\n" \
                                                "* This program will behave badly until you change this.\n" \
                                                "* See https://www.consul.io/docs/agent/options.html#http_max_conns_per_client for more info\n" \
                                                "********************************************************************************************\n\n"
            end
            ::Consul::Async::Debug.puts_error "[#{path}] Too many conns to #{conf.base_url}, errors=#{_last_429[:count]} - Retry in #{retry_in}s #{stats.body_bytes_human}"
          end
        else
          ::Consul::Async::Debug.puts_error "[#{path}] X-Consul-Index:#{consul_index} - #{error} - Retry in #{retry_in}s #{stats.body_bytes_human}"
        end
        @consecutive_errors += 1

        @task.async do
          http_result = HttpResponse.new(response, error)
          @e_callbacks.each { |c| c.call(http_result) }
        end
        @task.sleep(retry_in)
        yield
      end

      def fetch(task)
        @task = task
        consul_index = @x_consul_index
        options = {
          connect_timeout: 5, # default connection setup timeout
          inactivity_timeout: conf.wait_duration + 1 + (conf.wait_duration / 16) # default connection inactivity (post-setup) timeout
        }
        opts = build_request(consul_index)
        uri = URI.parse(conf.base_url)
        path = opts[:path]
        path = "/#{path}" unless path.start_with?('/')
        uri.path = path
        uri.query = URI.encode_www_form(opts[:query].to_a)
        i = ::Async::HTTP::Internet.instance
        unless conf.tls_cert_chain.nil?
          # @type [OpenSSL::SSL::SSLContext]
          ssl = OpenSSL::SSL::SSLContext.new
          ssl.key = OpenSSL::PKey.read(File.read(conf.tls_private_key))
          ssl.cert = OpenSSL::X509::Certificate.new(File.read(conf.tls_cert_chain))
          ssl.verify_mode = conf.tls_verify_peer ?
                              OpenSSL::SSL::VERIFY_PEER :
                              OpenSSL::SSL::VERIFY_NONE
          i.client_for(::Async::HTTP::Endpoint.parse(uri, ssl_context: ssl))
        end

        until @stopping
          begin
            # @type [::Async::HTTP::Protocol::HTTP2::Response]
            response = i.call('GET', uri.to_s, opts[:head].transform_values(&:to_s))

            # Dirty hack, but contrary to other path, when key is not present, Consul returns 404
            is_kv_empty = path.start_with?('/v1/kv') && response.status == 404
            if !is_kv_empty && enforce_json_200 && response.status != 200 && response.headers['content-type']&.first != 'application/json'
              _handle_error(response, consul_index, nil) do
                warn "[RETRY][#{path}] (#{@consecutive_errors} errors)" if (@consecutive_errors % 10) == 1
              end
            else
              n_consul_index = find_x_consul_index(response)
              @x_consul_index = n_consul_index.to_i if n_consul_index
              @consecutive_errors = 0
              http_result = if is_kv_empty
                              HttpResponse.new(response, nil, default_value)
                            else
                              HttpResponse.new(response)
                            end
              new_content = http_result.response.freeze
              modified = @last_result.fake? || @last_result.data != new_content
              if n_consul_index.nil?
                retry_in = modified ? conf.missing_index_retry_time_on_diff : conf.missing_index_retry_time_on_unchanged
                n_consul_index = consul_index
              else
                retry_in = modified ? conf.min_duration : conf.retry_on_non_diff
                retry_in = 0.1 if retry_in < (Time.now - @last_result.last_update)
              end
              retry_in = _compute_retry_in(retry_in)
              retry_in = 0.1 if retry_in < 0.1
              result = ConsulResult.new(new_content, modified, http_result, n_consul_index, stats, retry_in, fake: false)
              @last_result = result
              @ready = true
              @task.async do
                @s_callbacks.each { |c| c.call(result) }
              end
              consul_index = n_consul_index
              @task.sleep(retry_in) unless @stopping
            end
          rescue StandardError => e
            unless @stopping
              _handle_error(response, consul_index, e) do
                if (@consecutive_errors % 10) == 1
                  add_msg = e
                  if Gem.win_platform? && e.include?('unable to create new socket: Too many open files')
                    add_msg += "\n *** Windows does not support more than 2048 watches, watch less endpoints ***"
                  end
                  ::Consul::Async::Debug.puts_error "[RETRY][#{path}] (#{@consecutive_errors} errors) due to #{add_msg}"
                end
              end
            end
          end
        end
      end
    end
  end
end
