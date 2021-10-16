require 'consul/async/utilities'
require 'consul/async/stats'
require 'consul/async/debug'
require 'net/http'
require 'json'

module Consul
  module Async
    # Configuration for Vault Endpoints
    class VaultConfiguration
      attr_reader :base_url, :token, :token_renew, :retry_duration, :min_duration, :wait_duration, :max_retry_duration, :retry_on_non_diff,
                  :lease_duration_factor, :debug, :max_consecutive_errors_on_endpoint, :fail_fast_errors, :tls_cert_chain, :tls_private_key,
                  :tls_verify_peer

      def initialize(base_url: 'http://localhost:8200',
                     debug: { network: false },
                     token: nil,
                     token_renew: true,
                     retry_duration: 10,
                     min_duration: 0.1,
                     lease_duration_factor: 0.5,
                     max_retry_duration: 600,
                     paths: {},
                     max_consecutive_errors_on_endpoint: 10,
                     fail_fast_errors: false,
                     tls_cert_chain: nil,
                     tls_private_key: nil,
                     tls_verify_peer: true)
        @base_url = base_url
        @token_renew = token_renew
        @debug = debug
        @retry_duration = retry_duration
        @min_duration = min_duration
        @max_retry_duration = max_retry_duration
        @lease_duration_factor = lease_duration_factor
        @paths = paths
        @token = token
        @max_consecutive_errors_on_endpoint = max_consecutive_errors_on_endpoint
        @fail_fast_errors = fail_fast_errors
        @tls_cert_chain = tls_cert_chain
        @tls_private_key = tls_private_key
        @tls_verify_peer = tls_verify_peer
      end

      def ch(path, symbol)
        sub = @paths[path.to_sym]
        if sub && sub[symbol]
          ::Consul::Async::Debug.puts_info "Overriding #{symbol}:=#{sub[symbol]} for #{path}"
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
        VaultConfiguration.new(base_url: base_url,
                               debug: ch(path, :debug),
                               token: ch(path, :token),
                               retry_duration: ch(path, :retry_duration),
                               min_duration: ch(path, :min_duration),
                               max_retry_duration: ch(path, :max_retry_duration),
                               lease_duration_factor: ch(path, :lease_duration_factor),
                               paths: @paths,
                               max_consecutive_errors_on_endpoint: @max_consecutive_errors_on_endpoint,
                               fail_fast_errors: @fail_fast_errors)
      end
    end
    # Keep information about Vault result of a query
    class VaultResult
      attr_reader :data, :http, :stats, :retry_in

      def initialize(result, modified, stats, retry_in)
        @data = result.response
        @modified = modified
        @http = result
        @data_json = result.json
        @last_update = Time.now.utc
        @next_update = Time.now.utc + retry_in
        @stats = stats
        @retry_in = retry_in
      end

      def modified?
        @modified
      end

      def mutate(new_data)
        @data = new_data.dup
        @data_json = nil
      end

      def [](path)
        json[path]
      end

      def json
        @data_json = JSON.parse(data) if @data_json.nil?
        @data_json
      end
    end

    # VaultHttpResponse supports empty results (when no data has been received yet)
    class VaultHttpResponse
      attr_reader :response_header, :response, :error, :json

      # @param [Protocol::HTTP::Response, nil] http
      # @param [Exception, nil] error
      def initialize(http = nil, error = nil, override_nil_response = nil)
        @response_header = http&.headers&.dup&.freeze
        @response = http&.body? ? http&.body&.dup&.freeze : override_nil_response
        @error = error&.dup&.freeze
        @json = JSON[response]
      end
    end

    # Endpoint in vault (a path in Vault)
    class VaultEndpoint
      attr_reader :conf, :path, :http_method, :queue, :stats, :last_result, :enforce_json_200, :start_time, :default_value, :query_params

      def initialize(conf, path, http_method = 'GET', enforce_json_200 = true, query_params = {}, default_value = '{}', post_data = {}, agent: nil)
        @conf = conf.create(path, agent: agent)
        @default_value = default_value
        @path = path
        @http_method = http_method
        @x_consul_index = 0
        @s_callbacks = []
        @e_callbacks = []
        @enforce_json_200 = enforce_json_200
        @start_time = Time.now.utc
        @consecutive_errors = 0
        @query_params = query_params
        @post_data = post_data
        @stopping = false
        @stats = EndPointStats.new
        @last_result = VaultResult.new(VaultHttpResponse.new(nil, nil, default_value), false, stats, 1)
        on_response { |result| @stats.on_response result }
        on_error { |http| @stats.on_error http }
        _enable_network_debug if conf.debug && conf.debug[:network]
        fetch
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
        on_error { |http| ::Consul::Async::Debug.puts_error "#{path}: #{http.error}" }
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
      end

      private

      def build_request
        res = {
          head: {
            'Accept' => 'application/json',
            'X-Vault-Token' => conf.token
          },
          query: {},
          path: path,
          keepalive: true,
          callback: method(:on_response)
        }
        # if @post_data
        #   res[:body] = JSON.generate(@post_data)
        # end
        @query_params.each_pair do |k, v|
          res[:query][k] = v
        end
        res
      end

      def get_lease_duration(result)
        result.json['lease_duration'] || conf.min_duration
      end

      def _get_errors(http, error)
        return [error] if error

        http&.json&.fetch('errors', nil) || ['unknown error']
      end

      # @param [VaultHttpResponse] response
      def _handle_error(response, error = nil)
        retry_in = [conf.max_retry_duration, conf.retry_duration + 2**@consecutive_errors].min
        ::Consul::Async::Debug.puts_error "[#{path}][#{http_method}] Code: #{response.status} #{_get_errors(http_result, error).join(' - ')} - Retry in #{retry_in}s"
        @consecutive_errors += 1
        Async {@e_callbacks.each { |c| c.call(http_result) }}
        ::Async::Task.current.sleep retry_in
      end

      def fetch
        until @stopping
          begin
            options = {
              connect_timeout: 5, # default connection setup timeout
              inactivity_timeout: 1 # default connection inactivity (post-setup) timeout
            }
            unless conf.tls_cert_chain.nil?
              options[:tls] = {
                cert_chain_file: conf.tls_cert_chain,
                private_key_file: conf.tls_private_key,
                verify_peer: conf.tls_verify_peer
              }
            end
            opts = build_request
            uri = URI.parse(conf.base_url)
            uri.path = opts[:path]
            uri.query = URI.encode_www_form(opts[:query].to_a)

            # @type [::Async::HTTP::Protocol::HTTP2::Response]
            response = ::Async::HTTP::Internet.instance.call(http_method.upcase, uri.to_s, opts[:head])
            vault_response = VaultHttpResponse.new(response, nil, default_value)
            if enforce_json_200 && ![200, 404].include?(response.status)
              _handle_error(vault_response)
            else
              @consecutive_errors = 0
              modified = @last_result.nil? ? true : @last_result.data != vault_response.response # Leaving it do to stats with this later
              retry_in = (get_lease_duration(vault_response) * conf.lease_duration_factor)
                         .clamp(conf.min_duration, conf.max_retry_duration)
              result = VaultResult.new(vault_response, modified, stats, retry_in)
              @last_result = result
              @ready = true
              Async { @s_callbacks.each { |c| c.call(result) } } unless @stopping
              ::Async::Task.current.sleep retry_in
            end
          rescue StandardError => e
            _handle_error(http, e)
          end
        end
      end
    end
  end
end
