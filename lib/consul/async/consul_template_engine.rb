require 'consul/async/utilities'
require 'consul/async/consul_endpoint'
# require 'consul/async/json_endpoint'
require 'consul/async/vault_endpoint'
require 'consul/async/consul_template'
require 'consul/async/consul_template_render'
require 'em-http'
require 'erb'
module Consul
  module Async
    # The Engine keeps tracks of all templates, handle hot-reload of files if needed
    # as well as ticking the clock to compute state of templates periodically.
    class ConsulTemplateEngine
      # @return [EndPointsManager]
      attr_reader :template_manager
      attr_reader :hot_reload_failure, :template_frequency, :debug_memory, :result, :templates
      attr_writer :hot_reload_failure, :template_frequency, :debug_memory
      def initialize
        @templates = []
        @template_callbacks = []
        @hot_reload_failure = 'die'
        @all_templates_rendered = false
        @template_frequency = 1
        @periodic_started = false
        @debug_memory = false
        @result = 0
        @last_memory_state = build_memory_info
        @start = Time.now
      end

      def build_memory_info
        s = GC.stat
        {
          pages: s[:total_allocated_pages] - s[:total_freed_pages],
          objects: s[:total_allocated_objects] - s[:total_freed_objects],
          time: Time.now.utc
        }
      end

      def add_template_callback(&block)
        @template_callbacks << block
      end

      def add_template(source, dest, params = {})
        @templates.push([source, dest, params])
      end

      # Run templating engine once
      def do_run(template_manager, template_renders)
        unless template_manager.running
          ::Consul::Async::Debug.puts_info '[FATAL] TemplateManager has been stopped, stopping everything'
          @result = 3
          template_manager.terminate
          return
        end
        results = template_renders.map(&:run)
        all_ready = results.all?(&:ready?)
        if !@all_templates_rendered && all_ready
          @all_templates_rendered = true
          cur_time = Time.now
          ::Consul::Async::Debug.puts_info "First rendering of #{results.count} templates completed in #{cur_time - @start}s at #{cur_time}.  "
        end
        begin
          @template_callbacks.each do |c|
            c.call([all_ready, template_manager, results])
          end
        rescue StandardError => e
          ::Consul::Async::Debug.puts_error "callback error: #{e.inspect}"
          raise e
        end
      rescue Consul::Async::InvalidTemplateException => e
        warn "[FATAL]#{e}"
        @result = 1
        template_manager.terminate
      rescue StandardError => e
        warn "[FATAL] Error occured: #{e.inspect} - #{e.backtrace.join("\n\t")}"
        @result = 2
        template_manager.terminate
      end

      # Run template engine as fast as possible until first rendering occurs
      def do_run_fast(template_manager, template_renders)
        do_run(template_manager, template_renders)

        return if @all_templates_rendered || @periodic_started

        # We continue if rendering not done and periodic not started
        do_run_fast(template_manager, template_renders)
      end

      def run(template_manager)
        @template_manager = template_manager
        Async do
          template_renders = @templates.map do |template_file, output_file, params|
            Consul::Async::ConsulTemplateRender.new(
              template_manager, template_file, output_file,
              hot_reload_failure: hot_reload_failure,
              params: params
            )
          end
          # Initiate first run immediately to speed up rendering
          do_run_fast(template_manager, template_renders)
          Async do
            loop do
              @periodic_started = true
              do_run(template_manager, template_renders)
              if debug_memory
                GC.start
                new_memory_state = build_memory_info
                diff_allocated = new_memory_state[:pages] - @last_memory_state[:pages]
                diff_num_objects = new_memory_state[:objects] - @last_memory_state[:objects]
                if diff_allocated != 0 || diff_num_objects.abs > (@last_memory_state[:pages] / 3)
                  timediff = new_memory_state[:time] - @last_memory_state[:time]
                  warn "[MEMORY] #{new_memory_state[:time]} significant RAM Usage detected\n" \
                            "[MEMORY] #{new_memory_state[:time]} Pages  : #{new_memory_state[:pages]}" \
                            " (diff #{diff_allocated} aka #{(diff_allocated / timediff).round(0)}/s) \n" \
                            "[MEMORY] #{new_memory_state[:time]} Objects: #{new_memory_state[:objects]}"\
                            " (diff #{diff_num_objects} aka #{(diff_num_objects / timediff).round(0)}/s)"
                  @last_memory_state = new_memory_state
                end
              end
              sleep template_frequency
            end
          end
        end
        @result
      end
    end
  end
end
