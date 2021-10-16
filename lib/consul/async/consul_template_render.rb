require 'consul/async/utilities'
require 'erb'
module Consul
  module Async
    # Result of the rendering of a template
    # .ready? will tell if rendering is full or partial
    class ConsulTemplateRenderedResult
      attr_reader :template_file, :output_file, :hot_reloaded, :ready, :modified, :last_result
      def initialize(template_file, output_file, hot_reloaded, was_success, modified, last_result)
        @template_file = template_file
        @output_file = output_file
        @hot_reloaded = hot_reloaded
        @ready = was_success
        @modified = modified
        @last_result = last_result
      end

      def ready?
        @ready
      end
    end
    # Object handling the whole rendering of a template
    # It stores the input, the output and flags about last result being modified or simply readiness
    # information about whether the template did receive all data to be fully rendered
    class ConsulTemplateRender
      attr_reader :template_file, :output_file, :template_file_ctime, :hot_reload_failure, :params
      # @param [EndPointsManager] template_manager
      def initialize(template_manager, template_file, output_file, hot_reload_failure: 'die', params: {})
        @hot_reload_failure = hot_reload_failure
        @template_file = template_file
        @output_file = output_file
        @template_manager = template_manager
        @last_result = ''
        @last_result = File.read(output_file) if File.exist? output_file
        @template = load_template
        @params = params
        @was_rendered_once = false
      end

      def render(tpl = @template, current_template_info: _build_default_template_info)
        @template_manager.render(tpl, template_file, params, current_template_info: current_template_info)
      end

      def run
        hot_reloaded = hot_reload_if_needed
        was_success, modified, last_result = write
        ConsulTemplateRenderedResult.new(template_file, output_file, hot_reloaded, was_success, modified, last_result)
      end

      private

      def load_template
        @template_file_ctime = File.ctime(template_file)
        File.read(template_file)
      end

      # Will throw Consul::Async::InvalidTemplateException if template invalid
      def update_template(new_template, current_template_info)
        return false unless new_template != @template

        # We render to ensure the template is valid
        render(new_template, current_template_info: current_template_info)
        @template = new_template.freeze
        true
      end

      def _build_default_template_info
        {
          'destination' => @output_file,
          'source_root' => template_file.freeze,
          'source' => template_file.freeze,
          'was_rendered_once' => @was_rendered_once
        }
      end

      def write
        current_template_info = _build_default_template_info
        success, modified, last_res = @template_manager.write(@output_file, @template, @last_result, template_file, params, current_template_info: current_template_info)
        @last_result = last_res if last_res
        @was_rendered_once ||= success
        [success, modified, @last_result]
      end

      def hot_reload_if_needed
        new_time = File.ctime(template_file)
        if template_file_ctime != new_time
          begin
            current_template_info = _build_default_template_info.merge('ready' => false, 'hot_reloading_in_progress' => true)
            @template_file_ctime = new_time
            return update_template(load_template, current_template_info: current_template_info)
          rescue Consul::Async::InvalidTemplateException => e
            warn "****\n[ERROR] HOT Reload of template #{template_file} did fail due to:\n #{e}\n****\n template_info: #{current_template_info}\n****"
            raise e unless hot_reload_failure == 'keep'

            warn "[WARN] Hot reload of #{template_file} was not taken into account, keep running with previous version"
          end
        end
        false
      end
    end
  end
end
