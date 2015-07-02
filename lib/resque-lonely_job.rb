require 'resque-lonely_job/version'

module Resque
  module Plugins
    module LonelyJob
      LOCK_TIMEOUT = 60 * 60 * 24 * 5 # 5 days

      def requeue_interval
        self.instance_variable_get(:@requeue_interval) || 1
      end

      # Overwrite this method to uniquely identify which mutex should be used
      # for a resque worker.
      def redis_key(*args)
        "lonely_job:#{@queue}"
      end

      def can_lock_queue?(*args)
        key = redis_key(*args)

        # Per http://redis.io/commands/set
        Resque.redis.set key, 'anystring', nx: true, ex: LOCK_TIMEOUT
      end

      def unlock_queue(*args)
        Resque.redis.del(redis_key(*args))
      end

      def reenqueue(*args)
        Resque.enqueue(self, *args)
      end

      def before_perform(*args)
        unless can_lock_queue?(*args)
          # Sleep so the CPU's rest
          sleep(requeue_interval)

          # can't get the lock, so re-enqueue the task
          reenqueue(*args)

          # and don't perform
          raise Resque::Job::DontPerform
        end
      end

      def around_perform(*args)
        begin
          yield
        ensure
          unlock_queue(*args)
        end
      end
    end
  end
end
