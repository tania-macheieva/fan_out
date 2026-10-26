# frozen_string_literal: true

module FanOut
  class ClosedError < StandardError; end

  # Результат обробки однієї задачі (агрегатор збирає їх у черзі results).
  Result = Struct.new(:task, :ok, :value, :error) do
    def ok?
      ok
    end
  end

  class Pool
    def initialize(process, workers)
      raise ArgumentError, 'workers must be positive' unless workers.positive?

      @process = process
      @tasks = Queue.new
      @results = Queue.new
      @lock = Mutex.new
      @workers = Array.new(workers) { start_worker }
    end

    # Додає задачу в чергу; після shutdown нові задачі не приймаються.
    def submit(task)
      @tasks << [task] # обгортка дозволяє передавати і nil/false як задачу
      self
    rescue ClosedQueueError
      raise ClosedError, 'pool is shut down'
    end

    # Потік результатів у порядку завершення. Закривається після shutdown,
    # коли всі прийняті задачі оброблено.
    def results
      Enumerator.new do |out|
        while (result = @results.pop)
          out << result
        end
      end
    end

    # Graceful shutdown: нових задач не приймає, дообробляє вже прийняті,
    # чекає воркерів і закриває потік результатів.
    def shutdown
      @lock.synchronize do
        @tasks.close
        @workers.each(&:join)
        @results.close
      end
      self
    end

    private

    def start_worker
      Thread.new do
        Thread.current.report_on_exception = false
        # pop повертає nil, коли чергу закрито і вона порожня
        while (item = @tasks.pop)
          @results << handle(item.first)
        end
      end
    end

    def handle(task)
      Result.new(task, true, @process.call(task), nil)
    rescue StandardError => e
      Result.new(task, false, nil, e)
    end
  end

  def self.fan_out(process, workers:)
    Pool.new(process, workers)
  end
end
