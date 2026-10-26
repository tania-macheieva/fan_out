# frozen_string_literal: true

require 'timeout'
require_relative 'fan_out'

RSpec.describe FanOut do
  def build(process, workers)
    described_class.fan_out(process, workers: workers)
  end

  def elapsed
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
  end

  def run_all(pool, tasks)
    tasks.each { |task| pool.submit(task) }
    pool.shutdown
    pool.results.to_a
  end

  describe 'parallelism' do
    let(:slow) { ->(x) { sleep(0.05); x * 2 } }

    it 'is faster with N workers than with one' do
      tasks = (1..8).to_a
      single = elapsed { run_all(build(slow, 1), tasks) }
      parallel = elapsed { run_all(build(slow, 4), tasks) }

      expect(parallel).to be < single / 2
    end

    it 'runs no more than N tasks at the same time, and really in parallel' do
      lock = Mutex.new
      running = 0
      peak = 0
      process = lambda do |_|
        lock.synchronize { running += 1; peak = [peak, running].max }
        sleep(0.03)
        lock.synchronize { running -= 1 }
      end

      run_all(build(process, 3), (1..12).to_a)

      expect(peak).to be_between(2, 3)
    end
  end

  describe 'data safety' do
    it 'processes every task exactly once and returns every result exactly once' do
      lock = Mutex.new
      seen = Hash.new(0)
      process = lambda do |id|
        lock.synchronize { seen[id] += 1 }
        sleep(rand * 0.002)
        id * 10
      end

      results = run_all(build(process, 8), (0...200).to_a)

      expect(seen.keys.sort).to eq((0...200).to_a)
      expect(seen.values.uniq).to eq([1])
      expect(results.map(&:task).sort).to eq((0...200).to_a)
      expect(results.map(&:value).sort).to eq((0...200).map { |i| i * 10 })
    end

    it 'turns an error in process into a failed result and keeps the worker alive' do
      process = ->(x) { x == :bad ? raise('boom') : x }

      results = run_all(build(process, 1), [1, :bad, 2])
      failed = results.reject(&:ok?)

      expect(results.size).to eq(3)
      expect(results.select(&:ok?).map(&:value)).to eq([1, 2])
      expect(failed.map { |r| [r.task, r.error.message] }).to eq([[:bad, 'boom']])
    end
  end

  describe 'graceful shutdown' do
    it 'finishes all accepted tasks before shutdown returns' do
      done = Queue.new
      pool = build(->(x) { sleep(0.03); done << x; x }, 2)
      6.times { |i| pool.submit(i) }

      pool.shutdown

      expect(done.size).to eq(6)
      expect(pool.results.to_a.size).to eq(6)
    end

    it 'rejects new tasks after shutdown' do
      pool = build(->(x) { x }, 2)
      pool.shutdown

      expect { pool.submit(1) }.to raise_error(FanOut::ClosedError)
    end

    it 'closes the results stream after "no more tasks", not before' do
      pool = build(->(x) { x }, 2)
      consumer = Thread.new { pool.results.to_a }

      pool.submit(1).submit(2)
      sleep(0.05)
      expect(consumer).to be_alive

      pool.shutdown
      expect(Timeout.timeout(2) { consumer.value }.map(&:value)).to contain_exactly(1, 2)
    end

    it 'shuts down cleanly when no task was submitted' do
      pool = build(->(x) { x }, 3)

      expect(Timeout.timeout(2) { pool.shutdown.results.to_a }).to eq([])
    end

    it 'can be called more than once' do
      pool = build(->(x) { x }, 2)
      pool.submit(1)

      expect { 2.times { pool.shutdown } }.not_to raise_error
    end
  end

  describe 'constructor' do
    it 'rejects a non-positive number of workers' do
      expect { build(->(x) { x }, 0) }.to raise_error(ArgumentError)
    end
  end
end
