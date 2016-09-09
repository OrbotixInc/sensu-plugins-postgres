#! /usr/bin/env ruby
#
#   metric-postgres-locks
#
# DESCRIPTION:
#
#   This plugin collects postgres database lock metrics
#
# OUTPUT:
#   metric data
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#   gem: pg
#
# USAGE:
#   ./metric-postgres-locks.rb -u db_user -p db_pass -h db_host -d db
#
# NOTES:
#
# LICENSE:
#   Copyright (c) 2012 Kwarter, Inc <platforms@kwarter.com>
#   Author Gilles Devaux <gilles.devaux@gmail.com>
#   Released under the same terms as Sensu (the MIT license); see LICENSE
#   for details.
#

require 'sensu-plugin/metric/cli'
require 'pg'
require 'socket'

class PostgresStatsDBMetrics < Sensu::Plugin::Metric::CLI::Graphite
  option :connection_string,
         description: 'A postgres connection string to use, overrides any other parameters',
         short: '-c CONNECTION_STRING',
         long:  '--connection CONNECTION_STRING'

  option :user,
         description: 'Postgres User',
         short: '-u USER',
         long: '--user USER'

  option :password,
         description: 'Postgres Password',
         short: '-p PASS',
         long: '--password PASS'

  option :hostname,
         description: 'Hostname to login to',
         short: '-h HOST',
         long: '--hostname HOST',
         default: 'localhost'

  option :port,
         description: 'Database port',
         short: '-P PORT',
         long: '--port PORT',
         default: 5432

  option :database,
         description: 'Database name',
         short: '-d DB',
         long: '--db DB',
         default: 'postgres'

  option :scheme,
         description: 'Metric naming scheme, text to prepend to $queue_name.$metric',
         long: '--scheme SCHEME',
         default: "#{Socket.gethostname}.postgresql"

  option :timeout,
         description: 'Connection timeout (seconds)',
         short: '-T TIMEOUT',
         long: '--timeout TIMEOUT',
         default: nil

  def run
    timestamp = Time.now.to_i

    total_locks_per_type = Hash.new(0)
    per_db_locks = Hash.new{|h, k| h[k] = Hash.new(0)}

    if config[:connection_string]
      con = PG::Connection.new(config[:connection_string])
    else
      con     = PG.connect(host: config[:hostname],
                           dbname: config[:database],
                           user: config[:user],
                           password: config[:password],
                           connect_timeout: config[:timeout])
    end

    request = "SELECT pgd.datname,pgl.mode,count(pgl.mode) FROM pg_locks pgl JOIN pg_database pgd ON pgd.oid = pgl.database group by pgd.datname, pgl.mode;"

    con.exec(request) do |result|
      result.each do |row|
        db_name = row['datname']
        lock_name = row['mode'].downcase.to_sym
        total_locks_per_type[lock_name] += 1
        per_db_locks[db_name][lock_name] += 1
      end
    end

    total_locks_per_type.each do |lock_type, count|
      output "#{config[:scheme]}.locks", count, "#{timestamp} schema=total lock_type=#{lock_type}"
    end

    per_db_locks.each do |database,locks|
      locks.each do |lock_type, count|
        output "#{config[:scheme]}.locks.#{lock_type}", count, "#{timestamp} schema=#{database} lock_type=#{lock_type}"
      end
    end

    ok
  end
end
