#! /usr/bin/env ruby
#
#   metric-postgres-connections
#
# DESCRIPTION:
#
#   This plugin collects postgres connection metrics
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
#   ./metric-postgres-connections.rb -u db_user -p db_pass -h db_host -d db
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

    if config[:connection_string]
      con = PG::Connection.new(config[:connection_string])
    else
      con     = PG.connect(host: config[:hostname],
                           dbname: config[:database],
                           user: config[:user],
                           password: config[:password],
                           connect_timeout: config[:timeout])
    end

    request = [
      'select datname, count(*), waiting from pg_stat_activity',
      "group by datname, waiting"
    ]

    metrics = Hash.new{|h, k| h[k] ={
      active: 0,
      waiting: 0,
      total: 0
    }}

    con.exec(request.join(' ')) do |result|
      result.each do |row|
        if row['waiting'] == 't'
          metrics[row['datname']][:waiting] = row['count']
        elsif row['waiting'] == 'f'
          metrics[row['datname']][:active] = row['count']
        end
      end
    end

    metrics.each do |datname,metric|
      metric[:total] = (metric[:waiting].to_i + metric[:active].to_i)
    end

    metrics.each do |datname, metric|
      metric.each do |metric,value|
        output "#{config[:scheme]}.connections.#{metric}", value, "#{timestamp} schema=#{datname}"
      end
    end

    ok
  end
end
