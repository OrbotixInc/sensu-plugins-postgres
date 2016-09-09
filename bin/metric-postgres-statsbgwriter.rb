#! /usr/bin/env ruby
#
#   metric-postgres-statsbgwriter
#
# DESCRIPTION:
#
#   This plugin collects postgres database bgwriter metrics
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
#   ./metric-postgres-statsbgwriter.rb -u db_user -p db_pass -h db_host -d db
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
                           dbname: 'postgres',
                           user: config[:user],
                           password: config[:password],
                           connect_timeout: config[:timeout])
    end

    request = [
      'select checkpoints_timed, checkpoints_req,',
      'buffers_checkpoint, buffers_clean,',
      'maxwritten_clean, buffers_backend,',
      'buffers_alloc',
      'from pg_stat_bgwriter'
    ]
    con.exec(request.join(' ')) do |result|
      result.each do |row|
        output "#{config[:scheme]}.bgwriter.checkpoints_timed", row['checkpoints_timed'], timestamp
        output "#{config[:scheme]}.bgwriter.checkpoints_req", row['checkpoints_req'], timestamp
        output "#{config[:scheme]}.bgwriter.buffers_checkpoint", row['buffers_checkpoint'], timestamp
        output "#{config[:scheme]}.bgwriter.buffers_clean", row['buffers_clean'], timestamp
        output "#{config[:scheme]}.bgwriter.maxwritten_clean", row['maxwritten_clean'], timestamp
        output "#{config[:scheme]}.bgwriter.buffers_backend", row['buffers_backend'], timestamp
        output "#{config[:scheme]}.bgwriter.buffers_alloc", row['buffers_alloc'], timestamp
      end
    end

    ok
  end
end
