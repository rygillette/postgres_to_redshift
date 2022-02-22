require "postgres_to_redshift/version"
require 'pg'
require 'uri'
require 'aws-sdk-s3'
require 'zlib'
require 'tempfile'
require "postgres_to_redshift/table"
require "postgres_to_redshift/column"

class PostgresToRedshift
  class << self
    attr_accessor :source_uri, :target_uri
  end

  attr_reader :source_connection, :target_connection, :s3

  KILOBYTE = 1024
  MEGABYTE = KILOBYTE * 1024
  GIGABYTE = MEGABYTE * 1024

  def self.update_tables
    update_tables = PostgresToRedshift.new

    update_tables.tables.each do |table|
      target_connection.exec("CREATE TABLE IF NOT EXISTS #{schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")
      update_tables.copy_table(table)
      update_tables.import_table(table)
    end
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI'])
  end

  def self.target_uri
    @target_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_TARGET_URI'])
  end

  def self.source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: source_uri.password, dbname: source_uri.path[1..-1])
      @source_connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
    end

    @source_connection
  end

  def self.target_connection
    unless instance_variable_defined?(:"@target_connection")
      @target_connection = PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
    end

    @target_connection
  end

  def self.schema
    ENV.fetch('POSTGRES_TO_REDSHIFT_TARGET_SCHEMA')
  end

  def source_connection
    self.class.source_connection
  end

  def target_connection
    self.class.target_connection
  end

  def tables
    source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE') AND table_name NOT IN (#{ENV['SKIP_TABLES'].split(',')})").map do |table_attributes|
      table = Table.new(attributes: table_attributes)
      next if table.name =~ /^pg_/
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}' AND data_type != 'uuid' order by ordinal_position")
  end

  def s3
    region = ENV['S3_REGION']
    s3 = Aws::S3::Client.new(region: region, credentials: Aws::Credentials.new(ENV['S3_DATABASE_EXPORT_ID'], ENV['S3_DATABASE_EXPORT_KEY']))
  end

  def bucket_objects
    bucket_objects = s3.list_objects_v2(
      bucket: ENV['S3_DATABASE_EXPORT_BUCKET'],
      max_keys: 500
    ).contents
  end

  def object_uploaded?(bucket_name, object_key, object_content)
    response = s3.put_object(
      bucket: bucket_name,
      key: object_key,
      body: object_content
    )
    if response.etag
      return true
    else
      return false
    end
  rescue StandardError => e
    puts "Error uploading object: #{e.message}"
    return false
  end

  def object_deleted?(bucket_name, object_key)
    response = s3.delete_objects(
      bucket: bucket_name,
      delete: {
        objects: [
          {
            key: object_key
          }
        ]
      }
    )
    if response.deleted.count == 1
      return true
    else
      return false
    end
  rescue StandardError => e
    puts "Error deleting object: #{e.message}"
    return false
  end

  def copy_table(table)
    tmpfile = Tempfile.new("psql2rs")
    zip = Zlib::GzipWriter.new(tmpfile)
    chunksize = 1 * GIGABYTE # uncompressed
    chunk = 1
    object_name = "export/#{table.target_table_name}.psv.gz"
    if object_deleted?(ENV['S3_DATABASE_EXPORT_BUCKET'], object_name)
      puts "Object '#{object_name}' deleted."
    else
      puts "Object '#{object_name}' not deleted."
    end
    begin
      puts "Downloading #{table}"
      copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{table.name}) TO STDOUT WITH DELIMITER '|'"

      source_connection.copy_data(copy_command) do
        while row = source_connection.get_copy_data
          zip.write(row)
          if (zip.pos > chunksize)
            zip.finish
            tmpfile.rewind
            upload_table(table, tmpfile, chunk)
            chunk += 1
            zip.close unless zip.closed?
            tmpfile.unlink
            tmpfile = Tempfile.new("psql2rs")
            zip = Zlib::GzipWriter.new(tmpfile)
          end
        end
      end
      zip.finish
      tmpfile.rewind
      upload_table(table, tmpfile, chunk)
      source_connection.reset
    ensure
      zip.close unless zip.closed?
      tmpfile.unlink
    end
  end

  def upload_table(table, buffer, chunk)
    puts "Uploading #{table.target_table_name}.#{chunk}"
    object_name = "export/#{table.target_table_name}.psv.gz.#{chunk}"
    if object_uploaded?(ENV['S3_DATABASE_EXPORT_BUCKET'], object_name, buffer)
      puts "Object '#{object_name}' uploaded."
    else
      puts "Object '#{object_name}' uploaded. " \
        'Program will stop.'
      exit 1
    end
  end

  def import_table(table)
    puts "Importing #{table.target_table_name}"
    schema = self.class.schema
    
    target_connection.exec("DROP TABLE IF EXISTS #{schema}.#{table.target_table_name}_updating")

    target_connection.exec("BEGIN;")

    target_connection.exec("ALTER TABLE #{schema}.#{target_connection.quote_ident(table.target_table_name)} RENAME TO #{table.target_table_name}_updating")

    target_connection.exec("CREATE TABLE #{schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

    target_connection.exec("COPY #{schema}.#{target_connection.quote_ident(table.target_table_name)} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

    target_connection.exec("COMMIT;")
  end
end
