#!/usr/bin/ruby
# frozen_string_literal: true

require 'mysql2'
require 'pg'
require 'csv'

class DBConnection
  def pg_connect
    PG.connect( :hostaddr=>"localhost", :port=>5432, 
      :dbname=>"database", :user=>"postgres_user", :password=>'postgres_password')
  end
  
  def mysql_connect
    Mysql2::Client.new(:host => "localhost", :username => "mysql_user",
      :password => "mysql_password", :database => "database")
  end


  def copy_data(mysql_con, pg_con, old_table, new_table)

    query_str = "SELECT * FROM "+old_table.to_s
    mysql_rs = mysql_con.query(query_str)

    tables = CommonUtils.get_column_list
    table_columns = tables[new_table.to_sym]

    data = Array.new
    mysql_rs.each do  |row, key|
      data.push(CommonUtils.get_query_values(row, new_table))
    end

    enco = PG::TextEncoder::CopyRow.new
    copy_statement = 'COPY ' + new_table + '(' + (table_columns.keys).join(",") +  ') FROM STDIN'
    
    pg_con.copy_data copy_statement, enco do
      puts copy_statement
      data.each do |item|
        pg_con.put_copy_data item
      end
    end
    

  end
end

class CommonUtils

  def self.get_query(table_name)
    tables = get_column_list
    table_columns = tables[table_name.to_sym]
  
    query = 'INSERT INTO ' + table_name + ' (' + (table_columns.keys).join(",") + ')'

    query += ' VALUES(' +(Array(1..table_columns.length).map{|v| (v.to_s).prepend('$')}).join(",") + ")"

    puts query 
    return query 

  end

  def self.get_query_values(row, table_name)
    data = Array.new
    
    (self.get_column_list()[table_name.to_sym]).each do |key, value|      
      item = row[value]
      if !item
          item = nil   
      end
      data.push(item)
    end

    return data    
  end

  def self.get_column_list()
     {
       postgresql_table_1:  {
        postgresql_table_1_id: 'mysql_table_1_id',
        postgresql_table_1_column_1: 'mysql_table_1_column_1',
        postgresql_table_1_column_2: 'mysql_table_1_column_2',
        postgresql_table_1_id: 'mysql_table_1_id',
       },
       postgresql_table_2:  {
         postgresql_table_2_id: 'mysql_table_2_id',
         postgresql_table_2_column_1: 'mysql_table_2_column_1',
         postgresql_table_2_column_2: 'mysql_table_2_column_2',
         postgresql_table_2_id: 'mysql_table_2_id',
        }
    }

  end

  def self.get_tables_list
    {
      mysql_table_1: 'postgres_table_1',
      mysql_table_2: 'postgres_table_2',
      mysql_table_3: 'postgres_table_3',
    }

  end

end

begin
  db = DBConnection.new
  pg_con = db.pg_connect
  mysql_con = db.mysql_connect


  (CommonUtils.get_tables_list).each do |mysql_table, postgresql_table|   
    
    pg_con.exec('TRUNCATE TABLE ' + postgresql_table + ' CASCADE')
    db.copy_data(mysql_con, pg_con, mysql_table, postgresql_table)
    # Updating sequence number because postgresql doesn't update it when using raw query.
    pg_con.exec('SELECT setval(\'' + postgresql_table + '_id_seq\', (SELECT MAX(id) FROM '+ postgresql_table + '))')

  end
rescue Exception => e
  puts "caught exception #{e}! ohnoes!"
  pg_con.close
  mysql_con.close  
end