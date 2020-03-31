#!/usr/bin/ruby
# frozen_string_literal: true

require 'mysql2'
require 'pg'

class DBConnection
  def pg_connect
    PG.connect( :hostaddr=>"localhost", :port=>5432, 
      :dbname=>"database", :user=>"postgres_user", :password=>'postgres_password')
  end
  
  def mysql_connect
    Mysql2::Client.new(:host => "localhost", :username => "mysql_user",
      :password => "mysql_password", :database => "database")
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
      item = fetch_value_from_row(row, value)
   
      if !item
          item = nil   
      end
      
      data.push(item)
    end

    return data    
  end

  def self.fetch_value_from_row(row, key)
    row[key]
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
  i = 0

  (CommonUtils.get_tables_list).each do |mysql_table, postgresql_table|   

    pg_con.exec('TRUNCATE TABLE ' + postgresql_table + ' CASCADE')
    query_str = "SELECT * FROM "+mysql_table.to_s
    mysql_rs = mysql_con.query(query_str)

    pg_con.prepare("inse#{i}",CommonUtils.get_query(postgresql_table))
    mysql_rs.each do  |row, key|
      pg_con.exec_prepared("inse#{i}",CommonUtils.get_query_values(row, postgresql_table))
    end
    
    i+=1
  end
rescue Exception => e
  puts "caught exception #{e}! ohnoes!"
  pg_con.close
  mysql_con.close  
end