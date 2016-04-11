package com.bebo.cassandra;

import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class UserGridDataExtracter {

	private Cluster cluster;
	private Session session;
	
	public void connect(String node) {
		 cluster = Cluster.builder()
		         .addContactPoint(node)
		         .build();
		   Metadata metadata = cluster.getMetadata();
		   System.out.printf("Connected to cluster: %s\n", 
		         metadata.getClusterName());
		   for ( Host host : metadata.getAllHosts() ) {
		      System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
		         host.getDatacenter(), host.getAddress(), host.getRack());
		   }
		   
		   session = cluster.connect();
		
	}
	
	public void close() {
		   cluster.close();
		}
	
	
	
	
	
	public void querySchema(String keyspace,String table){
		List<Row> rowList = getRows(keyspace, table);
		for(Row row:rowList){
			ColumnDefinitions columnDefinitions=row.getColumnDefinitions();
			Iterator<Definition> iterator=columnDefinitions.iterator();
			while(iterator.hasNext()){
				Definition definition = iterator.next();
				System.out.println("columnName:" + definition.getName());
				//System.out.println("columnType:" + definition.getType());
				//System.out.println("columnValue:" + row.getString(definition.getName()));
				if(null != definition.getType() && "uuid".equalsIgnoreCase(definition.getType().toString())){
					System.out.println("columnValue:" + row.getUUID(definition.getName()));
				}
				
				if(null != definition.getType() && "varchar".equalsIgnoreCase(definition.getType().toString())){
					System.out.println("columnValue:" + row.getString(definition.getName()));
				}
			}
		}
	}
	public List<Row> getRows(String keyspace, String table) {
		   Statement statement = QueryBuilder.select().all().from(keyspace, table);
		   return session.execute(statement).all();
		} 
	
	public void querySchema(){
		ResultSet results = session.execute("SELECT * FROM \"Usergrid_Applications\".\"Entity_Log\" LIMIT 2;" );
		
			for (Row row : results) {
				System.out.println("<-------------------------------------------------->");
				ColumnDefinitions columnDefinitions=row.getColumnDefinitions();
				Iterator<Definition> iterator=columnDefinitions.iterator();
				while(iterator.hasNext()){
					Definition definition = iterator.next();
					System.out.println("columnName:" + definition.getName());
					System.out.println("columnType:" + definition.getType());
					//System.out.println("columnValue:" + row.getString(definition.getName()));
					if(null != definition.getType() && "uuid".equalsIgnoreCase(definition.getType().toString())){
						System.out.println("columnValue:" + row.getUUID(definition.getName()));
					}
					
					if(null != definition.getType() && "varchar".equalsIgnoreCase(definition.getType().toString())){
						System.out.println("columnValue:" + row.getString(definition.getName()));
					}
					
					if(null != definition.getType() && "blob".equalsIgnoreCase(definition.getType().toString())){
						System.out.println("columnValue:" + new String( row.getBytes(definition.getName()).array()));
					}
					
					if(null != definition.getType() && "bigint".equalsIgnoreCase(definition.getType().toString())){
						System.out.println("columnValue:" + row.getLong(definition.getName()));
					}
					
					if(null != definition.getType() && "counter".equalsIgnoreCase(definition.getType().toString())){
						System.out.println("columnValue:" + row.getLong(definition.getName()));
					}
					
					if(null != definition.getType() && "varint".equalsIgnoreCase(definition.getType().toString())){
						System.out.println("columnValue:" + row.getVarint(definition.getName()));
					}
				}
			}
			System.out.println();
	}
	public static void main(String[] args) {
		UserGridDataExtracter client = new UserGridDataExtracter();
		   client.connect("192.168.33.70");
		   client.querySchema();
		   client.close();
		}
	
	


}
