package com.bebo.cassandra;

/**
 * @author Amit Verma
 *
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

public class CassandraDataExtracterHector {

	// The string serializer translates the byte[] to and from String using
	// utf-8 encoding
	private static StringSerializer	stringSerializer	= StringSerializer
																.get();
	Cluster							cluster				= null;
	Keyspace						keyspace			= null;

	public void getConfig() {
		cluster = HFactory.getOrCreateCluster("Test Cluster",
				"192.168.33.70:9160");

		// Create a keyspace object from the existing keyspace we created
		// using CLI
		keyspace = HFactory.createKeyspace("USERS", cluster);

	}

	public static void main(String[] args) {
		CassandraDataExtracterHector sample = new CassandraDataExtracterHector();
		sample.getConfig();
	    sample.readObject();
}
	
	public void addStudents(String id,String name){
		CassandraDataExtracterHector sample = new CassandraDataExtracterHector();
		Student student1 = new Student();
		student1.setId(id);
		student1.setName(name);
		List<Student> students = new ArrayList<Student>();
    	students.add(student1);
		System.out.println("Inserting student records....");
		sample.insert(students);
		
	}

	public static void addKeySpace(Cluster cluster, String keySpaceName,
			String columnFamilyName) {
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keySpaceName, columnFamilyName, ComparatorType.BYTESTYPE);

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keySpaceName, ThriftKsDef.DEF_STRATEGY_CLASS, 0,
		Arrays.asList(cfDef));
		// Add the schema to the cluster.
		// "true" as the second param means that Hector will block until all
		// nodes see the change.
		cluster.addKeyspace(newKeyspace, true);
	}

	public void insert(List<Student> studentList) {

		if (null != cluster && null != keyspace) {
			ColumnFamilyDefinition columnFamily = HFactory.createColumnFamilyDefinition("USERS","myColumnFamily", ComparatorType.UTF8TYPE);

			cluster.addColumnFamily(columnFamily);

			StringSerializer stringSerializer = StringSerializer.get();

			Mutator<String> mutator = HFactory.createMutator(keyspace,stringSerializer);

			for (Student student : studentList) {

				Map<String, String> map = processKeyValueMap(student);

				Set set = map.entrySet();

				Iterator i = set.iterator();

				while (i.hasNext()) {

					Map.Entry entry = (Map.Entry) i.next();

					mutator.addInsertion(student.getId(), columnFamily

					.getName(), HFactory.createStringColumn(entry

					.getKey().toString(), entry.getValue().toString()));

					

				}

				mutator.execute();

			}

		}
		System.out.println("Done");

	}

	public Map processKeyValueMap(Student student) {

		HashMap returnMap = new HashMap<String, String>();
		if (null != student.getId() && null != student.getName()) {
			returnMap.put("id", student.getId());
			returnMap.put("name", student.getName());
		}
		return returnMap;

	}
	
	public void readObject() {
		List<String> resultList = new ArrayList<String>();
		RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
				.createRangeSlicesQuery(this.keyspace, StringSerializer.get(),
						StringSerializer.get(), StringSerializer.get());
		rangeSlicesQuery.setColumnFamily("myColumnFamily");
		rangeSlicesQuery.setKeys("", "");
		rangeSlicesQuery.setReturnKeysOnly();

		// rangeSlicesQuery.setRowCount(500);

		QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
		for (Row<String, String, String> row : result.get().getList()) {
			resultList.add(row.getKey());
			System.out.println("keys -->" + row.getKey());

			ColumnFamilyDefinition columnFamily = HFactory.createColumnFamilyDefinition("USERS","myColumnFamily", ComparatorType.UTF8TYPE);

			SliceQuery<String, String, String> query = HFactory

			.createSliceQuery(keyspace, StringSerializer.get(),

			StringSerializer.get(), StringSerializer.get())

			.setKey(row.getKey()).setColumnFamily(columnFamily.getName());

			ColumnSliceIterator<String, String, String> iterator = new ColumnSliceIterator<String, String, String>(query, null, "\u00FFF", false);

			while (iterator.hasNext()) {

				HColumnImpl<String, String> column = (HColumnImpl<String, String>) iterator.next();

				System.out.println(column.getName() + " = " + column.getValue());
			}
			// return resultList;
		}

	}
	

	  
	
}
