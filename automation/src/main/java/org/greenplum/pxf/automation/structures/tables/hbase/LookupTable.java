package org.greenplum.pxf.automation.structures.tables.hbase;

import java.util.ArrayList;

import io.qameta.allure.Step;
import org.apache.hadoop.hbase.client.Put;

public class LookupTable extends HBaseTable {

	public LookupTable() {
		super("pxflookup", new String[] { "mapping" });
	}

	@Step("Add mapping")
	public void addMapping(String hbaseTable, String pxfAlias, String toQualifier) {

		if (rowsToGenerate == null) {
			rowsToGenerate = new ArrayList<>();
		}

		Put hbasePut = new Put(hbaseTable.getBytes());

		hbasePut.addColumn(getFields()[0].getBytes(), pxfAlias.getBytes(), toQualifier.getBytes());

		rowsToGenerate.add(hbasePut);
	}
}
