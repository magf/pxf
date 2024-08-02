package org.greenplum.pxf.automation.structures.tables.hive;

import org.apache.commons.lang.StringUtils;

import org.greenplum.pxf.automation.structures.tables.basic.Table;

import java.util.List;

/**
 * Represents Hive Table
 */
public class HiveTable extends Table {

	private String format;
	private String delimiterFieldsBy;
	private String delimiterCollectionItemsBy;
	private String delimiterMapKeysBy;
	private String delimiterLinesBy;
	private String storedAs;
	private String[] partitionedBy;
	private String[] clusteredBy;
	private int clusterBucketCount = 0;
	private String[] sortedBy;
	private String[] skewedBy;
	private String[] skewedOn;
	private List<List<String>> tableProperties;
	private boolean storeAsDirectories;
	private String serde;

	public HiveTable(String name, String[] fields) {
		super(name, fields);
	}

	public HiveTable(String name, String schema, String[] fields) {
        super(name, fields);
        setSchema(schema);
    }

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getDelimiterFieldsBy() {
		return delimiterFieldsBy;
	}

	public void setDelimiterFieldsBy(String delimiterFieldsBy) {
		this.delimiterFieldsBy = delimiterFieldsBy;
	}

	@Override
	public String constructCreateStmt() {

		StringBuilder sb = new StringBuilder();

		sb.append(super.constructCreateStmt());

		if (partitionedBy != null) {
			sb.append(" PARTITIONED BY (").append(StringUtils.join(partitionedBy, ", ")).append(")");
		}

		if (clusteredBy != null && clusterBucketCount > 0) {
			sb.append(" CLUSTERED BY (").append(StringUtils.join(clusteredBy, ", ")).append(")");
			if (sortedBy != null){
				sb.append(" SORTED BY (").append(StringUtils.join(sortedBy, ", ")).append(")");
			}
			sb.append(" INTO ").append(clusterBucketCount).append(" BUCKETS");
		}

		if (skewedBy != null && skewedOn != null) {
			sb.append(" SKEWED BY (").append(StringUtils.join(skewedBy, ", ")).append(") ON (").append(StringUtils.join(skewedOn, ", ")).append(")");
			if (storeAsDirectories) {
				sb.append(" STORED AS DIRECTORIES");
			}
		}

		if (format != null) {
			sb.append(" ").append(format).append(" FORMAT");
		}

		if (getSerde() != null) {
			sb.append(" SERDE '").append(getSerde()).append("'");
		}

		if (delimiterFieldsBy != null) {

			sb.append(" DELIMITED FIELDS TERMINATED BY '").append(delimiterFieldsBy).append("'");
		}

		if (delimiterCollectionItemsBy != null) {

			sb.append(" COLLECTION ITEMS TERMINATED BY '").append(delimiterCollectionItemsBy).append("'");
		}

		if (delimiterMapKeysBy != null) {

			sb.append(" MAP KEYS TERMINATED BY '").append(delimiterMapKeysBy).append("'");
		}

		if (delimiterLinesBy != null) {

			sb.append(" LINES TERMINATED BY '").append(delimiterLinesBy).append("'");
		}

		if (storedAs != null) {
			sb.append(" STORED AS ").append(storedAs);
		}

		if (getLocation() != null) {
			sb.append(" LOCATION '").append(getLocation()).append("'");
		}

		if (tableProperties != null && !tableProperties.isEmpty()) {
			addTablePropertiesToString(sb);
		}

		return sb.toString();
	}

	public String getStoredAs() {
		return storedAs;
	}

	public void setStoredAs(String storedAs) {
		this.storedAs = storedAs;
	}

	public String[] getPartitionedBy() {
		return partitionedBy;
	}

	public void setPartitionedBy(String[] partitionBy) {
		this.partitionedBy = partitionBy;
	}

	public void setClusteredBy(String[] clusteredBy){
		this.clusteredBy = clusteredBy;
	}

	public void setClusterBucketCount(int count){
		this.clusterBucketCount = count;
	}

	public void setSortedBy(String[] sortedBy) {
		this.sortedBy = sortedBy;
	}

	public void setSkewedBy(String[] skewedBy) {
		this.skewedBy = skewedBy;
	}

	public void setSkewedOn(String[] skewedOn) {
		this.skewedOn = skewedOn;
	}

	public void setStoreAsDirectories(boolean storeAsDirectories){
		this.storeAsDirectories = storeAsDirectories;
	}

	public String getDelimiterCollectionItemsBy() {
		return delimiterCollectionItemsBy;
	}

	public void setDelimiterCollectionItemsBy(String delimiterCollectionItemsBy) {
		this.delimiterCollectionItemsBy = delimiterCollectionItemsBy;
	}

	public String getDelimiterMapKeysBy() {
		return delimiterMapKeysBy;
	}

	public void setDelimiterMapKeysBy(String delimiterMapKeysBy) {
		this.delimiterMapKeysBy = delimiterMapKeysBy;
	}

	public String getDelimiterLinesBy() {
		return delimiterLinesBy;
	}

	public void setDelimiterLinesBy(String delimiterLinesBy) {
		this.delimiterLinesBy = delimiterLinesBy;
	}

	public String getSerde() {
		return serde;
	}

	public void setSerde(String serde) {
		this.serde = serde;
	}

	public void setTableProperties(List<List<String>> tableProperties) {
		this.tableProperties = tableProperties;
	}

	private void addTablePropertiesToString(StringBuilder sb){
		sb.append(" TBLPROPERTIES(");
		boolean hasProperty = false;
		for (List<String> list : tableProperties) {
			//tableProperties list elements should only have specifically two items as a key/value pair
			if (list.size() != 2)
				continue;
			if (hasProperty)
				sb.append(",");
			sb.append("'").append(list.get(0)).append("'='").append(list.get(1)).append("'");
			hasProperty = true;
		}
		sb.append(")");
	}

	protected String getLocation() {
		return null;
	}
}