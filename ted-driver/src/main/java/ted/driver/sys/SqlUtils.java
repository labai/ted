package ted.driver.sys;

/**
 * @author Augustus
 *         created on 2019.12.18
 *
 * supported db types with some specific sql syntax
 *
 * for TED internal usage only!!!
 */
public class SqlUtils {

	public interface SqlDbExt {
		String now();
		String intervalSeconds(int secCount);
		String intervalDays(int dayCount);
		String rownum(int rowNum);
		String sequenceSql(String seqName);
		String sequenceSelect(String seqName);
		String systemColumn();
		String forUpdateSkipLocked();
	}

	public enum DbType {
		ORACLE(new SqlDbExt() {
			public String now() { return "cast(systimestamp as timestamp(3))"; }
			public String intervalSeconds(int secCount) { return secCount + " / 86400"; }
			public String intervalDays(int dayCount) { return "" + dayCount; }
			public String rownum(int rowNum) { return " and rownum <= " + rowNum; } // must be last of conditions
			public String sequenceSql(String seqName) { return seqName + ".nextval"; }
			public String sequenceSelect(String seqName) { return "select " + seqName + ".nextval from dual"; }
			public String systemColumn() { return "system"; }
			public String forUpdateSkipLocked() { return "for update skip locked"; }
		}),
		POSTGRES(new SqlDbExt() {
			public String now() { return "now()"; }
			public String intervalSeconds(int secCount) { return "interval '" + secCount + "' second"; }
			public String intervalDays(int dayCount) { return "interval '" + dayCount + "' day";}
			public String rownum(int rowNum) { return " limit " + rowNum; }
			public String sequenceSql(String seqName) { return "nextval('" + seqName + "')"; }
			public String sequenceSelect(String seqName) { return "select nextval('" + seqName + "')"; }
			public String systemColumn() { return "system"; }
			public String forUpdateSkipLocked() { return "for update skip locked"; }
		}),
		MYSQL(new SqlDbExt() {
			public String now() { return "now(3)"; }
			public String intervalSeconds(int secCount) { return "interval " + secCount + " second"; }
			public String intervalDays(int dayCount) { return "interval " + dayCount + " day";}
			public String rownum(int rowNum) { return " limit " + rowNum; }
			public String sequenceSql(String seqName) { return "nextval('" + seqName + "')"; }
			public String sequenceSelect(String seqName) { return "select nextval('" + seqName + "')"; }
			public String systemColumn() { return "`system`"; } // in MySql it is reserved
			public String forUpdateSkipLocked() { return "for update skip locked"; }
		}),
		HSQLDB(new SqlDbExt() { // use PostgreSQL dialect
			public String now() { return "now()"; }
			public String intervalSeconds(int secCount) { return "interval '" + secCount + "' second"; }
			public String intervalDays(int dayCount) { return "interval '" + dayCount + "' day";}
			public String rownum(int rowNum) { return " limit " + rowNum; }
			public String sequenceSql(String seqName) { return "nextval('" + seqName + "')"; }
			public String sequenceSelect(String seqName) { return "select nextval('" + seqName + "')"; }
			public String systemColumn() { return "system"; }
			public String forUpdateSkipLocked() { return ""; } // no locks here
		});
		private final SqlDbExt sql;

		public SqlDbExt sql() { return sql; }

		DbType(SqlDbExt sql) {
			this.sql = sql;
		}
	}
}
