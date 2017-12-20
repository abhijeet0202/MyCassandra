package Basics;

import java.util.Random;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.datastax.driver.core.Session;

public class TestVoucherTable implements BasicQuery {

	private static Session session = null;
	private static final String KEYSPACE_NAME = "main_vs";
	private static final String TABLE_NAME = "vouchers";
	private static String str = null;

	@DataProvider(name = "insertEntry")
	public Object[][] insertEntry() {
		return new Object[][] { 
			{ "'c:per:@activationCode'" }, { "'c:per:@batchId'" }, { "'c:per:@bk'" },
				{ "'c:per:@currency'" }, { "'c:per:@decimalValue'" }, { "'c:per:@expiryDate'" },
				{ "'c:per:@expiryKey'" }, { "'c:per:@purgeKey'" }, { "'c:per:@serialNumber'" }, { "'c:per:@state'" },
				{ "'c:per:@value'" }, { "'c:per:@voucherGroup'" }, { "'c:per:agent'" }, { "'c:per:extensionText1'" },
				{ "'c:per:extensionText2'" }, { "'c:per:extensionText3'" }, { "'c:vh:'" },
				{ "'c:per:@activationCode'" } };
	}

	@BeforeClass
	static void setUpBeforeClass() throws Exception {
		session = CassandraConnection.ConnectMe();
		StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ").append("main_vs")
				.append(" WITH replication = {").append("'class':").append("'SimpleStrategy'")
				.append(",'replication_factor':").append("1").append("};");

		System.out.println(sb.toString());
		session.execute(sb.toString());

		StringBuilder sb1 = new StringBuilder(
				"CREATE TABLE IF NOT EXISTS main_vs.vouchers ( key text, column1 text, value text, PRIMARY KEY (key, column1));");
		System.out.println(sb1.toString());
		session.execute(sb1.toString());
	}

	@BeforeTest
	static void randomgeneratiom() {
		Random random = new Random();
		Long lng = random.nextLong() & Long.MAX_VALUE;
		str = lng.toString();
	}

	@AfterClass
	static void tearDownAfterClass() throws Exception {
		System.out.println("Destroying Connection");
		// session.execute("DROP TABLE " + KEYSPACE_NAME+"."+TABLE_NAME);
		CassandraConnection.DestroyMe();
	}

	@Test(dataProvider = "insertEntry")
	void insertEntry(String query) {
		StringBuilder sqlQuery = null;
		if (query.equalsIgnoreCase("'c:vh:'")) {
			query = "'c:vh:"+str+"'";
		}

		sqlQuery = new StringBuilder("INSERT INTO ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append("(key ,column1,value)").append(" VALUES ('").append(str + "', " + query + ", 'abhijeet'")
				.append(");");

		System.out.println(sqlQuery);
		session.execute(sqlQuery.toString());
	}

}
