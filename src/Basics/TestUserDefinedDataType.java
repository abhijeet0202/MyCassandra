package Basics;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.Session;

/**
 * @author ezbanab
 *
 */
public class TestUserDefinedDataType implements BasicQuery {
	private static Session session = null;
	private static final String KEYSPACE_NAME = "basic";
	private static final String TABLE_NAME = "myUDT";
	private static BasicQuery myObject = null;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	static void setUpBeforeClass() throws Exception {
		session = CassandraConnection.ConnectMe();
		new CreateKeySpace(session, KEYSPACE_NAME);
		myObject = new UserDefinedDataTypes(session, KEYSPACE_NAME, TABLE_NAME);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	static void tearDownAfterClass() throws Exception {
		System.out.println("Destroying Connection");
		//session.execute("DROP TABLE " + KEYSPACE_NAME + "." + TABLE_NAME);
		CassandraConnection.DestroyMe();
	}

	@Test()
	void insertEntry() {
		StringBuilder builder = new StringBuilder("INSERT INTO ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" ( name, addresses) VALUES ('Abhijeet', { " + "'homeAddress' :"
						+ "	{ street: '1177 Orchid Island', city: 'Gurgaon', zip: '122001'," + "phones:{"
						+ " 'cell': '9910512611' } }  }) ;");
		myObject.executeQuery(session, builder.toString());
		myObject.printStatement(session, KEYSPACE_NAME, TABLE_NAME, null);
	}

	@Test(dependsOnMethods = "insertEntry")
	void alterUserDataTypeAdd() {
		StringBuilder builder = new StringBuilder("ALTER TYPE basic.address ADD country text;");
		myObject.executeQuery(session, builder.toString());
		myObject.printStatement(session, KEYSPACE_NAME, TABLE_NAME, null);
	}

	@Test(dependsOnMethods = "insertEntry")
	void alterUserDataTypeRename() {
		StringBuilder builder = new StringBuilder("ALTER TYPE basic.address RENAME zip to zipCode;");
		myObject.executeQuery(session, builder.toString());
		myObject.printStatement(session, KEYSPACE_NAME, TABLE_NAME, null);
	}

	@Test(dependsOnMethods = "insertEntry")
	void dropUserDataType() {
		StringBuilder dropTable = new StringBuilder("DROP TABLE IF EXISTS basic.myudt;");
		myObject.executeQuery(session, dropTable.toString());
		
		StringBuilder dropUDt = new StringBuilder("DROP TYPE IF EXISTS basic.address;");		
		myObject.executeQuery(session, dropUDt.toString());
		//myObject.printStatement(session, KEYSPACE_NAME, TABLE_NAME, null);
	}

}
