package Basics;

import com.datastax.driver.core.Session;

/**
 * @author ezbanab
 *
 */
public class UserDefinedDataTypes implements BasicQuery{

	/**
	 * 
	 */
	public UserDefinedDataTypes(Session session, String keySpaceName, String tableName) {
		/*StringBuilder sb = new StringBuilder("CREATE TYPE IF NOT EXISTS ").append(keySpaceName).append(".").append("mobile ( country_code int, number text )");
		System.out.println(sb.toString());
		session.execute(sb.toString());*/
		
		StringBuilder sb1 = new StringBuilder("CREATE TYPE IF NOT EXISTS ").append(keySpaceName).append(".").append("address ( street text, city text, zip text, phones map<text, text>)");
		System.out.println(sb1.toString());
		session.execute(sb1.toString());
		
		StringBuilder sb2 = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(keySpaceName).append(".").append(tableName).append(" (name text PRIMARY KEY, addresses map<text, frozen<address>>);");
		System.out.println(sb2.toString());
		session.execute(sb2.toString());
	}
	

}
