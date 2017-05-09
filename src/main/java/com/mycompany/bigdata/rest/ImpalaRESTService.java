package com.mycompany.bigdata.rest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

@Named
@Path("/rest")
public class ImpalaRESTService {
	
	
	@GET
	@Path("/query/{query}")
	@Produces(("text/plain"))
	public String produceJSON(@PathParam("query") String sqlQuery) {

		String finalResponse = "";
		String sqlCountQuery = "select count(*) from (" + sqlQuery + ") as a";
		// For POC purpose credentials and impala instances are hard-coded
		String connectionUrl = "jdbc:impala://<your_impala_host>:21050/default;AuthMech=3;UID=xxx;PWD=xxxxxx";
		String jdbcDriverName = "com.cloudera.impala.jdbc41.Driver";
		Connection con = null;
		ResultSet rs = null;
		Statement stmt = null;

		try {

			Class.forName(jdbcDriverName);
			con = DriverManager.getConnection(connectionUrl);
			stmt = con.createStatement();
			rs = stmt.executeQuery(sqlCountQuery);
			int recordCount = 0;
			while (rs.next()) {
				// get the record count
				recordCount = Integer.parseInt(rs.getString(1));
			}
			rs = stmt.executeQuery(sqlQuery);
			// get the column count
			int colCount = rs.getMetaData().getColumnCount();
			int j = 1;
			// Prepare the JSON response
			StringBuilder sb = new StringBuilder();
			sb.append("{\n");
			sb.append("  \"results\": {\n");
			sb.append("    \"success\": \"true\",\n");
			// Embed all resultset data in JSON
			while (rs.next()) {
				sb.append("    \"result\": {\n");
				for (int i = 0; i < colCount - 1; i++) {
					if (i == colCount - 2) {
						sb.append("      \"" + rs.getMetaData().getColumnLabel(i + 1) + "\": " + "\""
								+ rs.getString(i + 1) + "\"\n");
					} else {
						sb.append("      \"" + rs.getMetaData().getColumnLabel(i + 1) + "\": " + "\""
								+ rs.getString(i + 1) + "\",\n");
					}
				}
				if (j != recordCount) {
					sb.append("    },\n");
				} else {
					sb.append("    }\n");
				}
				j++;
			}
			// Complete the JSON response
			sb.append("  }\n");
			sb.append("}\n");
			finalResponse = sb.toString();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
				stmt.close();
				rs.close();
			} catch (Exception e) {
				// swallow
			}
		}
		return finalResponse;
	}

}
