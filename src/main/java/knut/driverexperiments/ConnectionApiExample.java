package knut.driverexperiments;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.cloud.spanner.connection.StatementResult;

public class ConnectionApiExample {

  public static void main(String[] args) {
    String project = "my-kubernetes-codelab-217414";
    String instance = "spring-demo";
    String database = "experiments";

    StringBuilder uri = new StringBuilder("cloudspanner:");
    uri.append("/projects/").append(project)
        .append("/instances/").append(instance)
        .append("/databases/").append(database)
        .append("?autocommit=false");

    ConnectionOptions options = ConnectionOptions.newBuilder().setUri(uri.toString()).build();

    try (Connection connection = options.getConnection()) {
      StatementResult result = connection.execute(Statement.of("SELECT * FROM food"));

      ResultSet resultSet = result.getResultSet();
      while (resultSet.next()) {
        System.out.println(resultSet.getString(0));
      }
    }
  }
}
