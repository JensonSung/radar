package sqlancer.tidb.transaction;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.SQLancerResultSet;

public class QueryResultUtil {

    public static List<Object> getQueryResult(SQLancerResultSet resultSet) throws SQLException {
        List<Object> resList = new ArrayList<>();
        if (resultSet != null) {
            ResultSetMetaData metaData = resultSet.getRs().getMetaData();
            int columns = metaData.getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= columns; i++) {
                    Object data = resultSet.getRs().getObject(i);
                    if (data instanceof byte[]) {
                        // Binary types like BLOB, BINARY return byte[]. Other types return themselves.
                        data = byteArrToHexStr((byte[]) data);
                    }
                    resList.add(data);
                }
            }
        }
        return resList;
    }

    private static String byteArrToHexStr(byte[] bytes) {
        if (bytes.length == 0) {
            return "0";
        }
        final String HEX = "0123456789ABCDEF";
        StringBuilder sb = new StringBuilder();
        sb.append("0x");
        for (byte b : bytes) {
            sb.append(HEX.charAt((b >> 4) & 0x0F));
            sb.append(HEX.charAt(b & 0x0F));
        }
        return sb.toString();
    }
}
