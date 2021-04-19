package com.sparkss.base.api;

import com.sparkss.base.inst.DataBaseMark;
import com.sparkss.base.keys.CommonOptionKey;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.*;
import scala.math.Ordering;

/**
 * hbase读写数据 对外调用api
 * @Author $ zho.li
 * @Date 2021/3/20 1:19
 **/
public class HbaseSources {
    /**
     * read hbase data
     * @param session           sparkSession
     * @param tableName         hbase table name
     * @param schema            like "`name` STRING, `score` STRING"
     * @param cfcc              like "cf:name,cf:score"
     * @return
     */
    public Dataset<Row> readHbase(SparkSession session,
                                  String tableName,
                                  String schema,
                                  String cfcc){
        final Dataset<Row> result = session.read()
                .format(CommonOptionKey.SOURCE_CLASS)
                .option(CommonOptionKey.TABLE_NAME, tableName)
                .option(CommonOptionKey.SCHEMA_STR, schema)
                .option(CommonOptionKey.FAMILY_COLUMN, cfcc)
                .option(CommonOptionKey.DATA_BASE_MODE, DataBaseMark.HBASE_READ.getMark())
                .load()
                .filter("score > 60")
                .select("name");
        return result;
    }


    /**
     * read hbase data
     * @param session           sparkSession
     * @param tableName         hbase table name
     * @param schema            like "`name` STRING, `score` STRING"
     * @param cfccPrefix        like "cf"
     * @return
     */
    public Dataset<Row> readHbase0(SparkSession session,
                                   String tableName,
                                   String schema,
                                   String cfccPrefix){
        return readHbase(session,tableName,schema,buildCfcc(schema,cfccPrefix));
    }

    private String buildCfcc(String schema, String cfccPrefix){
        StringBuilder sb = new StringBuilder();
        final String[] split = schema.split(",");
        int idx0 = 0;
        for(String oneSchema : split){
            final String[] split1 = oneSchema.split("\\`");
            // 合法格式验证
            if (split1.length == 3){
                sb.append(split1[1] + ":" + cfccPrefix);
            }
            if (idx0 < split.length - 1){
                sb.append(",");
                idx0++;
            }
        }
        return sb.toString();
    }

}
