package com.alibaba.datax.plugin.reader.httpreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

/**
 * http读取类
 */
public class HttpReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration jobConfig = null;

        private RestTemplate restTemplate;

        @Override
        public void init() {
            this.jobConfig = super.getPluginJobConf();
            this.jobConfig = super.getPluginJobConf();

            restTemplate = new RestTemplate();
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> splittedConfigs = new ArrayList<Configuration>();
            splittedConfigs.add(jobConfig);

            return splittedConfigs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Reader.Task {

        private static Logger LOG = LoggerFactory.getLogger(Task.class);

        private RestTemplate restTemplate;

        private Configuration configuration;

        @Override
        public void init() {
            restTemplate = new RestTemplate();
            configuration = this.getPluginJobConf();
        }

        @Override
        public void prepare() {
        }

        /**
         *  引入动态数据转换方式，对数据源的每行数据进行转换
         *
         * @param recordSender
         */
        @Override
        public void startRead(RecordSender recordSender) {
            String url = configuration.getString(Key.HTTPURL);

            LOG.info(String.format("reading url : [%s]", url));
            ResponseEntity<String> result = restTemplate.getForEntity(url, String.class);
            LOG.info(String.format("reading result : [%s]", result));

            JSONObject data = (JSONObject) JSON.parse(result.getBody());
            String dataJsonPath = configuration.getString(Key.DATAJSONPATH);
            JSONArray jsonArray = (JSONArray) data.get(dataJsonPath);

            JSONArray columnMeta = JSON.parseArray(configuration.getString(Key.COLUMN));
            for (Object row : jsonArray) {
                Record record = recordSender.createRecord();
                try {
                    final JSONObject item = JSON.parseObject(String.valueOf(row));
                    for (Object column : columnMeta) {
                        Object columnName = ((JSONObject) column).getString(Key.NAME);
                        Object columnType = ((JSONObject) column).getString(Key.TYPE);
                        record.addColumn(ColumnTypeEnum.valueOf(String.valueOf(columnType).
                            toUpperCase()).getColumnValue(item, columnName));
                    }
                } catch (Exception e) {
                    if (e instanceof DataXException) {
                        super.getTaskPluginCollector().collectDirtyRecord(record, e);
                        LOG.error("数据采集发生异常", e);
                        throw (DataXException) e;
                    }
                }
                recordSender.sendToWriter(record);
            }

            LOG.info("Finished read record by Sql: [{}\n] {}.",
                url);
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
