package com.alibaba.datax.plugin.reader.httpreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
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

            List<Object> conns = jobConfig.getList(Key.CONNECTION,
                Object.class);

            for (int i = 0, len = conns.size(); i < len; i++) {
                Configuration connConf = Configuration
                    .from(conns.get(i).toString());

                String url = connConf.getString(Key.HTTPURL);

                this.jobConfig.set(Key.HTTPURL, url);
            }
            //
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

        @Override
        public void startRead(RecordSender recordSender) {
            String url = configuration.getString(Key.HTTPURL);
            LOG.info(String.format("reading url : [%s]", url));

            ResponseEntity<String> result = restTemplate.getForEntity(url, String.class);
            LOG.info(String.format("reading result : [%s]", result));

            List<Configuration> columns = this.configuration
                .getListConfiguration(Key.COLUMN);

            Record record = recordSender.createRecord();

            for (Configuration eachColumnConf : columns) {
                String name = eachColumnConf.getString(Key.NAME);
                String type = eachColumnConf.getString(Key.TYPE);

                LOG.info(String.format("conf  column name : [%s]", name));
                LOG.info(String.format("conf  column type : [%s]", type));
            }
            Column column = new StringColumn("hello word");

            record.addColumn(column);

            recordSender.sendToWriter(record);
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
