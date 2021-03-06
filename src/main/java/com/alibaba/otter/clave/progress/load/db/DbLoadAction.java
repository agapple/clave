package com.alibaba.otter.clave.progress.load.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.support.lob.LobCreator;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.clave.common.convert.SqlUtils;
import com.alibaba.otter.clave.common.dialect.DbDialect;
import com.alibaba.otter.clave.exceptions.ClaveException;
import com.alibaba.otter.clave.model.EventColumn;
import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.model.EventType;
import com.alibaba.otter.clave.model.RowBatch;
import com.alibaba.otter.clave.progress.load.db.DbLoadContext.DbLoadCounter;
import com.alibaba.otter.clave.progress.load.db.DbLoadData.TableLoadData;
import com.alibaba.otter.clave.progress.load.interceptor.LoadInterceptor;
import com.alibaba.otter.clave.progress.load.weight.WeightBuckets;
import com.alibaba.otter.clave.progress.load.weight.WeightController;

/**
 * 数据库load的执行入口
 * 
 * @author jianghang 2011-10-31 下午03:17:43
 * @version 1.0.0
 */
public class DbLoadAction implements InitializingBean, DisposableBean {

    private static final Logger logger            = LoggerFactory.getLogger(DbLoadAction.class);
    private static final String WORKER_NAME       = "DbLoadAction";
    private static final int    DEFAULT_POOL_SIZE = 5;
    private int                 poolSize          = DEFAULT_POOL_SIZE;
    private int                 retry             = 3;
    private int                 retryWait         = 3000;
    private LoadInterceptor     interceptor;
    private ExecutorService     executor;
    private int                 batchSize         = 50;
    private boolean             useBatch          = true;
    private boolean             skipLoadException = false;

    /**
     * 返回结果为已处理成功的记录
     */
    public DbLoadContext load(RowBatch rowBatch, WeightController controller, DbLoadContext context) {
        Assert.notNull(rowBatch);
        try {
            List<EventData> datas = rowBatch.getDatas();
            context.setPrepareDatas(datas);
            // 执行重复录入数据过滤
            datas = context.getPrepareDatas();
            if (datas == null || datas.size() == 0) {
                logger.info("##no eventdata for load, return");
                return context;
            }

            // 因为所有的数据在DbBatchLoader已按照DateMediaSource进行归好类，不同数据源介质会有不同的DbLoadAction进行处理
            // 设置media source时，只需要取第一节点的source即可
            interceptor.prepare(context);
            // 执行重复录入数据过滤
            datas = context.getPrepareDatas();
            WeightBuckets<EventData> buckets = buildWeightBuckets(context, datas);
            List<Long> weights = buckets.weights();
            controller.start(weights);// weights可能为空，也得调用start方法
            if (CollectionUtils.isEmpty(datas)) {
                logger.info("##no eventdata for load");
            }
            // 按权重构建数据对象
            // 处理数据
            for (int i = 0; i < weights.size(); i++) {
                Long weight = weights.get(i);
                controller.await(weight.intValue());
                // 处理同一个weight下的数据
                List<EventData> items = buckets.getItems(weight);
                logger.debug("##start load for weight:" + weight);
                // 预处理下数据

                // 进行一次数据合并，合并相同pk的多次I/U/D操作
                items = DbLoadMerger.merge(items);
                // 按I/U/D进行归并处理
                DbLoadData loadData = new DbLoadData();
                doBefore(items, context, loadData);
                // 执行load操作
                doLoad(context, loadData);
                controller.single(weight.intValue());
                logger.debug("##end load for weight:" + weight);
            }
            interceptor.commit(context);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            interceptor.error(context);
        } catch (Exception e) {
            interceptor.error(context);
            throw new ClaveException(e);
        }

        return context;// 返回处理成功的记录
    }

    /**
     * 构建基于weight权重分组的item集合列表
     */
    private WeightBuckets<EventData> buildWeightBuckets(DbLoadContext context, List<EventData> datas) {
        WeightBuckets<EventData> buckets = new WeightBuckets<EventData>();
        for (EventData data : datas) {
            // 获取对应的weight
            // TODO 根据表名定义权重
            buckets.addItem(1, data);
        }

        return buckets;
    }

    /**
     * 执行数据处理，比如数据冲突检测
     */
    private void doBefore(List<EventData> items, final DbLoadContext context, final DbLoadData loadData) {
        for (final EventData item : items) {
            boolean filter = interceptor.before(context, item);
            if (!filter) {
                loadData.merge(item);// 进行分类
            }
        }
    }

    private void doLoad(final DbLoadContext context, DbLoadData loadData) {
        // 优先处理delete,可以利用batch优化
        List<List<EventData>> batchDatas = new ArrayList<List<EventData>>();
        for (TableLoadData tableData : loadData.getTables()) {
            if (useBatch) {
                // 优先执行delete语句，针对uniqe更新，一般会进行delete + insert的处理模式，避免并发更新
                batchDatas.addAll(split(tableData.getDeleteDatas()));
            } else {
                // 如果不可以执行batch，则按照单条数据进行并行提交
                // 优先执行delete语句，针对uniqe更新，一般会进行delete + insert的处理模式，避免并发更新
                for (EventData data : tableData.getDeleteDatas()) {
                    batchDatas.add(Arrays.asList(data));
                }
            }
        }
        doTwoPhase(context, batchDatas, true);
        batchDatas.clear();

        // 处理下insert/update
        for (TableLoadData tableData : loadData.getTables()) {
            if (useBatch) {
                // 执行insert + update语句
                batchDatas.addAll(split(tableData.getInsertDatas()));
                batchDatas.addAll(split(tableData.getUpadateDatas()));// 每条记录分为一组，并行加载
            } else {
                // 执行insert + update语句
                for (EventData data : tableData.getInsertDatas()) {
                    batchDatas.add(Arrays.asList(data));
                }
                for (EventData data : tableData.getUpadateDatas()) {
                    batchDatas.add(Arrays.asList(data));
                }
            }
        }

        doTwoPhase(context, batchDatas, true);
        batchDatas.clear();
    }

    /**
     * 将对应的数据按照sql相同进行batch组合
     */
    private List<List<EventData>> split(List<EventData> datas) {
        List<List<EventData>> result = new ArrayList<List<EventData>>();
        if (datas == null || datas.size() == 0) {
            return result;
        } else {
            int[] bits = new int[datas.size()];// 初始化一个标记，用于标明对应的记录是否已分入某个batch
            for (int i = 0; i < bits.length; i++) {
                // 跳过已经被分入batch的
                while (i < bits.length && bits[i] == 1) {
                    i++;
                }

                if (i >= bits.length) { // 已处理完成，退出
                    break;
                }

                // 开始添加batch，最大只加入batchSize个数的对象
                List<EventData> batch = new ArrayList<EventData>();
                bits[i] = 1;
                batch.add(datas.get(i));
                for (int j = i + 1; j < bits.length && batch.size() < batchSize; j++) {
                    if (bits[j] == 0 && canBatch(datas.get(i), datas.get(j))) {
                        batch.add(datas.get(j));
                        bits[j] = 1;// 修改为已加入
                    }
                }
                result.add(batch);
            }

            return result;
        }
    }

    /**
     * 判断两条记录是否可以作为一个batch提交，主要判断sql是否相等. 可优先通过schemaName进行判断
     */
    private boolean canBatch(EventData source, EventData target) {
        // return StringUtils.equals(source.getSchemaName(), target.getSchemaName())
        // && StringUtils.equals(source.getTableName(), target.getTableName())
        // && StringUtils.equals(source.getSql(), target.getSql());
        // return StringUtils.equals(source.getSql(), target.getSql());

        // 因为sqlTemplate构造sql时用了String.intern()的操作，保证相同字符串的引用是同一个，所以可以直接使用==进行判断，提升效率
        return source.getSql() == target.getSql();
    }

    /**
     * 首先进行并行执行，出错后转为串行执行
     */
    private void doTwoPhase(DbLoadContext context, List<List<EventData>> totalRows, boolean canBatch) {
        // 预处理下数据
        List<Future<Exception>> results = new ArrayList<Future<Exception>>();
        for (List<EventData> rows : totalRows) {
            if (CollectionUtils.isEmpty(rows)) {
                continue; // 过滤空记录
            }

            results.add(executor.submit(new DbLoadWorker(context, rows, canBatch)));
        }

        boolean partFailed = false;
        for (int i = 0; i < results.size(); i++) {
            Future<Exception> result = results.get(i);
            Exception ex = null;
            try {
                ex = result.get();
                for (EventData data : totalRows.get(i)) {
                    interceptor.after(context, data);// 通知加载完成
                }
            } catch (Exception e) {
                ex = e;
            }

            if (ex != null) {
                logger.warn("##load phase one failed!", ex);
                partFailed = true;
            }
        }

        if (true == partFailed) {
            if (CollectionUtils.isEmpty(context.getFailedDatas())) {
                logger.error("##load phase one failed but failedDatas is empty!");
                return;
            }

            List<EventData> retryEventDatas = new ArrayList<EventData>(context.getFailedDatas());
            context.getFailedDatas().clear(); // 清理failed data数据

            // 可能为null，manager老版本数据序列化传输时，因为数据库中没有skipClaveException变量配置
            if (skipLoadException) {// 如果设置为允许跳过单条异常，则一条条执行数据load，准确过滤掉出错的记录，并进行日志记录
                for (EventData retryEventData : retryEventDatas) {
                    DbLoadWorker worker = new DbLoadWorker(context, Arrays.asList(retryEventData), false);// 强制设置batch为false
                    try {
                        Exception ex = worker.call();
                        if (ex != null) {
                            // do skip
                            logger.warn("skip exception for data : {} , caused by {}", retryEventData,
                                        ExceptionUtils.getFullStackTrace(ex));
                        }
                    } catch (Exception ex) {
                        // do skip
                        logger.warn("skip exception for data : {} , caused by {}", retryEventData,
                                    ExceptionUtils.getFullStackTrace(ex));
                    }
                }
            } else {
                // 直接一批进行处理，减少线程调度
                DbLoadWorker worker = new DbLoadWorker(context, retryEventDatas, false);// 强制设置batch为false
                try {
                    Exception ex = worker.call();
                    if (ex != null) {
                        throw ex; // 自己抛自己接
                    }
                } catch (Exception ex) {
                    logger.error("##load phase two failed!", ex);
                    throw new ClaveException(ex);
                }
            }

            // 清理failed data数据
            for (EventData data : retryEventDatas) {
                interceptor.after(context, data);// 通知加载完成
            }
        }

    }

    public void afterPropertiesSet() throws Exception {
        executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
                                          new ArrayBlockingQueue(poolSize * 4), new NamedThreadFactory(WORKER_NAME),
                                          new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void destroy() throws Exception {
        executor.shutdownNow();
    }

    enum ExecuteResult {
        SUCCESS, ERROR, RETRY
    }

    class DbLoadWorker implements Callable<Exception> {

        private DbLoadContext   context;
        private DbDialect       dbDialect;
        private List<EventData> datas;
        private boolean         canBatch;
        private List<EventData> allFailedDatas   = new ArrayList<EventData>();
        private List<EventData> allProcesedDatas = new ArrayList<EventData>();
        private List<EventData> processedDatas   = new ArrayList<EventData>();
        private List<EventData> failedDatas      = new ArrayList<EventData>();

        public DbLoadWorker(DbLoadContext context, List<EventData> datas, boolean canBatch){
            this.context = context;
            this.datas = datas;
            this.canBatch = canBatch;

            dbDialect = context.getDbDialect();

        }

        public Exception call() throws Exception {
            return doCall();
        }

        private Exception doCall() {
            RuntimeException error = null;
            ExecuteResult exeResult = null;
            int index = 0;// 记录下处理成功的记录下标
            for (; index < datas.size();) {
                // 处理数据切分
                final List<EventData> splitDatas = new ArrayList<EventData>();
                if (useBatch && canBatch) {
                    int end = (index + batchSize > datas.size()) ? datas.size() : (index + batchSize);
                    splitDatas.addAll(datas.subList(index, end));
                    index = end;// 移动到下一批次
                } else {
                    splitDatas.add(datas.get(index));
                    index = index + 1;// 移动到下一条
                }

                int retryCount = 0;
                while (true) {
                    try {
                        if (CollectionUtils.isEmpty(failedDatas) == false) {
                            splitDatas.clear();
                            splitDatas.addAll(failedDatas); // 下次重试时，只处理错误的记录
                        } else {
                            failedDatas.addAll(splitDatas); // 先添加为出错记录，可能获取lob,datasource会出错
                        }

                        final LobCreator lobCreator = dbDialect.getLobHandler().getLobCreator();
                        if (useBatch && canBatch) {
                            // 处理batch
                            final String sql = splitDatas.get(0).getSql();
                            int[] affects = new int[splitDatas.size()];
                            affects = (int[]) dbDialect.getTransactionTemplate().execute(new TransactionCallback() {

                                public Object doInTransaction(TransactionStatus status) {
                                    // 初始化一下内容
                                    try {
                                        failedDatas.clear(); // 先清理
                                        processedDatas.clear();
                                        interceptor.transactionBegin(context, splitDatas, dbDialect);
                                        JdbcTemplate template = dbDialect.getJdbcTemplate();
                                        int[] affects = template.batchUpdate(sql, new BatchPreparedStatementSetter() {

                                            public void setValues(PreparedStatement ps, int idx) throws SQLException {
                                                doPreparedStatement(ps, dbDialect, lobCreator, splitDatas.get(idx));
                                            }

                                            public int getBatchSize() {
                                                return splitDatas.size();
                                            }
                                        });
                                        interceptor.transactionEnd(context, splitDatas, dbDialect);
                                        return affects;
                                    } finally {
                                        lobCreator.close();
                                    }
                                }

                            });

                            // 更新统计信息
                            for (int i = 0; i < splitDatas.size(); i++) {
                                processStat(splitDatas.get(i), affects[i], true);
                            }
                        } else {
                            final EventData data = splitDatas.get(0);// 直接取第一条
                            int affect = 0;
                            affect = (Integer) dbDialect.getTransactionTemplate().execute(new TransactionCallback() {

                                public Object doInTransaction(TransactionStatus status) {
                                    try {
                                        failedDatas.clear(); // 先清理
                                        processedDatas.clear();
                                        interceptor.transactionBegin(context, Arrays.asList(data), dbDialect);
                                        JdbcTemplate template = dbDialect.getJdbcTemplate();
                                        int affect = template.update(data.getSql(), new PreparedStatementSetter() {

                                            public void setValues(PreparedStatement ps) throws SQLException {
                                                doPreparedStatement(ps, dbDialect, lobCreator, data);
                                            }
                                        });
                                        interceptor.transactionEnd(context, Arrays.asList(data), dbDialect);
                                        return affect;
                                    } finally {
                                        lobCreator.close();
                                    }
                                }
                            });
                            // 更新统计信息
                            processStat(data, affect, false);
                        }

                        error = null;
                        exeResult = ExecuteResult.SUCCESS;
                    } catch (DeadlockLoserDataAccessException ex) {
                        error = new ClaveException(ExceptionUtils.getFullStackTrace(ex),
                                                   DbLoadDumper.dumpEventDatas(splitDatas));
                        exeResult = ExecuteResult.RETRY;
                    } catch (DataIntegrityViolationException ex) {
                        error = new ClaveException(ExceptionUtils.getFullStackTrace(ex),
                                                   DbLoadDumper.dumpEventDatas(splitDatas));
                        // if (StringUtils.contains(ex.getMessage(), "ORA-00001")) {
                        // exeResult = ExecuteResult.RETRY;
                        // } else {
                        // exeResult = ExecuteResult.ERROR;
                        // }
                        exeResult = ExecuteResult.ERROR;
                    } catch (RuntimeException ex) {
                        error = new ClaveException(ExceptionUtils.getFullStackTrace(ex),
                                                   DbLoadDumper.dumpEventDatas(splitDatas));
                        exeResult = ExecuteResult.ERROR;
                    } catch (Throwable ex) {
                        error = new ClaveException(ExceptionUtils.getFullStackTrace(ex),
                                                   DbLoadDumper.dumpEventDatas(splitDatas));
                        exeResult = ExecuteResult.ERROR;
                    }

                    if (ExecuteResult.SUCCESS == exeResult) {
                        allFailedDatas.addAll(failedDatas);// 记录一下异常到all记录中
                        allProcesedDatas.addAll(processedDatas);
                        failedDatas.clear();// 清空上一轮的处理
                        processedDatas.clear();
                        break; // do next eventData
                    } else if (ExecuteResult.RETRY == exeResult) {
                        retryCount = retryCount + 1;// 计数一次
                        // 出现异常，理论上当前的批次都会失败
                        processedDatas.clear();
                        failedDatas.clear();
                        failedDatas.addAll(splitDatas);
                        if (retryCount >= retry) {
                            processFailedDatas(index);// 重试已结束，添加出错记录并退出
                            throw new ClaveException(String.format("execute retry %s times failed", retryCount), error);
                        } else {
                            try {
                                int wait = retryCount * retryWait;
                                wait = (wait < retryWait) ? retryWait : wait;
                                Thread.sleep(wait);
                            } catch (InterruptedException ex) {
                                Thread.interrupted();
                                processFailedDatas(index);// 局部处理出错了
                                throw new ClaveException(ex);
                            }
                        }
                    } else {
                        // 出现异常，理论上当前的批次都会失败
                        processedDatas.clear();
                        failedDatas.clear();
                        failedDatas.addAll(splitDatas);
                        processFailedDatas(index);// 局部处理出错了
                        throw error;
                    }
                }
            }

            // 记录一下当前处理过程中失败的记录,affect = 0的记录
            context.getFailedDatas().addAll(allFailedDatas);
            context.getProcessedDatas().addAll(allProcesedDatas);
            return null;
        }

        private void doPreparedStatement(PreparedStatement ps, DbDialect dbDialect, LobCreator lobCreator,
                                         EventData data) throws SQLException {
            EventType type = data.getEventType();
            // 注意insert/update语句对应的字段数序都是将主键排在后面
            List<EventColumn> columns = new ArrayList<EventColumn>();
            if (type.isInsert()) {
                columns.addAll(data.getColumns()); // insert为所有字段
                columns.addAll(data.getKeys());
            } else if (type.isDelete()) {
                columns.addAll(data.getKeys());
            } else if (type.isUpdate()) {
                boolean existOldKeys = !CollectionUtils.isEmpty(data.getOldKeys());
                columns.addAll(data.getUpdatedColumns());// 只更新带有isUpdate=true的字段
                columns.addAll(data.getKeys());
                if (existOldKeys) {
                    columns.addAll(data.getOldKeys());
                }
            }
            for (int i = 0; i < columns.size(); i++) {
                int paramIndex = i + 1;
                EventColumn column = columns.get(i);
                int sqlType = column.getColumnType();

                // 获取一下当前字段名的数据是否必填
                Table table = dbDialect.findTable(data.getSchemaName(), data.getTableName());
                Map<String, Boolean> isRequiredMap = new HashMap<String, Boolean>();
                for (Column tableColumn : table.getColumns()) {
                    isRequiredMap.put(StringUtils.lowerCase(tableColumn.getName()), tableColumn.isRequired());
                }

                Boolean isRequired = isRequiredMap.get(StringUtils.lowerCase(column.getColumnName()));
                if (isRequired == null) {
                    throw new ClaveException(String.format("column name %s is not found in Table[%s]",
                                                           column.getColumnName(), table.toString()));
                }

                Object param = SqlUtils.stringToSqlValue(column.getColumnValue(), sqlType, isRequired,
                                                         dbDialect.isEmptyStringNulled());
                try {
                    switch (sqlType) {
                        case Types.CLOB:
                            lobCreator.setClobAsString(ps, paramIndex, (String) param);
                            break;

                        case Types.BLOB:
                            lobCreator.setBlobAsBytes(ps, paramIndex, (byte[]) param);
                            break;

                        default:
                            StatementCreatorUtils.setParameterValue(ps, paramIndex, sqlType, null, param);
                            break;
                    }
                } catch (SQLException ex) {
                    logger.error("## SetParam error , [table={}, sqltype={}, value={}]", new Object[] {
                            data.getSchemaName() + "." + data.getTableName(), sqlType, param });
                    throw ex;
                }
            }
        }

        private void processStat(EventData data, int affect, boolean batch) {
            if (batch && (affect < 1 && affect != Statement.SUCCESS_NO_INFO)) {
                failedDatas.add(data); // 记录到错误的临时队列，进行重试处理
            } else if (!batch && affect < 1) {
                failedDatas.add(data);// 记录到错误的临时队列，进行重试处理
            } else {
                processedDatas.add(data); // 记录到成功的临时队列，commit也可能会失败。所以这记录也可能需要进行重试
                DbLoadCounter counter = context.getCounters().get(
                                                                  Arrays.asList(data.getSchemaName(),
                                                                                data.getTableName()));
                EventType type = data.getEventType();
                if (type.isInsert()) {
                    counter.getInsertCount().incrementAndGet();
                } else if (type.isUpdate()) {
                    counter.getUpdateCount().incrementAndGet();
                } else if (type.isDelete()) {
                    counter.getDeleteCount().incrementAndGet();
                }

                counter.getRowSize().addAndGet(calculateSize(data));
            }
        }

        // 出现异常回滚了，记录一下异常记录
        private void processFailedDatas(int index) {
            allFailedDatas.addAll(failedDatas);// 添加失败记录
            context.getFailedDatas().addAll(allFailedDatas);// 添加历史出错记录
            for (; index < datas.size(); index++) { // 记录一下未处理的数据
                context.getFailedDatas().add(datas.get(index));
            }
            // 这里不需要添加当前成功记录，出现异常后会rollback所有的成功记录，比如processDatas有记录，但在commit出现失败 (bugfix)
            allProcesedDatas.addAll(processedDatas);
            context.getProcessedDatas().addAll(allProcesedDatas);// 添加历史成功记录
        }

        // 大致估算一下row记录的大小
        private long calculateSize(EventData data) {
            // long size = 0L;
            // size += data.getKeys().toString().getBytes().length - 12 - data.getKeys().size() + 1L;
            // size += data.getColumns().toString().getBytes().length - 12 - data.getKeys().size() + 1L;
            // return size;

            // byte[] bytes = JsonUtils.marshalToByte(data);// 走序列化的方式快速计算一下大小
            // return bytes.length;

            return data.getSize();// 数据不做计算，避免影响性能
        }
    }

    // =============== setter / getter ===============

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public void setRetryWait(int retryWait) {
        this.retryWait = retryWait;
    }

    public void setInterceptor(LoadInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public void setUseBatch(boolean useBatch) {
        this.useBatch = useBatch;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setSkipLoadException(boolean skipLoadException) {
        this.skipLoadException = skipLoadException;
    }

}
