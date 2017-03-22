package com.sdu.jstorm.transaction.trident.simple.state;

import com.google.common.collect.Lists;
import com.sdu.jstorm.utils.RedisUtils;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.*;
import org.apache.storm.trident.state.map.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Redis Map State
 *
 * @author hanhan.zhang
 * */
public class RedisMapState<T> implements IBackingMap<T> {

    // serializer
    private static final Map<StateType, Serializer> DEFAULT_SERIALIZERS = new HashMap<>();

    static {
        DEFAULT_SERIALIZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        DEFAULT_SERIALIZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        DEFAULT_SERIALIZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    // redis client
    private Jedis _jedis;

    // redis operation metric
    private CountMetric _redisReadMetric;
    private CountMetric _redisWriteMetric;
    private CountMetric _redisExceptionMetric;

    // trident state serializer
    private Serializer<T> _serializer;

    public RedisMapState(Jedis _jedis, Serializer _serializer) {
        this._jedis = _jedis;
        this._serializer = _serializer;
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        // List<Object> = Tuple
        int size = keys.size();
        List<T> values = Lists.newArrayListWithCapacity(size);
        try {
            List<Response<byte[]>> responses = Lists.newLinkedList();
            Pipeline pipeline = this._jedis.pipelined();
            for (int i = 0; i < size; i++) {
                responses.add(pipeline.get(getRedisKey(keys.get(i)).getBytes()));
            }
            pipeline.sync();
            _redisReadMetric.incrBy(size);
            values.addAll(responses.stream()
                                    .map(response -> {
                                        byte[] bytes = response.get();
                                        if (bytes == null) {
                                            return null;
                                        }
                                        return this._serializer.deserialize(bytes);
                                    })
                                    .collect(Collectors.toList()));
        } catch (Exception e) {
            e.printStackTrace();
            _redisExceptionMetric.incr();
        }
        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        // List<Object> = Tuple
        try {
            int size = keys.size();
            Pipeline pipeline = _jedis.pipelined();
            for (int i = 0; i < size; i++) {
                List<Object> tuple = keys.get(i);
                T value = values.get(i);
                pipeline.set(getRedisKey(tuple).getBytes(), this._serializer.serialize(value));
            }
            pipeline.sync();
            _redisWriteMetric.incrBy(size);
        } catch (Exception e) {
            _redisExceptionMetric.incr();
        }
    }

    protected void registerMetric(Map conf, IMetricsContext context) {
        int bucketSize = 60;
        _redisReadMetric = context.registerMetric("redis.read.count", new CountMetric(), bucketSize);
        _redisWriteMetric = context.registerMetric("redis.write.count", new CountMetric(), bucketSize);
        _redisExceptionMetric = context.registerMetric("redis.exception.count", new CountMetric(), bucketSize);
    }

    protected String getRedisKey(List<Object> tuple) {
        if (tuple.size() != 1) {
            throw new RuntimeException("redis state does not support compound keys !");
        }
        return tuple.get(0).toString();
    }

    public static <T> StateFactory build(Option<T> option) {
        return new RedisStateFactory(option);
    }

    protected static class RedisStateFactory implements StateFactory {
        // state enum
        private StateType _stateType;

        // state serializer
        private Serializer _serializer;

        // redis operate option
        private Option _option;

        public RedisStateFactory(Option _option) {
            this(_option.getStateType(), _option);
        }

        public RedisStateFactory(StateType _stateType, Option _option) {
            this._stateType = _stateType;
            this._option = _option;
            this._serializer = _option.getSerializer() == null ? DEFAULT_SERIALIZERS.get(this._stateType)
                                                                : this._option.getSerializer();
            if (this._serializer == null) {
                throw new RuntimeException("can't set serializer for trident state type " + _stateType);
            }
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            RedisMapState state = new RedisMapState(RedisUtils.getRedis(), this._serializer);
            state.registerMetric(conf, metrics);

            CachedMap cachedMap = new CachedMap(state, _option.getLocalCacheSize());

            MapState mapState;
            switch (this._stateType) {
                case TRANSACTIONAL:
                    mapState = TransactionalMap.build(cachedMap);
                    break;
                case NON_TRANSACTIONAL:
                    mapState = NonTransactionalMap.build(cachedMap);
                    break;
                case OPAQUE:
                    mapState = OpaqueMap.build(cachedMap);
                    break;
                default:
                    throw new RuntimeException("unknown trident state type " + _stateType);
            }
            return mapState;}
    }

}
