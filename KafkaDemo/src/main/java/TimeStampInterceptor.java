import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Guo
 * @create 2020-06-24-15:57
 */
public class TimeStampInterceptor implements Interceptor {



    private List<Event> result = new ArrayList<>();

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //1.获取header和body信息
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String logStr = new String(body);

        //2.将logStr转换为json对象
        JSONObject jsonObject = JSON.parseObject(logStr);
        String ts = jsonObject.getString("ts");

        //3.用数据中的时间戳替换headers中的时间戳
        headers.put("timestamp", ts);




        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        result.clear();

        for (Event event : events) {
            result.add(intercept(event));
        }
        return result;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
