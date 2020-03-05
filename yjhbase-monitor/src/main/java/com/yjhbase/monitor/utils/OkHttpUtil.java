package com.yjhbase.monitor.utils;

import okhttp3.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhengzhubin on 2018/6/6.
 */
public class OkHttpUtil {

    private static final Long HTTP_CONNECT_TIMEOUT_SECONDS = 5L;
    private static final Long HTTP_READ_TIMEOUT_SECONDS = 5L;
//    private static final Integer HTTP_RESPONSE_OKCODE = 200;

    private static final OkHttpClient HTTP_CLIENT = new OkHttpClient().newBuilder()
            .connectTimeout(HTTP_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .readTimeout(HTTP_READ_TIMEOUT_SECONDS , TimeUnit.SECONDS)
            .build();

    private static OkHttpClient getHttpClient() {
        return HTTP_CLIENT.newBuilder().build();
    }

    private static Response httpGetQuery(OkHttpClient client, String url, Map<String, String> headers,
                                         Map<String, String> paramValues) throws IOException {
        Request.Builder requestBuilder = new Request.Builder();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            requestBuilder.addHeader(entry.getKey(), entry.getValue());
        }

        HttpUrl.Builder urlBuilder = HttpUrl.parse(url).newBuilder();
        for (Map.Entry<String, String> param : paramValues.entrySet()) {
            urlBuilder.addQueryParameter(param.getKey(), param.getValue());
        }
        requestBuilder.url(urlBuilder.build());
        Request request = requestBuilder.build();
        return client.newCall(request).execute();
    }

    private static Response httpGetQuery(OkHttpClient client, String url) throws IOException {
        Request.Builder requestBuilder = new Request.Builder();
        requestBuilder.url(url);
        Request request = requestBuilder.build();

        return client.newCall(request).execute();
    }

    public static Response httpGet(String url, Map<String, String> headers, Map<String, String> paramValues)
            throws IOException {
        OkHttpClient client = getHttpClient();
        return httpGetQuery(client, url, headers, paramValues);
    }

    public static Response httpGet(String url)
            throws IOException {
        return httpGetQuery(getHttpClient(), url);
    }

    /**
     * post 请求方式
     * @param url
     * @param headers
     * @param bodyString
     * @return
     * @throws IOException
     */
    public static Response  httpPost(String url , Map<String , String> headers , String bodyString)
            throws IOException {
        Request.Builder requestBuilder = new Request.Builder();
        if(headers != null){
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                requestBuilder.addHeader(entry.getKey(), entry.getValue());
            }
        }
        requestBuilder.post(RequestBody.create(null , bodyString));
        OkHttpClient client = getHttpClient();
        HttpUrl.Builder urlBuilder = HttpUrl.parse(url).newBuilder();
        requestBuilder.url(urlBuilder.build());
        Request request = requestBuilder.build();
        return client.newCall(request).execute();
    }

    public static String getErrorResponseMessage(String service , Integer code , String url){
        return "service "+service+" http request faild.code => " + code + " & url => " + url;
    }
}
