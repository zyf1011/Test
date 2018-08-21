package com.huike.test00;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class log {//bean，将数据封装，转化为string
    private String remote_add;
    private String remote_user;
    private String time_local;
    private String request;
    private String statues;
    private String body_bytes_sent;
    private String http_referer;
    private String http_user_agent;
    private boolean valid = true;

    public String toString(){
        StringBuilder sb = new StringBuilder();

        sb.append("valid:"+this.valid);
        sb.append("\nremote:_addr:"+this.remote_add);
        sb.append("\nremote_user:"+this.remote_user);
        sb.append("\ntime_local:"+this.time_local);
        sb.append("\request:"+this.request);
        sb.append("\nstatues:"+this.statues);
        sb.append("\nbody_statues:"+this.body_bytes_sent);
        sb.append("\nhttp_referer:"+this.http_referer);
        sb.append("\nhttp_user_agent:"+this.http_user_agent);
        return sb.toString();
    }

    public String getRemote_add() {
        return remote_add;
    }

    public void setRemote_add(String remote_add) {
        this.remote_add = remote_add;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatues() {
        return statues;
    }

    public void setStatues(String statues) {
        this.statues = statues;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }
    public Date getTime_local_Date(){
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
    }
}
