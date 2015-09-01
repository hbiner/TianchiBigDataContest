package com.aliyun.odps.mapred;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.mapred.cli.OdpsConf;
import com.aliyun.odps.mapred.conf.SessionState;

/**
 * This is a LocalDriver
 */
public class LocalDriver {

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: TestDriver <mainclass> <tablename> [args]");
      System.exit(2);
    }
    OdpsConf odpsConf = new OdpsConf();
    Account account = new AliyunAccount(odpsConf.getAccessId(), odpsConf.getAccessKey());
    Odps odps = new Odps(account);
    if (odpsConf.getEndpoint() != null) {
      odps.setEndpoint(odpsConf.getEndpoint());
    }
    SessionState ss = SessionState.get();
    ss.setOdps(odps);
    ss.setLocalRun(true);
    ss.getDefaultJob().set("odps.mapred.temp.dir", "mr_local_jobs");
    ss.getDefaultJob().set("odps.mapred.temp.retain", "true");
    String tablename = args[1];
    odps.setDefaultProject(tablename);
    try {
      Method main = Class.forName(args[0]).getMethod("main", String[].class);
      main.invoke(null, (Object) Arrays.copyOfRange(args, 2, args.length));
    } catch (InvocationTargetException e) {
      Throwable t = e.getCause();
      if (t != null && t instanceof OdpsException) {
        OdpsException ex = (OdpsException) t;
        System.err.println(ex.getMessage());
        System.exit(-1);
      } else {
        throw new RuntimeException("Unknown error", e);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
