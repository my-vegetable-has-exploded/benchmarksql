package com.github.pgsqlio.benchmarksql.jtpcc;

/**
 * jTPCCApplication - Dummy of the DB specific implementation of the TPC-C Transactions
 */
public class jTPCCApplication {
  public void init(jTPCC gdata, int sut_id) throws Exception {}

  public void finish() throws Exception {}

  public void executeNewOrder(jTPCCTData.NewOrderData screen, boolean trans_rbk, long txn_id) throws Exception {}

  public void executePayment(jTPCCTData.PaymentData screen, long txn_id) throws Exception {}

  public void executeOrderStatus(jTPCCTData.OrderStatusData screen, long txn_id) throws Exception {}

  public void executeStockLevel(jTPCCTData.StockLevelData screen, long txn_id) throws Exception {}

  public void executeDeliveryBG(jTPCCTData.DeliveryBGData screen, long txn_id) throws Exception {}

  public void executeStore(jTPCCTData.StoreData screen, long txn_id) throws Exception {}
}
