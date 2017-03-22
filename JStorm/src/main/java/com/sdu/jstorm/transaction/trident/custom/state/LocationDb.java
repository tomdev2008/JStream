package com.sdu.jstorm.transaction.trident.custom.state;

import org.apache.storm.trident.state.State;

import java.util.List;

/**
 * Location State
 * */
public class LocationDb implements State {

    // current location state transaction id
    private long _curTxId;

    // last location state update transaction id
    private long _lastTxId;

    @Override
    public void beginCommit(Long txid) {
        this._curTxId = txid;
    }

    @Override
    public void commit(Long txid) {
        this._lastTxId = txid;
    }

    public void setBulkLocation(List<Long> userIds, List<String> locations) {
        // code to access database and set location(check current transaction id _value)
        StringBuffer sb = new StringBuffer();
        sb.append("_curTxId=").append(_curTxId).append("\t");
        sb.append("_lastTxId=").append(_lastTxId).append("\t");
        sb.append("userLocation=[");
        boolean first = true;
        for (int i = 0; i < userIds.size(); i++) {
            if (first) {
                sb.append(userIds.get(i)).append(":").append(locations.get(i));
                first = false;
            } else {
                sb.append(",").append(userIds.get(i)).append(":").append(locations.get(i));
            }
        }
        sb.append("]");
        System.out.println(sb.toString());
    }

    public List<String> getBulkLocation(List<Long> userIds) {
        // code to get location from database
        return null;
    }
}
