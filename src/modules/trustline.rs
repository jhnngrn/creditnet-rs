//! Trustline — bilateral credit with demurrage.
//!
//! Each connected peer gets a `SyncObject<TrustlineData>` holding balance,
//! credit limits, and demurrage state. The module handles:
//! - `set_credit_limit` — one-sided, sets peer's CreditLimitIn
//! - `set_demurrage` — one-sided, sets peer's DemurrageIn
//! - `payment` — direct payment, checked against bandwidth
//!
//! Account API: add_peer, remove_peer, set_credit_limit, set_demurrage, payment.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use mimesis::module::{HandleResult, Module};
use mimesis::registry::RegistryRef;
use mimesis::sync_object::SyncObject;

// ---------------------------------------------------------------------------
// Side constants
// ---------------------------------------------------------------------------

pub const IN: usize = 0;
pub const OUT: usize = 1;

// ---------------------------------------------------------------------------
// Demurrage
// ---------------------------------------------------------------------------

const RATE_BASE: u64 = 1000;
const SECS_PER_YEAR: u64 = 365 * 24 * 3600; // 31_536_000

/// 128-bit intermediate multiply-then-divide to avoid overflow.
fn mul_div(a: u64, b: u64, c: u64) -> u64 {
    let wide = (a as u128) * (b as u128);
    if wide / (c as u128) > u64::MAX as u128 {
        return u64::MAX;
    }
    (wide / c as u128) as u64
}

fn demurrage_decay(abs_balance: u64, rate: u64, elapsed_secs: i64) -> u64 {
    if rate == 0 || elapsed_secs <= 0 || abs_balance == 0 {
        return 0;
    }
    let per_year = mul_div(abs_balance, rate, RATE_BASE);
    let decay = mul_div(per_year, elapsed_secs as u64, SECS_PER_YEAR);
    if decay > abs_balance {
        abs_balance
    } else {
        decay
    }
}

// ---------------------------------------------------------------------------
// TrustlineData — the T in SyncObject<T>
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TrustlineData {
    pub credit_limit: i64,
    pub credit_limit_in: i64,
    pub balance: i64,
    pub demurrage: i64,      // my rate (promille/year)
    pub demurrage_in: i64,   // peer's rate
    pub demurrage_time: i64, // last materialized (secs)
}

impl TrustlineData {
    /// Materialize demurrage decay up to timestamp ts.
    /// Must be called with a shared timestamp (from sync) for consistency.
    pub fn materialize_demurrage(&mut self, ts: i64) {
        if self.demurrage_time == 0 || self.balance == 0 {
            // Never set backwards — a backward timestamp with zero balance
            // could plant a past anchor for future decay.
            if ts > self.demurrage_time {
                self.demurrage_time = ts;
            }
            return;
        }
        let elapsed = ts - self.demurrage_time;
        if elapsed <= 0 {
            return;
        }
        let rate = if self.balance > 0 {
            self.demurrage // peer owes me — my rate
        } else {
            self.demurrage_in // I owe peer — peer's rate
        };
        if rate <= 0 {
            self.demurrage_time = ts;
            return;
        }
        let abs = self.balance.unsigned_abs();
        let decay = demurrage_decay(abs, rate as u64, elapsed);
        if decay >= abs {
            self.balance = 0;
        } else if self.balance > 0 {
            self.balance -= decay as i64;
        } else {
            self.balance += decay as i64;
        }
        self.demurrage_time = ts;
    }

    /// Effective balance after demurrage decay, without modifying state.
    pub fn effective_balance(&self, now_secs: i64) -> i64 {
        if self.demurrage_time == 0 || self.balance == 0 {
            return self.balance;
        }
        let elapsed = now_secs - self.demurrage_time;
        if elapsed <= 0 {
            return self.balance;
        }
        let rate = if self.balance > 0 {
            self.demurrage
        } else {
            self.demurrage_in
        };
        if rate <= 0 {
            return self.balance;
        }
        let abs = self.balance.unsigned_abs();
        let decay = demurrage_decay(abs, rate as u64, elapsed);
        if decay >= abs {
            return 0;
        }
        if self.balance > 0 {
            self.balance - decay as i64
        } else {
            self.balance + decay as i64
        }
    }

    /// Materialize demurrage then apply delta.
    pub fn add_balance(&mut self, delta: i64, ts: i64) {
        self.materialize_demurrage(ts);
        self.balance += delta;
    }
}

// ---------------------------------------------------------------------------
// Receipts & ClosedBalance
// ---------------------------------------------------------------------------

const MAX_RECEIPTS: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    pub peer: String,
    pub amount: i64,
    pub status: bool,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedBalance {
    pub peer: String,
    pub balance: i64,
    pub time: i64,
}

// ---------------------------------------------------------------------------
// Reserver trait — child modules report reserved capacity
// ---------------------------------------------------------------------------

pub trait Reserver: Send + Sync {
    fn reserved(&self, peer: &str, side: usize) -> i64;
}

// ---------------------------------------------------------------------------
// Trustline module
// ---------------------------------------------------------------------------

/// Factory for creating SyncObjects. Provided by the framework layer.
pub type NewSyncObjectFn = Box<dyn Fn(&str) -> SyncObject<TrustlineData> + Send + Sync>;

/// Callback to initiate a connect via the framework.
pub type ConnectFn = Box<dyn Fn(&str, &str) -> Result<(), Box<dyn std::error::Error>> + Send + Sync>;

/// Callback to initiate a close via the framework.
pub type CloseFn = Box<dyn Fn(&str) -> Result<(), Box<dyn std::error::Error>> + Send + Sync>;

pub struct Trustline {
    pub trustlines: HashMap<String, SyncObject<TrustlineData>>,
    pub closed_balances: Vec<ClosedBalance>,
    pub receipts: Vec<Receipt>,
    pub name: String,
    pub node_id: String,
    registry: Option<RegistryRef>,
    new_sync_object_fn: Option<NewSyncObjectFn>,
    reservers: Vec<Box<dyn Reserver>>,
}

impl Trustline {
    pub fn new(name: String, node_id: String, registry: RegistryRef) -> Self {
        Trustline {
            trustlines: HashMap::new(),
            closed_balances: Vec::new(),
            receipts: Vec::new(),
            name,
            node_id,
            registry: Some(registry),
            new_sync_object_fn: None,
            reservers: Vec::new(),
        }
    }

    /// Create a minimal instance for testing.
    pub fn for_test(name: &str, node_id: &str) -> Self {
        Trustline {
            trustlines: HashMap::new(),
            closed_balances: Vec::new(),
            receipts: Vec::new(),
            name: name.to_string(),
            node_id: node_id.to_string(),
            registry: None,
            new_sync_object_fn: None,
            reservers: Vec::new(),
        }
    }

    pub fn set_new_sync_object_fn(&mut self, f: NewSyncObjectFn) {
        self.new_sync_object_fn = Some(f);
    }

    pub fn register_reserver(&mut self, r: Box<dyn Reserver>) {
        self.reservers.push(r);
    }

    fn new_sync_object(&self, peer: &str) -> SyncObject<TrustlineData> {
        if let Some(ref f) = self.new_sync_object_fn {
            return f(peer);
        }
        SyncObject::without_registry(TrustlineData::default(), peer)
    }

    fn add_receipt(&mut self, peer: &str, amount: i64, ok: bool, ts: i64) {
        if self.receipts.len() >= MAX_RECEIPTS {
            self.receipts.remove(0);
        }
        self.receipts.push(Receipt {
            peer: peer.to_string(),
            amount,
            status: ok,
            timestamp: ts,
        });
    }

    // -- bandwidth / pending balance --

    /// Effective balance minus reservations from child modules.
    /// Positive = debt exists: peer owes us (OUT) or we owe them (IN).
    pub fn pending_balance(&self, peer: &str, side: usize) -> i64 {
        let tl = match self.trustlines.get(peer) {
            Some(tl) => tl,
            None => return 0,
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let eff = tl.effective_balance(now);
        let mut debt = if side == OUT { eff } else { -eff };
        for r in &self.reservers {
            debt -= r.reserved(peer, side);
        }
        debt
    }

    /// Available capacity: credit_limit + pending_balance, floored at 0.
    pub fn bandwidth(&self, peer: &str, side: usize) -> i64 {
        let tl = match self.trustlines.get(peer) {
            Some(tl) => tl,
            None => return 0,
        };
        let limit = if side == OUT {
            tl.credit_limit_in
        } else {
            tl.credit_limit
        };
        let total = limit + self.pending_balance(peer, side);
        if total < 0 { 0 } else { total }
    }

    /// Check if a peer has a connected trustline.
    pub fn is_connected(&self, peer: &str) -> bool {
        self.trustlines.contains_key(peer)
    }

    /// List all connected peers.
    pub fn connected_peers(&self) -> Vec<String> {
        self.trustlines.keys().cloned().collect()
    }

    // -- sync request handlers --

    fn req_set_credit_limit(
        &mut self,
        peer: &str,
        payload: Option<Value>,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        let amount = parse_amount(&payload)?;
        let tl = self.trustlines.get_mut(peer)
            .ok_or("peer not connected")?;
        if amount < 0 {
            return Ok(HandleResult { ok: false });
        }
        tl.credit_limit_in = amount;
        Ok(HandleResult { ok: true })
    }

    fn req_set_demurrage(
        &mut self,
        peer: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        let rate = parse_rate(&payload)?;
        let tl = self.trustlines.get_mut(peer)
            .ok_or("peer not connected")?;
        if rate < 0 {
            return Ok(HandleResult { ok: false });
        }
        tl.materialize_demurrage(ts);
        tl.demurrage_in = rate;
        Ok(HandleResult { ok: true })
    }

    fn req_payment(
        &mut self,
        peer: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        let amount = parse_amount(&payload)?;
        if amount <= 0 {
            return Ok(HandleResult { ok: false });
        }
        if amount > self.bandwidth(peer, IN) {
            return Ok(HandleResult { ok: false });
        }
        let tl = self.trustlines.get_mut(peer)
            .ok_or("no trustline")?;
        tl.add_balance(amount, ts);
        self.add_receipt(peer, amount, true, ts);
        Ok(HandleResult { ok: true })
    }

    // -- sync response handlers --

    fn resp_set_credit_limit(&mut self, peer: &str, payload: Option<Value>, ok: bool) {
        if !ok { return; }
        let amount = match parse_amount(&payload) {
            Ok(a) => a,
            Err(_) => return,
        };
        if let Some(tl) = self.trustlines.get_mut(peer) {
            tl.credit_limit = amount;
        }
    }

    fn resp_set_demurrage(&mut self, peer: &str, payload: Option<Value>, ok: bool, ts: i64) {
        if !ok { return; }
        let rate = match parse_rate(&payload) {
            Ok(r) => r,
            Err(_) => return,
        };
        if let Some(tl) = self.trustlines.get_mut(peer) {
            tl.materialize_demurrage(ts);
            tl.demurrage = rate;
        }
    }

    fn resp_payment(&mut self, peer: &str, payload: Option<Value>, ok: bool, ts: i64) {
        let amount = match parse_amount(&payload) {
            Ok(a) => a,
            Err(_) => return,
        };
        if ok {
            if let Some(tl) = self.trustlines.get_mut(peer) {
                tl.add_balance(-amount, ts);
            }
        }
        self.add_receipt(peer, -amount, ok, ts);
    }

    // -- account API --

    fn account_set_credit_limit(
        &self,
        payload: Option<Value>,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        let (peer, amount) = parse_peer_amount(&payload)?;
        let tl = self.trustlines.get(&peer)
            .ok_or_else(|| format!("peer {} is not connected", peer))?;
        if amount < 0 {
            return Err("credit limit must be non-negative".into());
        }
        tl.send("set_credit_limit", Some(serde_json::json!({"amount": amount})));
        Ok(Some(serde_json::json!("ok")))
    }

    fn account_set_demurrage(
        &self,
        payload: Option<Value>,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        let (peer, rate) = parse_peer_rate(&payload)?;
        let tl = self.trustlines.get(&peer)
            .ok_or_else(|| format!("peer {} is not connected", peer))?;
        if rate < 0 {
            return Err("demurrage rate must be non-negative".into());
        }
        tl.send("set_demurrage", Some(serde_json::json!({"rate": rate})));
        Ok(Some(serde_json::json!("ok")))
    }

    fn account_payment(
        &self,
        payload: Option<Value>,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        let (peer, amount) = parse_peer_amount(&payload)?;
        if amount <= 0 {
            return Err("amount must be positive".into());
        }
        let _tl = self.trustlines.get(&peer)
            .ok_or_else(|| format!("peer {} has no trustline", peer))?;
        if amount > self.bandwidth(&peer, OUT) {
            return Err("insufficient credit".into());
        }
        let tl = self.trustlines.get(&peer).unwrap();
        tl.send("payment", Some(serde_json::json!({"amount": amount})));
        Ok(Some(serde_json::json!("ok")))
    }
}

// ---------------------------------------------------------------------------
// Module trait
// ---------------------------------------------------------------------------

impl Module for Trustline {
    // Default connects_on = "connect", closes_on = ["close"] — fine for trustline.

    fn handle_request(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        match method {
            "connect" => {
                // Create trustline on connect.
                if self.trustlines.contains_key(peer) {
                    return Err(format!("peer {} already connected", peer).into());
                }
                let so = self.new_sync_object(peer);
                self.trustlines.insert(peer.to_string(), so);
                Ok(HandleResult { ok: true })
            }
            "close" => {
                // Archive balance and clean up.
                if let Some(tl) = self.trustlines.remove(peer) {
                    if tl.balance != 0 {
                        self.closed_balances.push(ClosedBalance {
                            peer: peer.to_string(),
                            balance: tl.balance,
                            time: ts,
                        });
                    }
                    // Drop cleans up queue via SyncObject::drop
                }
                Ok(HandleResult { ok: true })
            }
            "set_credit_limit" => self.req_set_credit_limit(peer, payload),
            "set_demurrage" => self.req_set_demurrage(peer, payload, ts),
            "payment" => self.req_payment(peer, payload, ts),
            _ => Err("unknown method".into()),
        }
    }

    fn handle_response(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ok: bool,
        ts: i64,
    ) {
        match method {
            "connect" => {
                // Response side: peer accepted our connect.
                if ok && !self.trustlines.contains_key(peer) {
                    let so = self.new_sync_object(peer);
                    self.trustlines.insert(peer.to_string(), so);
                }
            }
            "close" => {
                // Response side: peer accepted our close.
                if let Some(tl) = self.trustlines.remove(peer) {
                    if tl.balance != 0 {
                        self.closed_balances.push(ClosedBalance {
                            peer: peer.to_string(),
                            balance: tl.balance,
                            time: ts,
                        });
                    }
                }
            }
            "set_credit_limit" => self.resp_set_credit_limit(peer, payload, ok),
            "set_demurrage" => self.resp_set_demurrage(peer, payload, ok, ts),
            "payment" => self.resp_payment(peer, payload, ok, ts),
            _ => {}
        }
    }

    fn handle_account(
        &mut self,
        method: &str,
        payload: Option<Value>,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        match method {
            "set_credit_limit" => self.account_set_credit_limit(payload),
            "set_demurrage" => self.account_set_demurrage(payload),
            "payment" => self.account_payment(payload),
            "status" => Ok(self.status()),
            _ => Err("unknown method".into()),
        }
    }

    fn status(&self) -> Option<Value> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let peers: serde_json::Map<String, Value> = self.trustlines.iter()
            .map(|(id, tl)| {
                let info = serde_json::json!({
                    "credit_limit": tl.credit_limit,
                    "credit_limit_in": tl.credit_limit_in,
                    "balance": tl.balance,
                    "effective_balance": tl.effective_balance(now),
                    "demurrage": tl.demurrage,
                    "demurrage_in": tl.demurrage_in,
                });
                (id.clone(), info)
            })
            .collect();
        Some(serde_json::json!({
            "peers": peers,
            "receipts": self.receipts,
            "closed_balances": self.closed_balances,
        }))
    }

    fn force_remove(&mut self, peer: &str) {
        // Same as close — archive and remove.
        if let Some(tl) = self.trustlines.remove(peer) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            if tl.balance != 0 {
                self.closed_balances.push(ClosedBalance {
                    peer: peer.to_string(),
                    balance: tl.balance,
                    time: now,
                });
            }
        }
    }

    fn reattach_sync_objects(&mut self, registry: &RegistryRef) {
        self.registry = Some(registry.clone());
        let name = self.name.clone();
        for tl in self.trustlines.values_mut() {
            tl.reattach(registry, &name);
        }
    }
}

// ---------------------------------------------------------------------------
// Payload parsing helpers
// ---------------------------------------------------------------------------

fn parse_amount(payload: &Option<Value>) -> Result<i64, Box<dyn std::error::Error>> {
    let v = payload.as_ref().ok_or("missing payload")?;
    v.get("amount")
        .and_then(|a| a.as_i64())
        .ok_or_else(|| "missing or invalid amount".into())
}

fn parse_rate(payload: &Option<Value>) -> Result<i64, Box<dyn std::error::Error>> {
    let v = payload.as_ref().ok_or("missing payload")?;
    v.get("rate")
        .and_then(|r| r.as_i64())
        .ok_or_else(|| "missing or invalid rate".into())
}

fn parse_peer_amount(payload: &Option<Value>) -> Result<(String, i64), Box<dyn std::error::Error>> {
    let v = payload.as_ref().ok_or("missing payload")?;
    let peer = v.get("peer")
        .and_then(|p| p.as_str())
        .ok_or("missing peer")?
        .to_lowercase();
    let amount = v.get("amount")
        .and_then(|a| a.as_i64())
        .ok_or("missing amount")?;
    Ok((peer, amount))
}

fn parse_peer_rate(payload: &Option<Value>) -> Result<(String, i64), Box<dyn std::error::Error>> {
    let v = payload.as_ref().ok_or("missing payload")?;
    let peer = v.get("peer")
        .and_then(|p| p.as_str())
        .ok_or("missing peer")?
        .to_lowercase();
    let rate = v.get("rate")
        .and_then(|r| r.as_i64())
        .ok_or("missing rate")?;
    Ok((peer, rate))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- demurrage tests --

    #[test]
    fn demurrage_zero_rate_no_decay() {
        let mut d = TrustlineData {
            balance: 1000,
            demurrage: 0,
            demurrage_in: 0,
            demurrage_time: 100,
            ..Default::default()
        };
        d.materialize_demurrage(100 + SECS_PER_YEAR as i64);
        assert_eq!(d.balance, 1000);
    }

    #[test]
    fn demurrage_positive_balance_decays() {
        // 10 promille/year = 1%/year. Balance 10000, 1 year elapsed.
        let mut d = TrustlineData {
            balance: 10000,
            demurrage: 10, // 1%/year
            demurrage_time: 1000, // anchor already set
            ..Default::default()
        };
        d.materialize_demurrage(1000 + SECS_PER_YEAR as i64);
        // decay = 10000 * 10 / 1000 = 100 per year
        assert_eq!(d.balance, 9900);
    }

    #[test]
    fn demurrage_negative_balance_uses_peer_rate() {
        let mut d = TrustlineData {
            balance: -10000,
            demurrage: 0,
            demurrage_in: 20, // 2%/year
            demurrage_time: 1000,
            ..Default::default()
        };
        d.materialize_demurrage(1000 + SECS_PER_YEAR as i64);
        // decay = 10000 * 20 / 1000 = 200
        assert_eq!(d.balance, -9800);
    }

    #[test]
    fn effective_balance_does_not_mutate() {
        let d = TrustlineData {
            balance: 10000,
            demurrage: 10,
            demurrage_time: 1000,
            ..Default::default()
        };
        let eff = d.effective_balance(1000 + SECS_PER_YEAR as i64);
        assert_eq!(eff, 9900);
        assert_eq!(d.balance, 10000); // unchanged
    }

    #[test]
    fn add_balance_materializes_first() {
        let mut d = TrustlineData {
            balance: 10000,
            demurrage: 10,
            demurrage_time: 1000,
            ..Default::default()
        };
        d.add_balance(500, 1000 + SECS_PER_YEAR as i64);
        // After demurrage: 9900, then +500 = 10400
        assert_eq!(d.balance, 10400);
    }

    #[test]
    fn demurrage_caps_at_zero() {
        let mut d = TrustlineData {
            balance: 100,
            demurrage: 1000, // 100%/year
            demurrage_time: 1000,
            ..Default::default()
        };
        // 2 years at 100% — should cap at 0, not go negative.
        d.materialize_demurrage(1000 + 2 * SECS_PER_YEAR as i64);
        assert_eq!(d.balance, 0);
    }

    #[test]
    fn backward_timestamp_ignored() {
        let mut d = TrustlineData {
            balance: 0,
            demurrage_time: 1000,
            ..Default::default()
        };
        // With zero balance, backward timestamp should not overwrite.
        d.materialize_demurrage(500);
        assert_eq!(d.demurrage_time, 1000);
    }

    // -- trustline module tests --

    fn make_test_trustline() -> Trustline {
        Trustline::for_test("trustline", "alice@a.com")
    }

    fn insert_peer(t: &mut Trustline, peer: &str, balance: i64, limit: i64, limit_in: i64) {
        let so = SyncObject::without_registry(TrustlineData {
            credit_limit: limit,
            credit_limit_in: limit_in,
            balance,
            ..Default::default()
        }, peer);
        t.trustlines.insert(peer.to_string(), so);
    }

    #[test]
    fn connect_creates_trustline() {
        let mut t = make_test_trustline();
        let result = t.handle_request("bob@b.com", "connect", None, 100).unwrap();
        assert!(result.ok);
        assert!(t.trustlines.contains_key("bob@b.com"));
    }

    #[test]
    fn connect_duplicate_rejected() {
        let mut t = make_test_trustline();
        t.handle_request("bob@b.com", "connect", None, 100).unwrap();
        let err = t.handle_request("bob@b.com", "connect", None, 200);
        assert!(err.is_err());
    }

    #[test]
    fn close_removes_trustline() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 0, 0, 0);
        let result = t.handle_request("bob@b.com", "close", None, 100).unwrap();
        assert!(result.ok);
        assert!(!t.trustlines.contains_key("bob@b.com"));
    }

    #[test]
    fn close_archives_nonzero_balance() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 500, 0, 0);
        t.handle_request("bob@b.com", "close", None, 100).unwrap();
        assert_eq!(t.closed_balances.len(), 1);
        assert_eq!(t.closed_balances[0].balance, 500);
    }

    #[test]
    fn close_zero_balance_no_archive() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 0, 0, 0);
        t.handle_request("bob@b.com", "close", None, 100).unwrap();
        assert!(t.closed_balances.is_empty());
    }

    #[test]
    fn set_credit_limit_request() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 0, 0, 0);
        let payload = serde_json::json!({"amount": 1000});
        let result = t.handle_request("bob@b.com", "set_credit_limit", Some(payload), 0).unwrap();
        assert!(result.ok);
        assert_eq!(t.trustlines["bob@b.com"].credit_limit_in, 1000);
    }

    #[test]
    fn set_credit_limit_response_ok() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 0, 0, 0);
        let payload = serde_json::json!({"amount": 500});
        t.handle_response("bob@b.com", "set_credit_limit", Some(payload), true, 0);
        assert_eq!(t.trustlines["bob@b.com"].credit_limit, 500);
    }

    #[test]
    fn set_credit_limit_response_nack_no_change() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 0, 0, 0);
        let payload = serde_json::json!({"amount": 500});
        t.handle_response("bob@b.com", "set_credit_limit", Some(payload), false, 0);
        assert_eq!(t.trustlines["bob@b.com"].credit_limit, 0);
    }

    #[test]
    fn payment_request_within_bandwidth() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 0, 1000, 0);
        let payload = serde_json::json!({"amount": 500});
        let result = t.handle_request("bob@b.com", "payment", Some(payload), 100).unwrap();
        assert!(result.ok);
        assert_eq!(t.trustlines["bob@b.com"].balance, 500);
        assert_eq!(t.receipts.len(), 1);
    }

    #[test]
    fn payment_request_exceeds_bandwidth() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 0, 100, 0);
        let payload = serde_json::json!({"amount": 500});
        let result = t.handle_request("bob@b.com", "payment", Some(payload), 100).unwrap();
        assert!(!result.ok);
        assert_eq!(t.trustlines["bob@b.com"].balance, 0);
    }

    #[test]
    fn payment_response_ok_subtracts() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 0, 0, 0);
        let payload = serde_json::json!({"amount": 300});
        t.handle_response("bob@b.com", "payment", Some(payload), true, 100);
        assert_eq!(t.trustlines["bob@b.com"].balance, -300);
        assert_eq!(t.receipts.len(), 1);
        assert_eq!(t.receipts[0].amount, -300);
    }

    #[test]
    fn bandwidth_calculation() {
        let mut t = make_test_trustline();
        // credit_limit = 1000 (IN side), balance = 200 (peer owes us)
        insert_peer(&mut t, "bob@b.com", 200, 1000, 500);

        // IN bandwidth: credit_limit + pending_balance(IN)
        // pending_balance(IN) = -eff = -200
        // bw = 1000 + (-200) = 800
        assert_eq!(t.bandwidth("bob@b.com", IN), 800);

        // OUT bandwidth: credit_limit_in + pending_balance(OUT)
        // pending_balance(OUT) = eff = 200
        // bw = 500 + 200 = 700
        assert_eq!(t.bandwidth("bob@b.com", OUT), 700);
    }

    #[test]
    fn bandwidth_floors_at_zero() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", -2000, 1000, 0);
        // IN: limit=1000, pending = -(-2000) = 2000, total=3000
        assert_eq!(t.bandwidth("bob@b.com", IN), 3000);
        // OUT: limit=0, pending = -2000, total=-2000 → 0
        assert_eq!(t.bandwidth("bob@b.com", OUT), 0);
    }

    #[test]
    fn force_remove_archives() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 750, 0, 0);
        t.force_remove("bob@b.com");
        assert!(!t.trustlines.contains_key("bob@b.com"));
        assert_eq!(t.closed_balances.len(), 1);
        assert_eq!(t.closed_balances[0].balance, 750);
    }

    #[test]
    fn set_demurrage_request() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 0, 0, 0);
        let payload = serde_json::json!({"rate": 10});
        let result = t.handle_request("bob@b.com", "set_demurrage", Some(payload), 1000).unwrap();
        assert!(result.ok);
        assert_eq!(t.trustlines["bob@b.com"].demurrage_in, 10);
    }

    #[test]
    fn set_demurrage_response_ok() {
        let mut t = make_test_trustline();
        insert_peer(&mut t, "bob@b.com", 0, 0, 0);
        let payload = serde_json::json!({"rate": 20});
        t.handle_response("bob@b.com", "set_demurrage", Some(payload), true, 1000);
        assert_eq!(t.trustlines["bob@b.com"].demurrage, 20);
    }

    #[test]
    fn connect_response_creates_trustline() {
        let mut t = make_test_trustline();
        t.handle_response("bob@b.com", "connect", None, true, 100);
        assert!(t.trustlines.contains_key("bob@b.com"));
    }

    #[test]
    fn connect_response_nack_no_trustline() {
        let mut t = make_test_trustline();
        t.handle_response("bob@b.com", "connect", None, false, 100);
        assert!(!t.trustlines.contains_key("bob@b.com"));
    }
}
