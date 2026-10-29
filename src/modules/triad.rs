//! Triad — three-party novation to eliminate circular debt.
//!
//! A fully functional module (not a demo), meant to be simple.
//! Each Novation has two SyncObject sides (IN and OUT), one per peer.
//! Drop on the SyncObject cleans up that side's outbox queue and
//! decrements the ref count on the ConnectionState.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use mimesis::link::RefCountHandle;
use mimesis::module::{HandleResult, Module};
use mimesis::registry::RegistryRef;
use mimesis::sync_object::SyncObject;

/// Directional constants matching creditnet convention.
pub const IN: usize = 0;
pub const OUT: usize = 1;

/// Maximum concurrent novations per module instance.
pub const MAX_NOVATIONS: usize = 9;

// ---------------------------------------------------------------------------
// Novation state
// ---------------------------------------------------------------------------

/// A single three-party novation in progress.
/// Each side has its own SyncObject — a 1:1 relationship with the peer.
#[derive(Serialize, Deserialize)]
pub struct Novation {
    pub amount: i64,
    /// SyncObject handles: [IN, OUT]. Each bound to one peer.
    pub side: [Option<SyncObject<()>>; 2],
    #[serde(default, skip_serializing_if = "is_false")]
    pub initiator: bool,
}

impl Novation {
    /// Peer name for a side, or "" if that side is empty.
    pub fn peer(&self, side: usize) -> &str {
        self.side[side].as_ref().map(|s| s.peer()).unwrap_or("")
    }

    /// True if a side has a SyncObject.
    pub fn has_side(&self, side: usize) -> bool {
        self.side[side].is_some()
    }
}

fn is_false(v: &bool) -> bool {
    !*v
}

// ---------------------------------------------------------------------------
// Wire messages
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct CommitMsg {
    novation_id: String,
    amount: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    next: String,
}

#[derive(Serialize, Deserialize)]
struct FinalizeMsg {
    novation_id: String,
}

#[derive(Serialize, Deserialize)]
struct RollbackMsg {
    novation_id: String,
}

// ---------------------------------------------------------------------------
// External dependencies (provided by the trustline layer)
// ---------------------------------------------------------------------------

/// Callback: available bandwidth on a peer in a direction.
pub type BandwidthFn = Box<dyn Fn(&str, usize) -> i64 + Send + Sync>;

/// Callback: pending balance on a peer in a direction.
pub type PendingBalanceFn = Box<dyn Fn(&str, usize) -> i64 + Send + Sync>;

/// Callback: add balance on a peer's trustline.
pub type AddBalanceFn = Box<dyn Fn(&str, i64, i64) + Send + Sync>;

/// Callback: check if a module is connected on a peer.
pub type IsConnectedFn = Box<dyn Fn(&str) -> bool + Send + Sync>;

/// Callback: list connected peers.
pub type ConnectedPeersFn = Box<dyn Fn() -> Vec<String> + Send + Sync>;

/// Callback: get RefCountHandle for a peer on this module.
/// When wired up, returns the ConnectionState's ref_count for module+peer.
pub type RefCountFn = Box<dyn Fn(&str) -> RefCountHandle + Send + Sync>;

// ---------------------------------------------------------------------------
// Triad module
// ---------------------------------------------------------------------------

/// Triad implements Module. Each novation has two SyncObject sides,
/// one per peer. Removing a side drops the SyncObject, cleaning up
/// its queue and decrementing the ref count.
pub struct Triad {
    pub novations: HashMap<String, Novation>,
    /// Registry reference for creating SyncObjects.
    registry: Option<RegistryRef>,
    /// Callback for getting RefCountHandles. When None, uses dummy counters.
    ref_count_fn: Option<RefCountFn>,
    pub name: String,
    pub node_id: String,
    bandwidth: Option<BandwidthFn>,
    pending_balance: Option<PendingBalanceFn>,
    add_balance: Option<AddBalanceFn>,
    is_connected: Option<IsConnectedFn>,
    connected_peers: Option<ConnectedPeersFn>,
}

impl Triad {
    /// Create a new Triad with full dependencies.
    pub fn new(
        name: String,
        node_id: String,
        registry: RegistryRef,
        bandwidth: BandwidthFn,
        pending_balance: PendingBalanceFn,
        add_balance: AddBalanceFn,
        is_connected: IsConnectedFn,
        connected_peers: ConnectedPeersFn,
    ) -> Self {
        Triad {
            novations: HashMap::new(),
            registry: Some(registry),
            ref_count_fn: None,
            name,
            node_id,
            bandwidth: Some(bandwidth),
            pending_balance: Some(pending_balance),
            add_balance: Some(add_balance),
            is_connected: Some(is_connected),
            connected_peers: Some(connected_peers),
        }
    }

    /// Create a minimal Triad for testing (no external deps).
    pub fn for_test(name: &str, node_id: &str) -> Self {
        Triad {
            novations: HashMap::new(),
            registry: None,
            ref_count_fn: None,
            name: name.to_string(),
            node_id: node_id.to_string(),
            bandwidth: None,
            pending_balance: None,
            add_balance: None,
            is_connected: None,
            connected_peers: None,
        }
    }

    /// Set the ref count callback.
    pub fn set_ref_count_fn(&mut self, f: RefCountFn) {
        self.ref_count_fn = Some(f);
    }

    /// Create a new SyncObject bound to a peer.
    fn new_sync_object(&self, peer: &str) -> SyncObject<()> {
        match &self.registry {
            Some(reg) => {
                let rc = self.get_ref_count(peer);
                SyncObject::new((), peer, reg, &self.name, rc)
            }
            None => SyncObject::without_registry((), peer),
        }
    }

    /// Get the RefCountHandle for a peer. Uses the callback if set,
    /// otherwise returns a standalone counter.
    fn get_ref_count(&self, peer: &str) -> RefCountHandle {
        self.ref_count_fn
            .as_ref()
            .map(|f| f(peer))
            .unwrap_or_else(|| Arc::new(AtomicUsize::new(0)))
    }

    /// How much capacity is reserved by in-flight novations on a peer+side.
    pub fn reserved(&self, peer: &str, side: usize) -> i64 {
        self.novations
            .values()
            .filter(|nov| nov.peer(side) == peer)
            .map(|nov| nov.amount)
            .sum()
    }

    // -- helpers --

    fn check_connected(&self, peer: &str) -> bool {
        self.is_connected.as_ref().map_or(false, |f| f(peer))
    }

    fn get_bandwidth(&self, peer: &str, side: usize) -> i64 {
        self.bandwidth.as_ref().map_or(0, |f| f(peer, side))
    }

    fn get_pending_balance(&self, peer: &str, side: usize) -> i64 {
        self.pending_balance.as_ref().map_or(0, |f| f(peer, side))
    }

    fn do_add_balance(&self, peer: &str, amount: i64, ts: i64) {
        if let Some(ref f) = self.add_balance {
            f(peer, amount, ts);
        }
    }

    /// Send a message via a novation's side SyncObject.
    fn send_side(&self, nov_id: &str, side: usize, method: &str, payload: Value) {
        if let Some(nov) = self.novations.get(nov_id) {
            if let Some(ref so) = nov.side[side] {
                so.send(method, Some(payload));
            }
        }
    }

    /// Close a novation side — Drop cleans up the queue and ref count.
    fn remove_side(&mut self, nov_id: &str, side: usize) {
        if let Some(nov) = self.novations.get_mut(nov_id) {
            nov.side[side] = None;
        }
    }

    // -- request handlers --

    fn req_commit(
        &mut self,
        peer: &str,
        payload: Option<Value>,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        let msg: CommitMsg = match payload {
            Some(v) => serde_json::from_value(v)?,
            None => return Err("bad payload".into()),
        };
        if msg.amount <= 0 {
            return Ok(HandleResult { ok: false });
        }

        // Confirmation back to initiator — novation already exists.
        if self.novations.contains_key(&msg.novation_id) {
            let nov = self.novations.get(&msg.novation_id).unwrap();
            if nov.has_side(IN) {
                return Err(format!(
                    "novation {} already confirmed",
                    msg.novation_id
                )
                .into());
            }
            if self.get_pending_balance(peer, IN) < nov.amount {
                return Ok(HandleResult { ok: false });
            }
            // Create IN side SyncObject for this peer.
            let so_in = self.new_sync_object(peer);
            let nov = self.novations.get_mut(&msg.novation_id).unwrap();
            nov.side[IN] = Some(so_in);
            // Send finalize to OUT peer.
            let finalize =
                serde_json::to_value(FinalizeMsg {
                    novation_id: msg.novation_id.clone(),
                })
                .unwrap();
            self.send_side(&msg.novation_id, OUT, "finalize", finalize);
            return Ok(HandleResult { ok: true });
        }

        // New commit — verify connectivity and bandwidth.
        if msg.next.is_empty() || !self.check_connected(&msg.next) {
            return Ok(HandleResult { ok: false });
        }
        if self.novations.len() >= MAX_NOVATIONS {
            return Ok(HandleResult { ok: false });
        }
        let bw_in = self.get_bandwidth(peer, IN);
        let bw_out = self.get_bandwidth(&msg.next, OUT);
        if bw_in < msg.amount || bw_out < msg.amount {
            return Ok(HandleResult { ok: false });
        }

        let so_in = self.new_sync_object(peer);
        let so_out = self.new_sync_object(&msg.next);
        let nov = Novation {
            amount: msg.amount,
            side: [Some(so_in), Some(so_out)],
            initiator: false,
        };
        self.novations.insert(msg.novation_id.clone(), nov);

        let fwd = serde_json::to_value(CommitMsg {
            novation_id: msg.novation_id.clone(),
            amount: msg.amount,
            next: peer.to_string(),
        })
        .unwrap();
        self.send_side(&msg.novation_id, OUT, "commit", fwd);
        Ok(HandleResult { ok: true })
    }

    fn resp_commit(&mut self, peer: &str, payload: Option<Value>, ok: bool) {
        let msg: CommitMsg = match payload.and_then(|v| serde_json::from_value(v).ok()) {
            Some(m) => m,
            None => return,
        };
        if !ok {
            let in_empty = self.novations.get(&msg.novation_id)
                .map(|nov| !nov.has_side(IN))
                .unwrap_or(true);
            if in_empty {
                self.novations.remove(&msg.novation_id); // Drop cleans up both sides.
                return;
            }
            let rollback = serde_json::to_value(RollbackMsg {
                novation_id: msg.novation_id.clone(),
            })
            .unwrap();
            self.send_side(&msg.novation_id, IN, "rollback", rollback);
            return;
        }
        // ACK — OUT side already has the right peer from new_sync_object at creation.
        let _ = peer;
    }

    fn req_finalize(
        &mut self,
        peer: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        let msg: FinalizeMsg = match payload {
            Some(v) => serde_json::from_value(v)?,
            None => return Err("bad payload".into()),
        };
        let amount = {
            let nov = self.novations.get(&msg.novation_id)
                .ok_or_else(|| format!("novation {} not found", msg.novation_id))?;
            if nov.peer(IN) != peer {
                return Err("wrong peer".into());
            }
            nov.amount
        };

        self.do_add_balance(peer, amount, ts);
        self.remove_side(&msg.novation_id, IN);

        let has_out = self.novations.get(&msg.novation_id)
            .map(|nov| nov.has_side(OUT))
            .unwrap_or(false);
        if has_out {
            let fwd = serde_json::to_value(FinalizeMsg {
                novation_id: msg.novation_id.clone(),
            })
            .unwrap();
            self.send_side(&msg.novation_id, OUT, "finalize", fwd);
        } else {
            self.novations.remove(&msg.novation_id);
        }
        Ok(HandleResult { ok: true })
    }

    fn resp_finalize(&mut self, peer: &str, payload: Option<Value>, ts: i64) {
        let msg: FinalizeMsg = match payload.and_then(|v| serde_json::from_value(v).ok()) {
            Some(m) => m,
            None => return,
        };
        let amount = match self.novations.get(&msg.novation_id) {
            Some(nov) => nov.amount,
            None => return,
        };

        self.do_add_balance(peer, -amount, ts);
        self.remove_side(&msg.novation_id, OUT);

        let in_empty = self.novations.get(&msg.novation_id)
            .map(|nov| !nov.has_side(IN))
            .unwrap_or(true);
        if in_empty {
            self.novations.remove(&msg.novation_id);
        }
    }

    fn req_rollback(
        &mut self,
        peer: &str,
        payload: Option<Value>,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        let msg: RollbackMsg = match payload {
            Some(v) => serde_json::from_value(v)?,
            None => return Err("bad payload".into()),
        };
        {
            let nov = self.novations.get(&msg.novation_id)
                .ok_or_else(|| format!("unknown novation {}", msg.novation_id))?;
            if nov.peer(OUT) != peer {
                return Err("wrong peer".into());
            }
        }
        self.remove_side(&msg.novation_id, OUT);

        let in_empty = self.novations.get(&msg.novation_id)
            .map(|nov| !nov.has_side(IN))
            .unwrap_or(true);
        if in_empty {
            self.novations.remove(&msg.novation_id);
            return Ok(HandleResult { ok: true });
        }
        let fwd = serde_json::to_value(RollbackMsg {
            novation_id: msg.novation_id.clone(),
        })
        .unwrap();
        self.send_side(&msg.novation_id, IN, "rollback", fwd);
        Ok(HandleResult { ok: true })
    }

    fn resp_rollback(&mut self, payload: Option<Value>) {
        let msg: RollbackMsg = match payload.and_then(|v| serde_json::from_value(v).ok()) {
            Some(m) => m,
            None => return,
        };
        self.novations.remove(&msg.novation_id); // Drop cleans up both sides.
    }

    /// Scan for circular debt and initiate novations (called periodically).
    pub fn try_novate(&mut self) {
        let peers = match &self.connected_peers {
            Some(f) => f(),
            None => return,
        };
        for debtor in &peers {
            if self.novations.len() >= MAX_NOVATIONS {
                return;
            }
            let mut avl_d = self.get_pending_balance(debtor, OUT);
            if avl_d <= 0 {
                continue;
            }
            for creditor in &peers {
                if creditor == debtor {
                    continue;
                }
                let avl_c = self.get_pending_balance(creditor, IN);
                if avl_c <= 0 {
                    continue;
                }
                let mut amount = avl_d;
                if avl_c < amount {
                    amount = avl_c;
                }
                if amount <= 0 {
                    continue;
                }
                let nov_id = random_novation_id();
                // OUT side = debtor (send commit downstream).
                // IN side left empty — filled when the confirmation comes back.
                let so_out = self.new_sync_object(debtor);
                let nov = Novation {
                    initiator: true,
                    amount,
                    side: [None, Some(so_out)],
                };
                self.novations.insert(nov_id.clone(), nov);
                let commit = serde_json::to_value(CommitMsg {
                    novation_id: nov_id.clone(),
                    amount,
                    next: creditor.clone(),
                })
                .unwrap();
                self.send_side(&nov_id, OUT, "commit", commit);
                avl_d -= amount;
                if avl_d <= 0 {
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Module trait
// ---------------------------------------------------------------------------

impl Module for Triad {
    fn on_connect(&mut self, _peer: &str) {}
    fn on_close(&mut self, _peer: &str) {}

    fn handle_request(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        match method {
            "commit" => self.req_commit(peer, payload),
            "finalize" => self.req_finalize(peer, payload, ts),
            "rollback" => self.req_rollback(peer, payload),
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
            "commit" => self.resp_commit(peer, payload, ok),
            "finalize" => self.resp_finalize(peer, payload, ts),
            "rollback" => self.resp_rollback(payload),
            _ => {}
        }
    }

    fn handle_account(
        &mut self,
        method: &str,
        payload: Option<Value>,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        match method {
            "forceRollback" | "forceFinalize" => {
                let (side, msg_method) = if method == "forceRollback" {
                    (IN, "rollback")
                } else {
                    (OUT, "finalize")
                };
                let p: RollbackMsg = match payload {
                    Some(v) => serde_json::from_value(v)?,
                    None => return Err("bad payload".into()),
                };
                let nov = self.novations.get(&p.novation_id)
                    .ok_or_else(|| format!("unknown novation {}", p.novation_id))?;
                if !nov.has_side(side) {
                    return Err(format!("no peer on {} side", method).into());
                }
                let other = if side == OUT { IN } else { OUT };
                if nov.has_side(other) {
                    return Err(format!(
                        "{}: opposite side still has peer {}",
                        method, nov.peer(other)
                    )
                    .into());
                }
                // Check for pending messages on either side.
                let has_pending = nov.side.iter()
                    .filter_map(|s| s.as_ref())
                    .any(|so| so.has_pending());
                if has_pending {
                    return Err(format!(
                        "{}: messages still queued for novation",
                        method
                    )
                    .into());
                }
                let msg_payload = if msg_method == "finalize" {
                    serde_json::to_value(FinalizeMsg {
                        novation_id: p.novation_id.clone(),
                    })
                    .unwrap()
                } else {
                    serde_json::to_value(RollbackMsg {
                        novation_id: p.novation_id.clone(),
                    })
                    .unwrap()
                };
                self.send_side(&p.novation_id, side, msg_method, msg_payload);
                Ok(None)
            }
            _ => Err("unknown method".into()),
        }
    }

    fn status(&self) -> Option<Value> {
        let novs: HashMap<String, Value> = self
            .novations
            .iter()
            .map(|(id, nov)| {
                (
                    id.clone(),
                    serde_json::json!({
                        "amount": nov.amount,
                        "in": nov.peer(IN),
                        "out": nov.peer(OUT),
                    }),
                )
            })
            .collect();
        Some(serde_json::json!({"novations": novs}))
    }

    fn force_remove(&mut self, peer: &str) {
        let ids: Vec<String> = self.novations.keys().cloned().collect();
        for id in ids {
            let (is_out, is_in) = match self.novations.get(&id) {
                Some(nov) => (nov.peer(OUT) == peer, nov.peer(IN) == peer),
                None => continue,
            };
            if is_out {
                // OUT peer removed — safe to rollback.
                self.remove_side(&id, OUT);
                let has_in = self.novations.get(&id)
                    .map(|nov| nov.has_side(IN))
                    .unwrap_or(false);
                if has_in {
                    let rollback = serde_json::to_value(RollbackMsg {
                        novation_id: id.clone(),
                    })
                    .unwrap();
                    self.send_side(&id, IN, "rollback", rollback);
                } else {
                    self.novations.remove(&id);
                }
                continue;
            }
            if is_in {
                // IN peer removed — clear reference, operator resolves.
                self.remove_side(&id, IN);
                let has_out = self.novations.get(&id)
                    .map(|nov| nov.has_side(OUT))
                    .unwrap_or(false);
                if !has_out {
                    self.novations.remove(&id);
                }
            }
        }
    }

    fn reattach_sync_objects(&mut self, registry: &RegistryRef) {
        self.registry = Some(registry.clone());
        let name = self.name.clone();
        for nov in self.novations.values_mut() {
            for side in &mut nov.side {
                if let Some(ref mut so) = side {
                    let rc = Arc::new(AtomicUsize::new(0)); // TODO: wire up real ref_count via ref_count_fn
                    so.reattach(registry, &name, rc);
                }
            }
        }
    }
}

/// Generate a random novation ID (64 hex chars).
fn random_novation_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:032x}", ts)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a test novation with SyncObject sides.
    fn make_nov(t: &Triad, amount: i64, in_peer: &str, out_peer: &str, initiator: bool) -> Novation {
        let side_in = if in_peer.is_empty() {
            None
        } else {
            Some(t.new_sync_object(in_peer))
        };
        let side_out = if out_peer.is_empty() {
            None
        } else {
            Some(t.new_sync_object(out_peer))
        };
        Novation {
            amount,
            side: [side_in, side_out],
            initiator,
        }
    }

    #[test]
    fn triad_reserved_counts() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        t.novations.insert("n1".into(), make_nov(&t, 100, "bob@b.com", "carol@c.com", false));
        t.novations.insert("n2".into(), make_nov(&t, 50, "bob@b.com", "", false));
        assert_eq!(t.reserved("bob@b.com", IN), 150);
        assert_eq!(t.reserved("carol@c.com", OUT), 100);
        assert_eq!(t.reserved("carol@c.com", IN), 0);
    }

    #[test]
    fn triad_commit_creates_novation() {
        let reg = mimesis::registry::new_registry();
        let mut t = Triad::for_test("triad", "alice@a.com");
        t.registry = Some(reg.clone());
        t.is_connected = Some(Box::new(|_| true));
        t.bandwidth = Some(Box::new(|_, _| 1_000_000));

        let payload = serde_json::to_value(CommitMsg {
            novation_id: "test-nov".into(),
            amount: 100,
            next: "carol@c.com".into(),
        })
        .unwrap();

        let result = t
            .handle_request("bob@b.com", "commit", Some(payload), 0)
            .unwrap();
        assert!(result.ok);
        assert!(t.novations.contains_key("test-nov"));

        let nov = &t.novations["test-nov"];
        assert_eq!(nov.amount, 100);
        assert_eq!(nov.peer(IN), "bob@b.com");
        assert_eq!(nov.peer(OUT), "carol@c.com");
        // Should have forwarded commit to carol via OUT side's queue.
        assert!(nov.side[OUT].as_ref().unwrap().has_pending());
    }

    #[test]
    fn triad_rollback_clears_novation() {
        let reg = mimesis::registry::new_registry();
        let mut t = Triad::for_test("triad", "alice@a.com");
        t.registry = Some(reg.clone());
        t.novations.insert("n1".into(), make_nov(&t, 100, "", "bob@b.com", false));

        let payload = serde_json::to_value(RollbackMsg {
            novation_id: "n1".into(),
        })
        .unwrap();
        let result = t
            .handle_request("bob@b.com", "rollback", Some(payload), 0)
            .unwrap();
        assert!(result.ok);
        assert!(!t.novations.contains_key("n1"));
    }

    #[test]
    fn triad_force_remove_rolls_back_out_side() {
        let reg = mimesis::registry::new_registry();
        let mut t = Triad::for_test("triad", "alice@a.com");
        t.registry = Some(reg.clone());
        t.novations.insert("n1".into(), make_nov(&t, 100, "charlie@c.com", "bob@b.com", false));

        t.force_remove("bob@b.com");

        // OUT side cleared; rollback sent to IN peer.
        let nov = &t.novations["n1"];
        assert!(!nov.has_side(OUT));
        assert!(nov.side[IN].as_ref().unwrap().has_pending());
    }

    #[test]
    fn triad_drop_cleans_up_queues() {
        let reg = mimesis::registry::new_registry();
        let mut t = Triad::for_test("triad", "alice@a.com");
        t.registry = Some(reg.clone());
        let nov = make_nov(&t, 100, "bob@b.com", "", false);
        t.novations.insert("n1".into(), nov);
        // Queue a message so there's something to clean up.
        t.novations.get("n1").unwrap().side[IN].as_ref().unwrap()
            .send("commit", None);
        assert!(reg.lock().unwrap().has_pending("triad", "bob@b.com"));

        // Remove novation — Drop should unregister from registry.
        t.novations.remove("n1");
        assert!(!reg.lock().unwrap().has_pending("triad", "bob@b.com"));
    }
}
