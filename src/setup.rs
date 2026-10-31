//! Module registration and wiring for creditnet.
//!
//! Creates trustline and triad modules with proper cross-references:
//! - Triad gets callbacks into Trustline (bandwidth, pending_balance, etc.)
//! - Trustline gets a Reserver that queries Triad's reserved amounts.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use mimesis::module::Module;
use mimesis::node::Node;
use mimesis::server::Server;

use crate::modules::triad::Triad;
use crate::modules::trustline::{Reserver, Trustline};

// ---------------------------------------------------------------------------
// TriadReserver — Trustline queries Triad for reserved capacity
// ---------------------------------------------------------------------------

struct TriadReserver {
    triad: Arc<Mutex<Triad>>,
}

impl Reserver for TriadReserver {
    fn reserved(&self, peer: &str, side: usize) -> i64 {
        self.triad.lock().unwrap().reserved(peer, side)
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register trustline (root) and triad (child) on the server.
/// Call before `server.init()`.
pub fn register_modules(server: &mut Server) {
    // Shared map so triad factory can find the trustline for the same node.
    let trustlines: Arc<Mutex<HashMap<String, Arc<Mutex<Trustline>>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // -- Trustline factory (must be registered first) --
    let tl_map = trustlines.clone();
    server.register(
        "trustline",
        Box::new(move |name: &str, id: &str, node: &Node| {
            let tl = Arc::new(Mutex::new(Trustline::new(
                name.to_string(),
                id.to_string(),
                node.registry(),
            )));
            tl_map.lock().unwrap().insert(id.to_string(), tl.clone());
            tl as Arc<Mutex<dyn Module>>
        }),
    );

    // -- Triad factory --
    let tl_map2 = trustlines;
    server.register(
        "triad",
        Box::new(move |name: &str, id: &str, node: &Node| {
            let tl = tl_map2.lock().unwrap().get(id).cloned();

            let triad = match tl {
                Some(tl) => {
                    let tl_bw = tl.clone();
                    let tl_pb = tl.clone();
                    let tl_ab = tl.clone();
                    let tl_ic = tl.clone();
                    let tl_cp = tl.clone();

                    let mut t = Triad::new(
                        name.to_string(),
                        id.to_string(),
                        node.registry(),
                        Box::new(move |peer, side| tl_bw.lock().unwrap().bandwidth(peer, side)),
                        Box::new(move |peer, side| {
                            tl_pb.lock().unwrap().pending_balance(peer, side)
                        }),
                        Box::new(move |peer, amount, ts| {
                            if let Some(so) = tl_ab.lock().unwrap().trustlines.get_mut(peer) {
                                so.add_balance(amount, ts);
                            }
                        }),
                        Box::new(move |peer| tl_ic.lock().unwrap().is_connected(peer)),
                        Box::new(move || tl_cp.lock().unwrap().connected_peers()),
                    );

                    let triad = Arc::new(Mutex::new(t));

                    // Register reserver on trustline.
                    tl.lock().unwrap().register_reserver(Box::new(TriadReserver {
                        triad: triad.clone(),
                    }));

                    triad as Arc<Mutex<dyn Module>>
                }
                None => {
                    // No trustline found — create standalone triad (shouldn't happen).
                    Arc::new(Mutex::new(Triad::for_test(name, id))) as Arc<Mutex<dyn Module>>
                }
            };

            triad
        }),
    );

    // Declare trustline as root with triad as child.
    server.declare_root("trustline", vec!["triad".into()]);
}
