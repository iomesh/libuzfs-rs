mod dataset;
pub use dataset::*;
pub(crate) mod bindings;
pub(crate) mod context;
pub(crate) mod io;
pub mod metrics;
pub(crate) mod sync;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub struct UzfsTestEnv {
    dev_file: tempfile::NamedTempFile,
}

#[cfg(test)]
impl UzfsTestEnv {
    /// Create an env for uzfs test.
    ///
    /// If dev_size is not zero, this function will create a temp file as block device,
    /// the size of device is `dev_size` bytes.
    ///
    /// If dev_size is zero, no block device and pool/dataset is created
    ///
    /// All of the resources (temp file, dev_file) will be deleted automatically when the env
    /// goes out of scope
    pub fn new(dev_size: u64) -> Self {
        let dev_file = tempfile::NamedTempFile::new().unwrap();

        if dev_size > 0 {
            dev_file.as_file().set_len(dev_size).unwrap();
        }

        UzfsTestEnv { dev_file }
    }

    pub fn get_dev_path(&self) -> &str {
        self.dev_file.path().to_str().unwrap()
    }

    pub fn set_dev_size(&self, new_size: u64) {
        self.dev_file.as_file().set_len(new_size).unwrap();
    }
}

pub fn fetch_contention_graph(weight_threshold: u64) -> String {
    use petgraph::dot::*;
    let contentions = context::stack::fetch_lock_contentions();
    let mut node_map = std::collections::HashMap::with_capacity(128);
    let mut graph = petgraph::Graph::new();
    graph.reserve_nodes(128);
    graph.reserve_edges(1024);
    for (bt, c) in contentions {
        let nodes: Vec<_> = bt
            .into_iter()
            .map(|addr| {
                if let Some(node) = node_map.get(&addr) {
                    *node
                } else {
                    let mut symbol = String::new();
                    unsafe {
                        backtrace::resolve_unsynchronized(addr as *mut libc::c_void, |s| {
                            if s.lineno().is_some() && s.name().is_some() {
                                symbol = format!("{}:{}", s.name().unwrap(), s.lineno().unwrap());
                            }
                        })
                    };
                    let node = graph.add_node(symbol);
                    node_map.insert(addr, node);
                    node
                }
            })
            .collect();

        for i in 0..(nodes.len() - 1) {
            if let Some(idx) = graph.find_edge(nodes[i], nodes[i + 1]) {
                *graph.edge_weight_mut(idx).unwrap() += c
            } else {
                graph.add_edge(nodes[i], nodes[i + 1], c);
            }
        }
    }

    graph.retain_edges(|g, e| *g.edge_weight(e).unwrap() > weight_threshold);
    graph.retain_nodes(|g, n| {
        g.neighbors_directed(n, petgraph::Direction::Incoming)
            .next()
            .is_some()
            || g.neighbors_directed(n, petgraph::Direction::Outgoing)
                .next()
                .is_some()
    });
    Dot::with_config(&graph, &[]).to_string()
}
