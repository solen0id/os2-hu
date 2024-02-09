//! Contains code for locally simulating a network with unreliable connections.

use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::{
    cell::Cell,
    fmt, fs,
    io::{self, Write},
    path,
    time::{Duration, Instant},
};
use tracing::{debug, info, trace, trace_span};

use crate::protocol::Command;

/// Constant that causes an artificial delay in the relaying of messages.
const NETWORK_DELAY: Duration = Duration::from_millis(10);

#[derive(Debug, PartialEq)]
pub enum State {
    Leader,
    Candidate,
    Follower,
}

pub struct LogEntry {
    term: usize,
    index: usize,
    command: Command,
}

/// Virtual node in a virtual network.
pub struct NetworkNode<T> {
    /// This node's partition number.
    /// Can only communicate with nodes in the same partition.
    partition: Arc<AtomicUsize>,

    /// The unreliable channel. Local sends *always* succeed.
    /// MPSC to bypass dealing with multiplexing multiple connections.
    channel: (mpsc::Sender<T>, mpsc::Receiver<T>),

    /// Public (unique) address of this node.
    /// Useful to identify this node in messages to other nodes.
    pub address: usize,

    /// Logfile to store committed entries in, can not be changed.
    log_file: fs::File,

    /// In-memory log of the node, can be overwritten/changed by leader.
    working_memory_log: Vec<LogEntry>,

    /// Number of entries in the logfile.
    last_log_index: Cell<u32>,

    /// connections to other nodes
    pub connections: HashMap<usize, Connection<Command>>,

    /// current state of the node
    pub state: State,

    /// timeout for heartbeats
    pub heartbeat_timeout: Duration,

    /// election timeout
    pub election_timeout: Duration,

    /// last contact with leader, used to detect leader failure
    pub last_heartbeat: Instant,

    /// election start instant, used to detect election timeout
    pub last_election_start: Instant,

    /// latest term server has seen (initialized to 0 on first boot, increases monotonically)
    pub current_term: usize,

    /// candidateId that received vote in current term (or null if none)
    pub voted_for: Option<usize>,

    /// index of highest log entry known to be committed (initialized to 0, increases monotonically)
    commit_index: usize,

    /// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    last_applied: usize,

    /// Volatile state on leaders, reinitialized after election

    /// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    next_index: HashMap<usize, usize>,

    /// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    match_index: HashMap<usize, usize>,

    /// number of received votes
    votes: usize,

    /// current leader
    pub current_leader: Option<usize>,
}

/// Reliable channel to a network node.
/// Can be "upgraded" to a full (unreliable) Connection.
#[derive(Debug)]
pub struct Channel<T> {
    /// Entry point for sending messages to the node that created this channel.
    port: mpsc::Sender<T>,

    /// Unique address of the creator.
    pub address: usize,

    /// Pointer to the creators partition number.
    pub part: Arc<AtomicUsize>,
}

/// Unreliable one-directional connection between two virtual network nodes.
pub struct Connection<T> {
    /// Entry point for sending messages to the node that created this channel.
    port: mpsc::Sender<T>,

    /// Partition number of the receiver.
    /// Note: if this is different from the sender's, no messages are relayed
    src: Arc<AtomicUsize>,

    /// Partition number of the sender.
    /// Note: if this is different from the receiver's, no messages are relayed
    dst: Arc<AtomicUsize>,
}

impl<T> NetworkNode<T> {
    /// Creates a new network node and stores logfile in the specified path.
    pub fn new<P: AsRef<path::Path>>(address: usize, path: P) -> io::Result<Self> {
        // we use a slightly larger range for the election timeout than the paper suggests
        // for easier testing and debugging
        let timeout = rand::thread_rng().gen_range(300..500);

        let node = Self {
            partition: Arc::new(AtomicUsize::new(0)),
            channel: mpsc::channel(),
            address,
            log_file: fs::File::create(path.as_ref().join(address.to_string() + ".log"))?,
            working_memory_log: vec![],
            last_log_index: Cell::new(0),
            connections: HashMap::new(),
            state: State::Follower,
            heartbeat_timeout: Duration::from_millis(timeout),
            election_timeout: Duration::from_millis(
                rand::thread_rng().gen_range(timeout..2 * timeout),
            ),
            last_heartbeat: Instant::now(),
            last_election_start: Instant::now(),
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes: 0,
            current_leader: None,
        };
        Ok(node)
    }

    pub fn is_leader(&self) -> bool {
        self.state == State::Leader
    }

    pub fn is_candidate(&self) -> bool {
        self.state == State::Candidate
    }

    pub fn become_leader(&mut self) {
        info!(self.address, "becoming leader");
        self.state = State::Leader;
        self.voted_for = None;

        for (address, _) in self.connections.iter() {
            self.next_index
                .insert(*address, self.last_log_index.get() as usize + 1);

            self.match_index.insert(*address, 0);
        }
    }

    pub fn become_follower(&mut self) {
        self.state = State::Follower;
        self.voted_for = None;
        self.next_index = HashMap::new();
        self.match_index = HashMap::new();
        self.last_heartbeat = Instant::now();
    }

    pub fn become_candidate(&mut self) {
        self.state = State::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.address);
        self.votes = 1;
    }

    pub fn start_election(&mut self) {
        debug!("starting election");
        self.last_election_start = Instant::now();
        self.send_vote_requests();
    }

    pub fn check_term_and_convert_to_follower_if_needed(&mut self, term: usize) -> bool {
        if term > self.current_term {
            debug!(
                self.current_term,
                term, "term is outdated, accepting new term converting to follower"
            );
            self.current_term = term;
            self.become_follower();
            return true;
        }
        return false;
    }

    pub fn apply_commited_if_possible(&mut self) -> bool {
        if self.commit_index > self.last_applied {
            // apply log entries
            // let entry = self.log_get_at_index(self.last_applied);
            // TODO: apply entry

            self.last_applied += 1;

            return true;
        }
        return false;
    }

    pub fn add_vote(&mut self) {
        self.votes += 1;
    }

    pub fn check_if_election_won_and_become_leader(&mut self) -> bool {
        if self.votes > self.votes_needed_to_win() {
            self.become_leader();
            return true;
        }
        return false;
    }

    pub fn votes_needed_to_win(&self) -> usize {
        self.connections.len() / 2
    }

    pub fn get_prev_log_index(&self) -> usize {
        // Returns the index of the log entry immediately preceding new ones
        // return self.last_log_index.get() as usize;
        return self.working_memory_log.len();
    }

    pub fn get_prev_log_term(&self) -> usize {
        // Returns the term of the log entry immediately preceding new ones
        // TODO: implement

        match self.working_memory_log.last() {
            Some(entry) => entry.term,
            None => 0,
        }
    }

    pub fn send_heartbeat(&self) {
        for (_address, connection) in self.connections.iter() {
            let _ = connection.encode(Command::AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.address,
                prev_log_index: self.get_prev_log_index(),
                prev_log_term: self.get_prev_log_term(),
                entries: vec![],
                leader_commit: self.commit_index,
            });
        }
    }

    pub fn send_vote_requests(&self) {
        for (_address, connection) in self.connections.iter() {
            let _ = connection.encode(Command::RequestVoteRequest {
                term: self.current_term,
                candidate_id: self.address,
                last_log_index: self.last_log_index.get() as usize,
                last_log_term: 0, // TODO: implement
            });
        }
    }

    pub fn send_vote(&self, candidate_id: usize, vote_granted: bool) {
        let connection = self.connections.get(&candidate_id).unwrap();
        let _ = connection.encode(Command::RequestVoteResponse {
            term: self.current_term,
            vote_granted,
        });
    }

    pub fn try_forward_to_leader(&self, command: Command) -> bool {
        match self.current_leader {
            Some(leader_id) => {
                let connection = self.connections.get(&leader_id).unwrap();
                match connection.encode(command) {
                    Ok(_) => {
                        debug!("successfully forwarded command to leader");
                        return true;
                    }
                    Err(_) => {
                        // let _ = self.connections.get(&self.address).unwrap().encode(command);
                        return false;
                    }
                }
            }
            None => {
                // let _ = self.connections.get(&self.address).unwrap().encode(command);
                return false;
            }
        }
    }

    pub fn send_self(&self, command: Command) {
        match self.connections.get(&self.address).unwrap().encode(command) {
            Ok(_) => {
                //  debug!(self.address, "successfully sent command to self");
            }
            Err(_) => {
                debug!(self.address, "failed to send command to self");
            }
        }
    }

    /// Creates a new (reliable) channel to this network node.
    pub fn channel(&self) -> Channel<T> {
        Channel {
            port: self.channel.0.clone(),
            address: self.address,
            part: self.partition.clone(),
        }
    }

    /// Upgrades a channel to a fully fledged (unreliable) connection.
    /// Note: The connection can be relied upon, if the channel was created by
    /// the same node.
    pub fn accept(&self, request: Channel<T>) -> Connection<T> {
        Connection {
            port: request.port,
            src: self.partition.clone(),
            dst: request.part,
        }
    }

    /// Receives a message from the channel with an optional timeout.
    /// This method introduces a fixed network delay of 10ms per received
    /// message if the channel was empty before. This is meant to crudely model
    /// network delays since channels would basically be instantaneous otherwise.
    pub fn decode(&self, timeout: Option<Instant>) -> Result<T, mpsc::RecvTimeoutError> {
        use std::thread::sleep;

        // delay if the channel was empty before, since other messages might
        // have arrived while we were waiting for the first one;
        // this is really not optimal though, since we'll also be waiting on
        // local transmissions
        match self.channel.1.try_recv() {
            Ok(msg) => Ok(msg),
            Err(_) => {
                if let Some(timeout) = timeout {
                    let now = Instant::now();

                    if now < timeout {
                        let msg = self.channel.1.recv_timeout(timeout - now)?;
                        sleep(NETWORK_DELAY);
                        Ok(msg)
                    } else {
                        Err(mpsc::RecvTimeoutError::Timeout)
                    }
                } else {
                    let msg = self.channel.1.recv()?;
                    sleep(NETWORK_DELAY);
                    Ok(msg)
                }
            }
        }
    }

    /// Appends a message to the logfile.
    /// This can not be undone. Make sure it's consistent with the other nodes.
    pub fn append<E: fmt::Debug>(&self, entry: &E) {
        let next_log_index = self.last_log_index.get() + 1;

        (&self.log_file)
            .write(format!("{:4} {:?}\n", next_log_index, entry).as_bytes())
            .ok();

        self.last_log_index.set(next_log_index);
    }
}

impl<T> Channel<T> {
    /// Reliably sends a message through the channel.
    /// Both order and content of the message are preserved.
    pub fn send(&self, t: T) {
        self.port.send(t).unwrap();
    }
}

impl<T> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Self {
            port: self.port.clone(),
            address: self.address,
            part: self.part.clone(),
        }
    }
}

impl<T> Connection<T> {
    /// Tries to send a message through the connection.
    /// Order and content of the message are preserved, but sending will fail
    /// if the sender and the receiver are not in the same partition.
    pub fn encode(&self, t: T) -> Result<(), mpsc::SendError<T>> {
        use Ordering::SeqCst as Order;

        // prevent a data race here by checking if the operation is local first
        if Arc::ptr_eq(&self.src, &self.dst) || self.src.load(Order) == self.dst.load(Order) {
            self.port.send(t)
        } else {
            Err(mpsc::SendError(t))
        }
    }
}

/// Routine disrupting and restoring the network connections randomly.
pub fn daemon<T>(mut channels: Vec<Channel<T>>, events_per_sec: f32, duration_in_sec: f32) {
    use rand::prelude::*;
    use rand_distr::{Exp, Uniform};
    use std::{collections::BinaryHeap, thread};

    struct Event<T> {
        time: Instant,
        kind: EventType<T>,
    }

    enum EventType<T> {
        Disrupt,
        Restore(Channel<T>),
    }

    impl<'a, T> PartialOrd for Event<T> {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl<'a, T> Ord for Event<T> {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.time.cmp(&other.time).reverse()
        }
    }

    impl<'a, T> PartialEq for Event<T> {
        fn eq(&self, other: &Self) -> bool {
            self.time.eq(&other.time)
        }
    }

    impl<'a, T> Eq for Event<T> {}

    // short-circuit no-op daemons
    if events_per_sec == 0.0 || duration_in_sec == 0.0 {
        return;
    }

    // assert at least one network node
    assert_ne!(channels.len(), 0);

    // get the threads random number generator and the current time
    let mut rng = thread_rng();
    let start = Instant::now();

    // create a span for the daemon process
    let _guard = trace_span!("Daemon");
    let _guard = _guard.enter();

    // create the neg-exp delay distribution
    // (rationale: assuming independent failure rates)
    let delay = Exp::new(events_per_sec / 1000.0).unwrap();

    // create the neg-exp duration distribution
    // (rationale: assuming independent failure causes)
    let duration = Exp::new(1.0 / (duration_in_sec * 1000.0)).unwrap();

    // create a uniform partition distribution
    // (rationale: assuming equal chance of being grouped together)
    let partition = Uniform::new(1, channels.len());

    // initialize a priority queue for the scheduler
    let mut sched = BinaryHeap::new();

    // add the first disruption event to the scheduler
    sched.push(Event {
        time: start + Duration::from_millis(rng.sample(delay) as u64),
        kind: EventType::Disrupt,
    });

    // never-ending loop (each popped disruption-event causes another one to be pushed)
    while let Some(event) = sched.pop() {
        // suspend the thread until the next event occurs
        let now = Instant::now();
        if event.time > now {
            thread::sleep(event.time.duration_since(now));
        }

        // dispatch according to the event type
        match event.kind {
            EventType::Disrupt => {
                // select a random node to go offline
                channels.partial_shuffle(&mut rng, 1);

                // remove this node from the pool of disruptible nodes
                if let Some(channel) = channels.pop() {
                    let partition = rng.sample(partition);
                    channel.part.store(partition, Ordering::SeqCst);

                    trace!(node = channel.address, "disrupt");

                    // add the restoration-event to the scheduler
                    sched.push(Event {
                        time: now + Duration::from_millis(rng.sample(duration) as u64),
                        kind: EventType::Restore(channel),
                    });
                }

                // add another disruption event to the scheduler
                sched.push(Event {
                    time: now + Duration::from_millis(rng.sample(delay) as u64),
                    kind: EventType::Disrupt,
                });
            }
            EventType::Restore(channel) => {
                // restore the previously disrupted network node
                channel.part.store(0, Ordering::SeqCst);
                trace!(node = channel.address, "restore");
                channels.push(channel);
            }
        }
    }
}
