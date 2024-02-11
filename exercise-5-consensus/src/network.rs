//! Contains code for locally simulating a network with unreliable connections.

use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::{
    cell::Cell,
    cmp::min,
    fmt, fs,
    io::{self, Write},
    path,
    time::{Duration, Instant},
};
use tracing::{debug, info, trace, trace_span};

use crate::bank::Bank;
use crate::protocol::Command;

/// Constant that causes an artificial delay in the relaying of messages.
const NETWORK_DELAY: Duration = Duration::from_millis(10);

#[derive(Debug, PartialEq)]
pub enum State {
    Leader,
    Candidate,
    Follower,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: usize,
    pub index: usize,
    pub command: Command,
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
    pub commit_index: usize,

    /// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    pub last_applied: usize,

    /// Volatile state on leaders, reinitialized after election

    /// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    pub next_index: HashMap<usize, usize>,

    /// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    pub match_index: HashMap<usize, usize>,

    /// number of received votes
    votes: usize,

    /// current leader
    pub current_leader: Option<usize>,

    /// bank
    bank: Bank,
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
            bank: Bank::new(),
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
        debug!(self.address, "becoming leader");

        self.state = State::Leader;
        self.voted_for = None;

        self.reset_next_index();
        self.reset_match_index();
    }

    fn reset_next_index(&mut self) {
        for (address, _) in self.connections.iter() {
            self.next_index
                .insert(*address, self.last_log_index.get() as usize + 1);
        }
    }

    fn reset_match_index(&mut self) {
        for (address, _) in self.connections.iter() {
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
        trace!("starting election");
        self.last_election_start = Instant::now();
        self.send_vote_requests();
    }

    pub fn check_term_and_convert_to_follower_if_needed(&mut self, term: usize) -> bool {
        if term > self.current_term {
            trace!(
                self.current_term,
                term,
                "term is outdated, accepting new term converting to follower"
            );
            self.current_term = term;
            self.become_follower();
            return true;
        }
        return false;
    }

    pub fn apply_command_to_state_machine(&mut self, command: Command) -> bool {
        match self.bank.apply_command(&command) {
            Ok(_) => {
                return true;
            }
            Err(_) => {
                return false;
            }
        }
    }

    pub fn apply_commited_entry_to_log_if_possible(&mut self) {
        while self.commit_index > self.last_applied {
            // apply log entries

            // let entry = self.get_log_entry_at_index(self.last_applied + 1).unwrap();
            let entry = self.working_memory_log.get(self.last_applied).unwrap();

            if self.is_leader() {
                info!("applying log entry to state machine {:?}", entry);
            }

            match self.bank.apply_command(&entry.command) {
                Ok(_) => {
                    self.append(&entry, true);
                }
                Err(err) => {
                    if self.is_leader() {
                        info!(
                            "failed to apply log entry to state machine {:?} {:?}",
                            entry, err
                        );
                    }
                    debug!("failed to apply log entry to state machine {:?}", entry);
                    self.append(&entry, false);
                }
            }

            self.last_applied += 1;
        }
    }

    pub fn add_vote(&mut self) {
        self.votes += 1;
    }

    pub fn check_if_election_won_and_become_leader(&mut self) -> bool {
        let votes_needed = self.votes_needed_to_win();

        trace!(self.votes, votes_needed, "checking if election won");

        if self.votes >= votes_needed {
            trace!(self.address, "election won");
            self.become_leader();
            return true;
        }
        return false;
    }

    pub fn votes_needed_to_win(&self) -> usize {
        // 3 nodes --> 2 votes needed to win
        // 4 nodes --> 3 votes needed to win
        // 5 nodes --> 3 votes needed to win
        // 6 nodes --> 4 votes needed to win

        (self.connections.len() + 1).div_ceil(2)
    }

    pub fn get_prev_log_index(&self) -> usize {
        // Returns the index of the log entry immediately preceding new ones
        // starting with 0 for an empty log and 1 for a log with one entry
        // (log index in the paper starts at 1)
        return self.working_memory_log.len();
    }

    pub fn get_prev_log_term(&self) -> usize {
        // Returns the term of the log entry immediately preceding new ones
        // starting with 0 for an empty log
        match self.working_memory_log.last() {
            Some(entry) => entry.term,
            None => 0,
        }
    }

    pub fn get_log_entry_at_index(&self, index: usize) -> Option<&LogEntry> {
        if index == 0 || self.working_memory_log.is_empty() || index > self.working_memory_log.len()
        {
            return None;
        }

        // subtract 1 because the index in the paper starts at 1
        // but our vector is zero indexed
        return self.working_memory_log.get(index - 1);
    }

    pub fn append_entry_to_log(&mut self, entry: LogEntry) {
        self.working_memory_log.push(entry.clone());
    }

    pub fn append_entries_to_log(&mut self, entries: Vec<LogEntry>) {
        // at this point we are already sure that the previous entry
        // is the same as the one on the leader

        // remove all entries starting from the index of the first new entry
        // and append the new entries
        let insert_index = entries[0].index;

        if insert_index > 1 {
            // subtract another 1, because we only want to keep old entries
            // with indices lower than the first new entry
            self.working_memory_log.truncate(insert_index - 1);
        } else {
            self.working_memory_log.clear();
        }
        self.working_memory_log.extend(entries);
    }

    pub fn set_commit_index(&mut self, leader_commit_index: usize) {
        // leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        let last_append_entry_index = self.working_memory_log.len();

        if leader_commit_index > self.commit_index {
            self.commit_index = min(leader_commit_index, last_append_entry_index);
        }
    }

    pub fn increment_next_index(&mut self, address: usize) {
        if let Some(next_index) = self.next_index.get_mut(&address) {
            *next_index += 1;
        }
    }

    pub fn decrement_next_index(&mut self, address: usize) {
        let next_index = self.next_index.get_mut(&address).unwrap();

        if next_index > &mut 0 {
            *next_index -= 1;
        } else {
            *next_index = 0;
        }
    }

    pub fn increment_match_index(&mut self, address: usize) {
        let match_index = self.match_index.get_mut(&address).unwrap();
        *match_index += 1;
    }

    pub fn advance_commit_index_if_possible(&mut self) {
        let match_indexes: Vec<usize> = self
            .match_index
            .values()
            .map(|x| *x)
            .collect::<Vec<usize>>();

        // count the number of indices
        let mut counter: HashMap<usize, usize> = HashMap::new();
        for elem in match_indexes.iter() {
            *counter.entry(*elem).or_default() += 1;
        }

        // sort counter map by highest value
        let mut counter_vec: Vec<(usize, usize)> = counter.into_iter().collect();
        counter_vec.sort_by(|a, b| b.1.cmp(&a.1));

        // get the majority index
        let hightest_count_index = counter_vec[0].0;
        let highest_count = counter_vec[0].1;

        if highest_count >= self.votes_needed_to_win() && hightest_count_index > self.commit_index {
            match self.get_log_entry_at_index(hightest_count_index) {
                Some(entry) => {
                    if entry.term == self.current_term {
                        self.commit_index = hightest_count_index;
                    }
                }
                None => {
                    // this should probably not happen!
                    debug!("no log entry at index {}", hightest_count_index);
                }
            }
        }
    }

    pub fn send_heartbeat(&self) {
        for (address, connection) in self.connections.iter() {
            if address != &self.address {
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

    pub fn send_append_entries_request(&self, address: usize) {
        if address == self.address {
            return;
        }

        let connection = self.connections.get(&address).unwrap();
        let next_index = self.next_index.get(&address).unwrap();
        let leader_commit = self.commit_index;

        if next_index <= &1 {
            // cant get log at index 0, we must be at the beginning
            let prev_log_index: usize = 0;
            let prev_log_term: usize = 0;
            let _ = connection.encode(Command::AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.address,
                prev_log_index,
                prev_log_term,
                entries: self.working_memory_log.clone(),
                leader_commit,
            });
        } else {
            let prev_log_index = next_index - 1;
            let prev_log_term = self.get_log_entry_at_index(next_index - 1).unwrap().term;
            let _ = connection.encode(Command::AppendEntriesRequest {
                term: self.current_term,
                leader_id: self.address,
                prev_log_index,
                prev_log_term,
                entries: self.working_memory_log[*next_index - 1..].to_vec(),
                leader_commit,
            });
        }
    }

    pub fn send_append_entries_response(&self, term: usize, success: bool) {
        // we take care to set the leader id to the current leader before calling this
        // function so well cheekily unwrap without checking here
        let connection = self.connections.get(&self.current_leader.unwrap()).unwrap();
        let _ = connection.encode(Command::AppendEntriesResponse {
            term,
            success,
            sender_id: self.address,
            sender_last_match_index: self.working_memory_log.len(),
        });
    }

    pub fn send_append_entries_request_to_all_followers(&self) {
        for (follower_address, next_index) in self.next_index.iter() {
            if *follower_address != self.address && &self.get_prev_log_index() >= next_index {
                self.send_append_entries_request(*follower_address);
            }
        }
    }

    pub fn try_forward_to_leader(&self, command: Command) -> bool {
        match self.current_leader {
            Some(leader_id) => {
                let connection = self.connections.get(&leader_id).unwrap();
                match connection.encode(command.clone()) {
                    Ok(_) => {
                        trace!("successfully forwarded command {:?} to leader", command);
                        return true;
                    }
                    Err(_) => {
                        return false;
                    }
                }
            }
            None => {
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
    pub fn append<E: fmt::Debug>(&self, entry: &E, success: bool) {
        let next_log_index = self.last_log_index.get() + 1;

        (&self.log_file)
            .write(format!("{:4} {} {:?}\n", next_log_index, success, entry).as_bytes())
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

                    debug!(node = channel.address, "disrupt");

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
                debug!(node = channel.address, "restore");
                channels.push(channel);
            }
        }
    }
}
