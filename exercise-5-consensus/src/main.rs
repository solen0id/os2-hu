//! Implementation of the Raft Consensus Protocol for a banking application.

#[allow(unused_imports)]
use core::time;
use std::{collections::VecDeque, env::args, fs, io, thread};

use rand::prelude::*;
use std::time::{Duration, Instant};
use tracing::{debug, trace, trace_span, Level};

use crate::network::State;
use network::{daemon, Channel, LogEntry, NetworkNode};
use protocol::Command;

pub mod bank;
pub mod network;
pub mod protocol;

/// Creates and connects a number of branch offices for the bank.
pub fn setup_offices(office_count: usize, log_path: &str) -> io::Result<Vec<Channel<Command>>> {
    let mut channels = Vec::with_capacity(office_count);

    // create the log directory if needed
    fs::create_dir_all(log_path)?;

    // create various network nodes and start them
    for address in 0..office_count {
        let mut node: NetworkNode<Command> = NetworkNode::new(address, &log_path)?;
        channels.push(node.channel());

        thread::spawn(move || {
            // configure a span to associate log-entries with this network node
            let _guard = trace_span!("NetworkNode", id = node.address);
            let _guard = _guard.enter();

            // connect to self, needed for command retries
            node.connections
                .insert(node.address, node.accept(node.channel()));

            // used to only send one heartbeat every 100ms
            let mut last_leader_heartbeat_send = Instant::now();
            let heartbeat_send_interval = Duration::from_millis(100);

            // used to retrying/forwarding client commands to leader
            let mut command_buffer: VecDeque<Command> = VecDeque::new();
            let mut last_client_command_retry = Instant::now() - Duration::from_secs(1);
            let client_command_retry_interval = Duration::from_millis(100);

            // set a timeout on Command Queue processing,
            // so we can check the node state periodically
            let cq_timeout = time::Duration::from_millis(20);

            loop {
                match node.state {
                    State::Follower => {
                        if node.last_heartbeat.elapsed() > node.heartbeat_timeout {
                            trace!("missed heartbeat, converting to candidate state");
                            node.become_candidate();
                            node.start_election();
                        }
                    }

                    State::Candidate => {
                        if node.last_election_start.elapsed() > node.election_timeout {
                            trace!("election timeout, starting new election");
                            node.become_candidate();
                            node.start_election();
                        }
                    }

                    State::Leader => {
                        if last_leader_heartbeat_send.elapsed() > heartbeat_send_interval {
                            trace!("sending heartbeat");
                            last_leader_heartbeat_send = Instant::now();
                            node.send_heartbeat();
                        }

                        // check all next_index and match_index and send append_entries
                        // requests to followers if needed
                        node.send_append_entries_request_to_all_followers();

                        // check if a majority of match_index is greater than commit_index
                        node.advance_commit_index_if_possible();
                    }
                }

                // If commitIndex > lastApplied: increment lastApplied,
                // apply log[lastApplied] to state machine
                node.apply_commited_entry_to_log_if_possible();

                // replay any buffered commands after the retry interval
                if !command_buffer.is_empty()
                    && last_client_command_retry.elapsed() > client_command_retry_interval
                {
                    if node.is_leader() {
                        while let Some(cmd) = command_buffer.pop_front() {
                            let _ = node.send_self(cmd);
                        }
                    } else {
                        // node is not leader, retry forwarding the command
                        if let Some(cmd) = command_buffer.pop_front() {
                            if !node.try_forward_to_leader(cmd.clone()) {
                                command_buffer.push_front(cmd);
                            }
                        }
                    }

                    last_client_command_retry = Instant::now();
                }

                while let Ok(cmd) = node.decode(Some(Instant::now() + cq_timeout)) {
                    // prepare a log entry for customer commands,
                    // won't be used for control commands, but saves some code duplication
                    let log_entry = LogEntry {
                        term: node.current_term,
                        index: node.get_prev_log_index() + 1,
                        command: cmd.clone(),
                    };

                    match cmd.clone() {
                        // customer requests
                        Command::Open { account } => {
                            if !node.is_leader() {
                                trace!("forwarding open account request to leader");

                                if !node.try_forward_to_leader(cmd.clone()) {
                                    command_buffer.push_back(cmd);
                                }

                                continue;
                            }
                            debug!("request to open an account for {:?}", account);
                            node.append_entry_to_log(log_entry);
                        }

                        Command::Deposit { account, amount } => {
                            if !node.is_leader() {
                                trace!("forwarding deposit request to leader");

                                if !node.try_forward_to_leader(cmd.clone()) {
                                    command_buffer.push_back(cmd);
                                }

                                continue;
                            }
                            debug!(amount, ?account, "request to deposit");
                            node.append_entry_to_log(log_entry);
                        }

                        Command::Withdraw { account, amount } => {
                            if !node.is_leader() {
                                trace!("forwarding withdraw request to leader");

                                if !node.try_forward_to_leader(cmd.clone()) {
                                    command_buffer.push_back(cmd);
                                }

                                continue;
                            }
                            debug!(amount, ?account, "request to withdraw");
                            node.append_entry_to_log(log_entry);
                        }

                        Command::Transfer { src, dst, amount } => {
                            if !node.is_leader() {
                                trace!("forwarding withdraw request to leader");

                                if !node.try_forward_to_leader(cmd.clone()) {
                                    command_buffer.push_back(cmd);
                                }

                                continue;
                            }
                            debug!(amount, ?src, ?dst, "request to transfer");
                            node.append_entry_to_log(log_entry);
                        }

                        // Control commands

                        // Accept a new channel
                        Command::Accept(channel) => {
                            node.connections
                                .insert(channel.address, node.accept(channel.clone()));
                            trace!(origin = channel.address, "accepted connection");
                        }

                        Command::RequestVoteRequest {
                            term,
                            candidate_id,
                            last_log_index,
                            last_log_term,
                        } => {
                            trace!(term, candidate_id, "received vote request");

                            // check if the term on the responder is greater than our own,
                            // and if so convert from candidate to follower
                            node.check_term_and_convert_to_follower_if_needed(term);

                            // don't grant vote if the term is less than our current term
                            if term < node.current_term {
                                trace!(term, "no vote, term is less than current term");
                                node.send_vote(candidate_id, false);
                            }

                            // only grant vote, if candidate log is at least as up-to-date as our own
                            if last_log_term < node.get_prev_log_term()
                                || (last_log_term == node.get_prev_log_term()
                                    && last_log_index < node.get_prev_log_index())
                            {
                                trace!(last_log_term, "no vote, candidate log is not up-to-date");
                                node.send_vote(candidate_id, false);
                            }

                            if node.voted_for.is_none() || node.voted_for == Some(candidate_id) {
                                node.voted_for = Some(candidate_id);
                                trace!(node.voted_for, "yes vote for candidate");
                                node.send_vote(candidate_id, true);
                            } else {
                                trace!(
                                    node.voted_for,
                                    "no vote, already voted for another candidate"
                                );
                                node.send_vote(candidate_id, false);
                            }
                        }

                        // Request for votes
                        Command::RequestVoteResponse { term, vote_granted } => {
                            trace!(term, vote_granted, "received vote response");

                            // check if the term on the responder is greater than our own,
                            // and if so convert from candidate to follower and do not
                            // process the vote
                            if node.check_term_and_convert_to_follower_if_needed(term) {
                                continue;
                            }

                            // we only need to check if we won the election if we are still
                            // a candidate and the vote was granted from the responder
                            if vote_granted && !node.is_leader() {
                                node.add_vote();
                                let _won = node.check_if_election_won_and_become_leader();
                            }
                        }

                        Command::AppendEntriesRequest {
                            term,
                            leader_id,
                            prev_log_index,
                            prev_log_term,
                            entries,
                            leader_commit,
                        } => {
                            node.last_heartbeat = Instant::now();
                            node.current_leader = Some(leader_id);

                            // check if the term on the responder is greater than our
                            // own, and if so update term and become follower
                            node.check_term_and_convert_to_follower_if_needed(term);

                            // check if current node is a candidate and the heartbeat
                            // comes from a leader, and if so convert to follower
                            if node.is_candidate() && term >= node.current_term {
                                trace!(
                                    leader_id,
                                    "candidate received append entries, converting to follower"
                                );
                                node.become_follower();
                            }

                            // reply false if the term is less than our current term
                            if term < node.current_term {
                                trace!(
                                    term,
                                    "reject append entries, term is less than current term"
                                );
                                node.send_append_entries_response(node.current_term, false);
                                continue;
                            }

                            node.set_commit_index(leader_commit);

                            if entries.is_empty() {
                                // heartbeat, nothing to append
                                trace!("received heartbeat");
                            } else {
                                trace!(
                                    ?entries,
                                    prev_log_index,
                                    prev_log_term,
                                    "received append entries"
                                )
                            }

                            // Reply false if log doesn’t contain an entry at prevLogIndex
                            // whose term matches prevLogTerm
                            match node.get_log_entry_at_index(prev_log_index) {
                                None => {
                                    // an initial append should work, even though the
                                    // log is empty which means we can't fetch the
                                    // previous log entry
                                    if prev_log_index == 0 && prev_log_term == 0 {
                                        if !entries.is_empty() {
                                            //append!
                                            node.append_entries_to_log(entries.clone());
                                        }
                                        node.send_append_entries_response(node.current_term, true);
                                    } else {
                                        trace!(
                                            prev_log_index,
                                            prev_log_term,
                                            "reject append entries, log does not contain previous entry"
                                        );
                                        node.send_append_entries_response(node.current_term, false);
                                        continue;
                                    }
                                }
                                Some(entry) => {
                                    if entry.term != prev_log_term || entry.index != prev_log_index
                                    {
                                        trace!(
                                            prev_log_index,
                                            prev_log_term,
                                            entry.term,
                                            "reject append entries, term or index of prevoious entry does not match"
                                        );
                                        node.send_append_entries_response(node.current_term, false);
                                    } else {
                                        if !entries.is_empty() {
                                            // append!
                                            node.append_entries_to_log(entries.clone());
                                        }
                                        node.send_append_entries_response(node.current_term, true);
                                    }
                                }
                            }
                        }

                        Command::AppendEntriesResponse {
                            term,
                            success,
                            sender_id,
                            sender_last_match_index,
                        } => {
                            trace!(term, success, "received append entries response");

                            // check if the term on the responder is greater than our own,
                            // and if so convert from candidate to follower
                            if node.check_term_and_convert_to_follower_if_needed(term) {
                                continue;
                            } else if node.is_leader() {
                                if success {
                                    let match_index = node.match_index.get_mut(&sender_id).unwrap();
                                    let next_index = node.next_index.get_mut(&sender_id).unwrap();

                                    *match_index = sender_last_match_index;
                                    *next_index = sender_last_match_index + 1;

                                    // node.increment_next_index(sender_id);
                                    // node.increment_match_index(sender_id);
                                } else {
                                    // find out who sent the response and decrement their next_index
                                    node.decrement_next_index(sender_id);
                                    node.send_append_entries_request(address)
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // connect the network nodes in random order
    let mut rng = thread_rng();
    for src in channels.iter() {
        for dst in channels.iter().choose_multiple(&mut rng, office_count) {
            if src.address == dst.address {
                continue;
            }
            src.send(Command::Accept(dst.clone()));
        }
    }

    Ok(channels)
}

fn main() -> io::Result<()> {
    use tracing_subscriber::{fmt::time::ChronoLocal, FmtSubscriber};
    let log_path = args().nth(1).unwrap_or("logs".to_string());

    // initialize the tracer
    FmtSubscriber::builder()
        .with_timer(ChronoLocal::new("[%Mm %Ss]".to_string()))
        .with_max_level(Level::DEBUG)
        .init();

    // create and connect a number of offices
    let channels = setup_offices(6, &log_path)?;
    let copy = channels.clone();

    // activate the thread responsible for the disruption of connections

    thread::spawn(move || daemon(copy, 1.0, 1.0));

    // sample script for your convenience
    script! {
        // tell the macro which collection of channels to use
        use channels;

        // customer requests start with the branch office index,
        // followed by the source account name and a list of requests
        [0] "Weber"   => open(), deposit( 50);
        [1] "Redlich" => open(), deposit(100);
        sleep();
        [2] "Redlich" => transfer("Weber", 20);
        sleep();
        [3] "Weber"   => withdraw(60);
        sleep(2);
    }

    Ok(())
}
