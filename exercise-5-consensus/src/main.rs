//! Implementation of the Raft Consensus Protocol for a banking application.

use std::time::{Duration, Instant};
use std::{env::args, fs, io, thread};

use rand::prelude::*;

use tracing::{debug, info, trace, trace_span, Level, warn};

use crate::protocol::{commit, compare_log_entries, Transaction};
use network::{daemon, Channel, NetworkNode};
use protocol::{Command, LogEntry, State};

pub mod network;
pub mod protocol;

/// Delay between timeouts
const TIMEOUT_DELAY: u64 = 150;

/// Creates and connects a number of branch offices for the bank.
pub fn setup_offices(office_count: usize, log_path: &str) -> io::Result<Vec<Channel<Command>>> {
    let mut channels = Vec::with_capacity(office_count);

    // create the log directory if needed
    fs::create_dir_all(log_path)?;

    // create various network nodes and start them
    for address in 0..office_count {
        let mut node = NetworkNode::new(address, &log_path, office_count)?;
        channels.push(node.channel());

        thread::spawn(move || {
            // configure a span to associate log-entries with this network node
            let _guard = trace_span!("NetworkNode", id = node.address);
            let _guard = _guard.enter();

            // initialize connections
            for _i in 0..office_count {
                node.connections.push(None);
            }

            // additional initializations for the node
            node.connections[address] = Some(node.accept(node.channel()));
            node.send(Command::CheckForTimeout {}, address);
            node.follower_last_log_index = vec![0; office_count];

            // timeout measurements
            let mut last_leader_contact = Instant::now();
            let mut last_timeout = Instant::now();
            let mut dynamic_waiting_time = 0;

            // dispatching event loop
            while let Ok(cmd) = node.decode(None) {
                match cmd {
                    // customer requests
                    Command::Open { account } => {
                        debug!("request to open an account for {:?}", account);
                        let msg = LogEntry {
                            command_type: Transaction::Open,
                            acc1: account.clone(),
                            acc2: "".to_string(),
                            amount: 0,
                            term: node.current_term,
                            origin_id: node.address,
                            origin_nr: node.request_counter,
                        };
                        node.request_counter += 1;
                        node.request_buffer.push(msg.clone());
                    }

                    Command::Deposit { account, amount } => {
                        debug!(amount, ?account, "request to deposit");
                        let msg = LogEntry {
                            command_type: Transaction::Deposit,
                            acc1: account.clone(),
                            acc2: "".to_string(),
                            amount: amount,
                            term: node.current_term,
                            origin_id: node.address,
                            origin_nr: node.request_counter,
                        };
                        node.request_counter += 1;
                        node.request_buffer.push(msg.clone());
                    }

                    Command::Withdraw { account, amount } => {
                        debug!(amount, ?account, "request to withdraw");
                        let msg = LogEntry {
                            command_type: Transaction::Withdraw,
                            acc1: account.clone(),
                            acc2: "".to_string(),
                            amount: amount,
                            term: node.current_term,
                            origin_id: node.address,
                            origin_nr: node.request_counter,
                        };
                        node.request_counter += 1;
                        node.request_buffer.push(msg.clone());
                    }

                    Command::Transfer { src, dst, amount } => {
                        debug!(amount, ?src, ?dst, "request to transfer");
                        let msg = LogEntry {
                            command_type: Transaction::Transfer,
                            acc1: src.clone(),
                            acc2: dst.clone(),
                            amount: amount,
                            term: node.current_term,
                            origin_id: node.address,
                            origin_nr: node.request_counter,
                        };
                        node.request_counter += 1;
                        node.request_buffer.push(msg.clone());
                    }

                    // control messages

                    // Accept a new channel
                    Command::Accept(channel) => {
                        node.connections[channel.address] = Some(node.accept(channel.clone()));
                        trace!(origin = channel.address, "accepted connection");
                    }

                    // Check periodically if a timeout happened
                    Command::CheckForTimeout {} => {
                        if node.state != State::Leader {
                            if last_leader_contact.elapsed() > Duration::from_millis(TIMEOUT_DELAY)
                            {
                                if node.state == State::Follower {
                                    trace!("registered a timeout from the leader");
                                } else {
                                    trace!("election timed out; return to follower status");
                                    node.state = State::Follower;
                                }
                                // wait random amount before starting a new election
                                last_timeout = Instant::now();
                                dynamic_waiting_time =
                                    rand::thread_rng().gen_range(1..TIMEOUT_DELAY);
                                node.send(Command::Timeout {}, address);
                            } else {
                                // Check later again
                                node.send(Command::CheckForTimeout {}, address);
                            }
                        }
                    }

                    // Check if during the dynamic waiting time a candidate was elected
                    Command::Timeout {} => {
                        // Check if another leader has already been found
                        if last_leader_contact.elapsed() < Duration::from_millis(TIMEOUT_DELAY) {
                            // Return to normal follower behaviour
                            node.send(Command::CheckForTimeout {}, address);
                            continue;
                        }

                        if last_timeout.elapsed() > Duration::from_millis(dynamic_waiting_time) {
                            // start election if dynamic waiting time passed
                            node.send(Command::Election {}, address);
                            node.send(Command::CheckForTimeout {}, address);
                        } else {
                            // Check later again
                            node.send(Command::Timeout {}, address);
                        }
                    }
                    // After 150 + (1-150)ms join the election as candidate
                    Command::Election {} => {
                        trace!(
                            "joined the election as candidate for term {}",
                            node.current_term + 1
                        );

                        // Reset votes (voted for himself)
                        node.votes = 1;
                        node.state = State::Candidate;
                        node.current_term += 1;
                        last_leader_contact = Instant::now();

                        // Inform all threads
                        for address in 0..office_count {
                            if address != node.address {
                                node.send(
                                    Command::RequestVote {
                                        candidate_id: node.address,
                                        candidate_term: node.current_term,
                                        last_log_index: node.log.len(),
                                        last_log_term: node.log.last().unwrap().term,
                                    },
                                    address,
                                );
                            }
                        }
                    }
                    // Request to vote for the sending candidate
                    Command::RequestVote {
                        candidate_id,
                        candidate_term,
                        last_log_term,
                        last_log_index,
                    } => {
                        // ignore older requests
                        if (candidate_term > node.current_term)
                            | ((candidate_term == node.current_term) & !node.vote_granted)
                        {
                            if node.state == State::Leader {
                                trace!(
                                    origin = candidate_id,
                                    "Resigned since new term detected (RequestVote)"
                                );
                                node.state = State::Follower;
                            }

                            if last_log_term > node.log.last().unwrap().term
                                || (last_log_term == node.log.last().unwrap().term
                                    && last_log_index >= node.log.len())
                            {
                                // Accept candidate
                                trace!(origin = candidate_id, "support election");
                                // Reset timeout
                                last_leader_contact = Instant::now();
                                // Change to next term -> can only vote once per term
                                node.current_term = candidate_term;
                                node.vote_granted = true;
                                // Send vote result to candidate
                                node.send(
                                    Command::VoteYes {
                                        voter_id: node.address,
                                    },
                                    candidate_id,
                                );
                            } else {
                                // reject candidate
                                trace!(origin = candidate_id, "denied election => log issues");
                                if candidate_term > node.current_term {
                                    node.current_term = candidate_term;
                                    node.vote_granted = false;
                                }
                                node.send(
                                    Command::VoteNo {
                                        voter_id: node.address,
                                        voter_term: node.current_term,
                                    },
                                    candidate_id,
                                );
                            }
                        } else {
                            // already voted or in newer term
                            trace!(
                                origin = candidate_id,
                                "denied election => already voted for this term"
                            );
                            node.send(
                                Command::VoteNo {
                                    voter_id: node.address,
                                    voter_term: node.current_term,
                                },
                                candidate_id,
                            );
                        }
                    }

                    // Receiving a positive vote
                    Command::VoteYes { voter_id } => {
                        trace!(origin = voter_id, "Received pos Vote");
                        node.votes += 1;
                        // Received enough votes
                        if (node.votes > office_count / 2) & (node.state == State::Candidate) {
                            node.state = State::Leader;
                            info!("is the new leader for term {}", node.current_term);
                            node.send(Command::SendingHeartbeat {}, node.address);
                        }
                    }

                    // Receiving a negative vote
                    Command::VoteNo {
                        voter_id,
                        voter_term,
                    } => {
                        trace!(origin = voter_id, "Received neg Vote");
                        if voter_term > node.current_term {
                            node.state = State::Follower;
                            trace!(
                                origin = voter_id,
                                "cancel candidate status since new term detected"
                            );
                        }
                    }

                    // Sending heartbeats every 120 ms
                    Command::SendingHeartbeat {} => {
                        if node.state == State::Leader {
                            if last_leader_contact.elapsed()
                                > Duration::from_millis(TIMEOUT_DELAY * 4 / 5)
                            {
                                trace!("sends heartbeat to all followers");
                                for address in 0..office_count {
                                    if address != node.address{
                                        node.send(
                                            Command::AppendEntry {
                                                leader_term: node.current_term,
                                                leader_commit: node.commit_index,
                                                last_index: node.log.len()-1,
                                                leader_id: node.address,
                                                last_entry: node.log.last().unwrap().clone(),
                                                current_entry: LogEntry {
                                                    command_type: Transaction::Heartbeat,
                                                    acc1: "".to_string(),
                                                    acc2: "".to_string(),
                                                    amount: 0,
                                                    origin_id: node.address,
                                                    origin_nr: 0,
                                                    term: 0,
                                                },
                                            },
                                            address,
                                        );
                                    }
                                }
                                last_leader_contact = Instant::now();
                                node.send(Command::SendingHeartbeat {}, node.address);
                            } else {
                                // Check request buffer
                                if (node.request_buffer.len() > 0) & !node.request_in_log {
                                    // internal message to himself with the request
                                    node.send(
                                        Command::ForwardedCommand {
                                            forwarded: node.request_buffer.first().unwrap().clone(),
                                            origin_id: node.address,
                                        },
                                        node.address,
                                    );
                                }
                                // Check later again
                                node.send(Command::SendingHeartbeat {}, node.address);
                            }

                        }
                    }

                    // AppendEntry from leader or Heartbeat
                    Command::AppendEntry {
                        leader_term,
                        leader_id,
                        leader_commit,
                        last_index,
                        last_entry,
                        current_entry,
                    } => {
                        // ignore commands from old leaders
                        if node.current_term > leader_term {
                            node.send(
                                Command::AppendEntryResponse {
                                    success: false,
                                    term: node.current_term,
                                    responder_id: node.address,
                                    responder_index: node.log.len() - 1,
                                },
                                leader_id,
                            );
                            trace!(
                                origin = leader_id,
                                "Received AppendEntry from old leader (term {})",
                                leader_term
                            );
                            continue;
                        }
                        // sender is the leader of the current term
                        else if node.current_term == leader_term {
                            last_leader_contact = Instant::now();
                            // Other candidates must return to follower status
                            if node.state == State::Candidate {
                                node.state = State::Follower;
                                node.send(Command::CheckForTimeout {}, node.address);
                                trace!(
                                    origin = leader_id,
                                    "Accept current leader (term {})",
                                    leader_term
                                );
                            } else if node.state == State::Leader { continue; }
                        }
                        // sender is the leader of a new term
                        else if node.current_term < leader_term {
                            trace!(
                                origin = leader_id,
                                "Accept new leader (term {})",
                                leader_term
                            );

                            node.current_term = leader_term;
                            node.vote_granted = false;
                            last_leader_contact = Instant::now();

                            // Other candidates and leaders must return to follower status
                            if (node.state == State::Candidate) | (node.state == State::Leader) {
                                node.state = State::Follower;
                                node.send(Command::CheckForTimeout {}, node.address);
                            }
                        }

                        // Check log consistency if the leader was valid
                        if compare_log_entries(node.log.last().unwrap(), &last_entry) {
                            // Check for heartbeat (empty new lo entry)
                            if current_entry.command_type == Transaction::Heartbeat {
                                trace!("Was empty heartbeat; Log consistent");
                                node.send(
                                    Command::AppendEntryResponse {
                                        success: true,
                                        term: node.current_term,
                                        responder_id: node.address,
                                        responder_index: node.log.len() - 1,
                                    },
                                    leader_id,
                                );
                            } else {
                                trace!("Received payload; Log consistent");

                                node.log.push(current_entry.clone());
                                node.follower_last_log_index[node.address] = node.log.len()-1;
                                trace!("Log appended; new last log index {}", node.log.len()-1);

                                // Check if the new entry was a request from this node
                                if node.request_buffer.len() > 0 {
                                    if compare_log_entries(
                                        &current_entry,
                                        node.request_buffer.first().unwrap(),
                                    ) {
                                        trace!("Stop sending requests to the leader");
                                        node.request_in_log = true;
                                    }
                                }
                                // inform leader about the success
                                node.send(
                                    Command::AppendEntryResponse {
                                        success: true,
                                        term: node.current_term,
                                        responder_id: node.address,
                                        responder_index: node.log.len() - 1,
                                    },
                                    leader_id,
                                );
                            }

                            // commit if log was consistent
                            while (leader_commit > node.commit_index)
                                & (node.commit_index < node.log.len() - 1)
                            {
                                node.commit_index += 1;
                                // if a request is
                                if node.request_in_log & !node.request_buffer.is_empty() {
                                    trace!("Compare to data");
                                    if compare_log_entries(
                                        &node.log[node.commit_index],
                                        node.request_buffer.first().unwrap(),
                                    ) {
                                        trace!("Committed own request");
                                        node.request_in_log = false;
                                        node.request_buffer.remove(0);
                                    }
                                }
                                trace!("committed log entry {}", node.commit_index);
                                let str = commit(
                                    node.log[node.commit_index].clone(),
                                    &mut node.local_bank_db,
                                );
                                node.append(&str);
                            }
                            // Check if the node needs to forward request to the leader
                            if !node.request_buffer.is_empty() & !node.request_in_log {
                                trace!("input buffer non-empty; forward request {}",
                                    node.request_buffer.first().unwrap().origin_id);
                                node.send(
                                    Command::ForwardedCommand {
                                        forwarded: node.request_buffer.first().unwrap().clone(),
                                        origin_id: node.address,
                                    },
                                    leader_id,
                                );
                            }
                        } else {
                            // If log was inconsistent and too long -> delete last element
                            if (node.commit_index < node.log.len() - 1) & (last_index<=node.log.len()-1) {
                                trace!("Log was inconsistent; Remove entry {}", node.log.len() - 1);
                                if !node.request_buffer.is_empty(){
                                    if compare_log_entries(
                                        &node.log[node.log.len() - 1],
                                        node.request_buffer.first().unwrap(),
                                    ) {
                                        node.request_in_log = false;
                                    }
                                }
                                node.log.remove(node.log.len() - 1);
                            } else if node.commit_index > node.log.len() - 1 {
                                warn!("Log was inconsistent; need additional elements");
                            }
                            // Inform the leader to retry
                            node.send(
                                Command::AppendEntryResponse {
                                    success: false,
                                    term: node.current_term,
                                    responder_id: node.address,
                                    responder_index: node.log.len()-1,
                                },
                                leader_id,
                            );
                        }
                    }

                    // AppendEntryResponse from follower
                    Command::AppendEntryResponse {
                        success,
                        term,
                        responder_id,
                        responder_index,
                    } => {
                        if term > node.current_term {
                            // Resign if a new term was detected
                            node.current_term = term;
                            node.vote_granted = false;
                            node.state = State::Follower;
                        } else {
                            if success {
                                // Remember each last log index of each follower
                                node.follower_last_log_index[responder_id] = responder_index;

                                let mut copy_of_log_indices = node.follower_last_log_index.clone();
                                copy_of_log_indices.sort();
                                //  calculate the index that at least 50%+1 nodes have in the log
                                let majority_index = copy_of_log_indices[((office_count + 1) / 2) -1];

                                if node.commit_index < majority_index {
                                    // Commit the additional entries
                                    while (majority_index > node.commit_index)
                                        & (node.commit_index < node.log.len() - 1)
                                    {
                                        node.commit_index += 1;
                                        if !node.request_buffer.is_empty() &  node.request_in_log {
                                            trace!("Compare to data");
                                            if node.request_in_log & compare_log_entries(
                                                &node.log[node.commit_index],
                                                node.request_buffer.first().unwrap(),
                                            ) {
                                                trace!("Remove saved entry");
                                                node.request_in_log = false;
                                                node.request_buffer.remove(0);
                                            } else {
                                                trace!("Other log entry do not remove saved entry");
                                            }
                                        }
                                        trace!("committed log entry {}", node.commit_index);
                                        let str = commit(
                                            node.log[node.commit_index].clone(),
                                            &mut node.local_bank_db,
                                        );
                                        node.append(&str);
                                    }
                                    info!("leader increases commit index to {}", node.commit_index);
                                }
                                trace!(
                                    "leader gets the info that node {} has {} as last log index",
                                    responder_id,
                                    responder_index
                                );
                            }

                            // Send additional log entries to each follower who is missing entries
                            if responder_index < node.log.len() - 1 {
                                node.send(
                                    Command::AppendEntry {
                                        leader_term: node.current_term,
                                        leader_commit: node.commit_index,
                                        leader_id: node.address,
                                        last_index: responder_index,
                                        last_entry: node.log.get(responder_index).unwrap().clone(),
                                        current_entry: node
                                            .log
                                            .get(responder_index + 1)
                                            .unwrap()
                                            .clone(),
                                    },
                                    responder_id,
                                );
                            }
                        }
                    }

                    // Forwarded commands to leader
                    Command::ForwardedCommand {
                        mut forwarded,
                        origin_id,
                    } => {
                        trace!( "asked to accept request {} from node {}", forwarded.origin_nr ,origin_id );

                        if (node.state == State::Leader)
                            & (node.follower_last_log_index[origin_id] == node.log.len() - 1)
                        {
                            trace!("request from node {} accepted -> send request to all followers", origin_id);
                            forwarded.term = node.current_term;
                            last_leader_contact = Instant::now();

                            for address in 0..office_count {
                                if address != node.address {
                                    node.send(
                                        Command::AppendEntry {
                                            leader_term: node.current_term,
                                            leader_commit: node.commit_index,
                                            leader_id: node.address,
                                            last_index: node.log.len()-1,
                                            last_entry: node.log.last().unwrap().clone(),
                                            current_entry: forwarded.clone(),
                                        },
                                        address,
                                    );
                                }
                            }

                            if (node.request_buffer.len() > 0) & !node.request_in_log {
                                trace!("Compare to data");
                                if compare_log_entries(
                                    &forwarded,
                                    node.request_buffer.first().unwrap(),
                                ) {
                                    node.request_in_log = true;
                                }
                            }
                            //let str = commit(forwarded.clone(), &mut node.local_bank_db);
                            //node.append(&str);
                            node.log.push(forwarded);
                            node.follower_last_log_index[node.address] = node.log.len()-1;


                        } else {
                            trace!("Log from node {} not consistent -> request ignored", origin_id);
                        }
                    }

                    // NOOP
                    Command::HeartBeat {} => {}
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
        .with_max_level(Level::DEBUG) //DEBUG for overview; TRACE for details
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
        sleep();
        [3] "Weber"   => withdraw(70);          // should fail
        sleep();
        [3] "Weber"   => deposit(30);           // should be okay -> failed transactions shouldn't stop the execution
        sleep(2);
    }

    Ok(())
}
