//! Implementation of the Raft Consensus Protocol for a banking application.

use core::time;
use std::{
    env::args,
    fs, io,
    thread::{self},
};

use rand::prelude::*;
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{debug, info, trace, trace_span, Level};


use crate::network::State;
use network::{daemon, Channel, NetworkNode};
use protocol::Command;

pub mod network;
pub mod protocol;

/// Creates and connects a number of branch offices for the bank.
pub fn setup_offices(office_count: usize, log_path: &str) -> io::Result<Vec<Channel<Command>>> {
    let mut channels = Vec::with_capacity(office_count);

    // create the log directory if needed
    fs::create_dir_all(log_path)?;

    // create various network nodes and start them
    for address in 0..office_count {
        let mut node = NetworkNode::new(address, &log_path)?;
        channels.push(node.channel());

        thread::spawn(move || {
            // configure a span to associate log-entries with this network node
            let _guard = trace_span!("NetworkNode", id = node.address);
            let _guard = _guard.enter();
            let mut last_leader_heartbeat_send = Instant::now();

            loop {
                match node.state {
                    State::Follower => {
                        if node.last_heartbeat.elapsed() > node.heartbeat_timeout {
                            debug!("missed heartbeat, converting to candidate state");
                            node.become_candidate();
                            node.start_election();
                        }
                    }

                    State::Candidate => {
                        if node.last_election_start.elapsed() > node.election_timeout {
                            debug!("election timeout, starting new election");
                            node.become_candidate();
                            node.start_election();
                        }
                    }

                    State::Leader => {
                        if last_leader_heartbeat_send.elapsed() > Duration::from_millis(150) {
                            last_leader_heartbeat_send = Instant::now();
                            node.send_heartbeat();
                        }
                    }
                }

                // set a timeout so we can check the state periodically
                let timeout = time::Duration::from_millis(20);

                while let Ok(cmd) = node.decode(Some(Instant::now() + timeout)) {
                    match cmd {
                        // customer requests
                        Command::Open { account } => {
                            debug!("request to open an account for {:?}", account);
                        }
                        Command::Deposit { account, amount } => {
                            debug!(amount, ?account, "request to deposit");
                        }
                        Command::Withdraw { account, amount } => {
                            debug!(amount, ?account, "request to withdraw");
                        }
                        Command::Transfer { src, dst, amount } => {
                            debug!(amount, ?src, ?dst, "request to transfer");
                        }

                        Command::NOOP => {
                            // debug!("NOOP");
                            continue;
                        }

                        // Accept a new channel
                        Command::Accept(channel) => {
                            node.connections
                                .insert(channel.address, node.accept(channel.clone()));
                            trace!(origin = channel.address, "accepted connection");
                        }

                        Command::RequestVoteRequest {
                            term,
                            candidate_id,
                            last_log_index: _,
                            last_log_term: _,
                        } => {
                            // debug!(
                            //     term,
                            //     candidate_id,
                            //     last_log_index,
                            //     last_log_term,
                            //     "received vote request"
                            // );

                            // reset the timer
                            // debug!("received vote request, resetting leader timeout");

                            // if node.last_heartbeat.elapsed() < node.heartbeat_timeout {
                            //     debug!("ignoring vote, still connected to a live leader");
                            //     node.send_vote(candidate_id, false);
                            //     continue;
                            // }

                            // check if the term on the responder is greater than our own,
                            // and if so convert from candidate to follower
                            if node.check_term_and_convert_to_follower_if_needed(term) {
                                node.send_vote(candidate_id, false);
                            }

                            // TODO: check if the responder has a log that is at least as up-to-date as our own
                            if node.voted_for.is_none() || node.voted_for == Some(candidate_id) {
                                node.voted_for = Some(candidate_id);
                                debug!(node.voted_for, "yes vote for candidate");
                                node.send_vote(candidate_id, true);
                            } else {
                                debug!(
                                    node.voted_for,
                                    "no vote, already voted for another candidate"
                                );
                                node.send_vote(candidate_id, false);
                            }

                            // // check if the responder has a log that is at least as up-to-date as our own
                            // let log_up_to_date = node.check_log_up_to_date(last_log_index, last_log_term);
                            // if log_up_to_date {
                            //     node.reset_election_timeout();
                            //     node.send_vote(candidate_id, true);
                            // } else {
                            //     node.send_vote(candidate_id, false);
                            // }
                        }

                        // Request for votes
                        Command::RequestVoteResponse { term, vote_granted } => {
                            // debug!(term, vote_granted, "received vote response");

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
                            if entries.is_empty() {
                                debug!(
                                    term,
                                    leader_id,
                                    // prev_log_index,
                                    // prev_log_term,
                                    // leader_commit,
                                    "received heartbeat"
                                );
                            } else {
                                debug!(
                                    term,
                                    leader_id,
                                    prev_log_index,
                                    prev_log_term,
                                    leader_commit,
                                    "received append entries"
                                );
                            }

                            node.last_heartbeat = Instant::now();

                            // check if the term on the responder is greater than our
                            // own, and if so update term and become follower
                            node.check_term_and_convert_to_follower_if_needed(term);

                            // check if current node is a candidate, and if so convert to follower
                            if node.is_candidate() {
                                debug!(
                                    leader_id,
                                    "candidate received append entries, converting to follower"
                                );
                                node.become_follower();
                            }
                        }

                        Command::AppendEntriesResponse { term, success } => {
                            debug!(term, success, "received append entries response");

                            // check if the term on the responder is greater than our own,
                            // and if so convert from candidate to follower
                            if node.check_term_and_convert_to_follower_if_needed(term) {
                                continue;
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
        .with_max_level(Level::TRACE)
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
