//! Implementation of the Raft Consensus Protocol for a banking application.

use std::{env::args, fs, io, thread};
use std::collections::HashMap;
use std::thread::sleep;
use std::time::{Duration, Instant};

use rand::prelude::*;
#[allow(unused_imports)]
use tracing::{debug, info, Level, trace, trace_span};
use tracing_subscriber::fmt::time;

use network::{Channel, daemon, NetworkNode};
use protocol::Command;
use crate::network::Connection;
use crate::protocol::State;

pub mod network;
pub mod protocol;

/// Creates and connects a number of branch offices for the bank.
pub fn setup_offices(office_count: usize, log_path: &str) -> io::Result<Vec<Channel<Command>>> {
	let mut channels = Vec::with_capacity(office_count);
	
	// create the log directory if needed
	fs::create_dir_all(log_path)?;
	
	// create various network nodes and start them
	for address in 0..office_count {
		let node = NetworkNode::new(address, &log_path)?;
		channels.push(node.channel());
		
		thread::spawn(move || {
			// configure a span to associate log-entries with this network node
			let _guard = trace_span!("NetworkNode", id = node.address);
			let _guard = _guard.enter();
			let mut connections: HashMap<usize, Connection<Command>> = HashMap::new();
			connections.insert(node.address, node.accept(node.channel()));
			let _ = connections.get(&node.address).unwrap().encode(Command::CheckForTimeout{});
			let mut leader = node.address;
			let mut current_term = 0;
			let mut log_term = 0;
			let mut log_index = 0;
			let mut votes = 0;
			let mut timemult = 0;
			let mut last_leader_contact = Instant::now();
			let mut last_timeout = Instant::now();
			let mut dynamic_waiting_time = 0;
			let mut state = State::Follower;
			
			// dispatching event loop
			while let Ok(cmd) = node.decode(None) {
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
					
					// control messages

					// Accept a new channel
					Command::Accept(channel) => {
						//let ids = channel.address;
						connections.insert(channel.address, node.accept(channel.clone()));
						trace!(origin = channel.address, "accepted connection");
					}

					// Check periodically if a timeout happened
					Command::CheckForTimeout {} => {
						if state != State::Leader {
							if last_leader_contact.elapsed() > Duration::from_millis(300) {
								trace!("registered a timeout from the leader");
								// wait random amount before starting a new elecetion
								last_timeout = Instant::now();
								dynamic_waiting_time = rand::thread_rng().gen_range(1..300);
								let _ = connections.get(&node.address).unwrap().encode(Command::Timeout {});

							} else {
								// Check every 50ms the status of the leader
								sleep(Duration::from_millis(50*timemult));
								let _ = connections.get(&node.address).unwrap().encode(Command::CheckForTimeout{});
							}
						}
					}
					// Check if during the dynamic waiting time a candidate was elected
					Command::Timeout {} => {
						// Check if another leader has already been found
						if last_leader_contact.elapsed() < Duration::from_millis(300) {
							// Return to normal follower behaviour
							let _ = connections.get(&node.address).unwrap().encode(Command::CheckForTimeout{});
							continue;
						}

						if last_timeout.elapsed() > Duration::from_millis(dynamic_waiting_time) {
							// start election if dynamic waiting time passed
							let _ = connections.get(&node.address).unwrap().encode(Command::CheckForTimeout{});
							let _ = connections.get(&node.address).unwrap().encode(Command::Election {});
						} else {
							// Check every 25ms again
							sleep(Duration::from_millis(25*timemult));
							let _ = connections.get(&node.address).unwrap().encode(Command::Timeout{});
						}
					}
					// After 300ms + (1-300)ms join the election as candidate
					Command::Election {} => {
						trace!("joined the election as candidate");
						// Reset votes
						votes = 0;
						state = State::Candidate;
						// Inform all threads
						for cons in &connections {
							let _ = cons.1.encode(Command::ElectMe{
								candidate_id: node.address,
								last_entry_index: 0,
								last_entry_term: 0});
						}
					}
					// All threads are required to vote
					Command::ElectMe { candidate_id, last_entry_term, last_entry_index } => {
						if last_entry_term > log_term || (last_entry_term == log_term && last_entry_index >= log_index) {
							trace!(origin = candidate_id, "support election");
							// Reset timeout
							last_leader_contact = Instant::now();
							// Change to next term
							current_term+=1; //-> is in Vote yes
							// Send vote result to candidate
							let _ = connections.get(&candidate_id).unwrap().encode(Command::VoteYes {origin_id: node.address});
						} else {
							trace!(origin = candidate_id, "denied election");
							let _ = connections.get(&candidate_id).unwrap().encode(Command::VoteNo {origin_id: node.address});
						}
					}
					// Receiving a positive vote
					Command::VoteYes { origin_id } => {
						trace!(origin = origin_id, "Received pos Vote");
						votes += 1;
						// Received enough votes
						if(votes > office_count/2){
							//current_term+=1;
							votes = 0;
							state = State::Leader;
							trace!("is the new leader for term {}", current_term);
							let _ = connections.get(&node.address).unwrap().encode(Command::SendingHeartbeat{});
						}
					}

					// Receiving a negative vote
					Command::VoteNo { origin_id } => {
						trace!(origin = origin_id, "Received neg Vote");
					}

					// Sending heartbeats every 250ms
					Command::SendingHeartbeat {} => {
						if last_leader_contact.elapsed() > Duration::from_millis(250) {
							trace!("sends hearbeats to all followers");
							for cons in &connections {
								let _ = cons.1.encode(Command::HeartBeat{ leader_term: current_term});
							}
							last_leader_contact = Instant::now();
							let _ = connections.get(&node.address).unwrap().encode(Command::SendingHeartbeat{});
						} else {
							// Check every 10ms again
							sleep(Duration::from_millis(10*timemult));
							let _ = connections.get(&node.address).unwrap().encode(Command::SendingHeartbeat{});
						}
					}
					// Can be merged into Append
					Command::HeartBeat { leader_term } => {
						if state == State::Leader {
							// Resign if a new leader is found
							if current_term<leader_term {
								state = State::Follower;
								current_term=leader_term;
								trace!("Resigned in favor of new leader");
								last_leader_contact = Instant::now();
								let _ = connections.get(&node.address).unwrap().encode(Command::CheckForTimeout{});
							}
							// Otherwise ignore heartbeat
							else if current_term>leader_term{
								trace!("Received Heartbeat from older leader");
							}
						}
						else if state == State::Candidate {
							// Resign if a new leader is found
							if current_term-1<=leader_term {
								state = State::Follower;
								current_term=leader_term;
								trace!("Cancel Election in favor of current or new leader");
								last_leader_contact = Instant::now();
								let _ = connections.get(&node.address).unwrap().encode(Command::CheckForTimeout{});
							}
							// Otherwise ignore heartbeat
							else if current_term>leader_term{
								trace!("Received Heartbeat from older leader");
							}
						}
						else if state==State::Follower {
							if current_term>leader_term {
								trace!("Received Heartbeat from older leader");
							}
							if current_term==leader_term {
								trace!("Received Heartbeat from known leader");
								// Reset timeout
								last_leader_contact = Instant::now();
							}
							if current_term<leader_term {
								trace!("Received Heartbeat from new leader");
								current_term=leader_term;
								// Reset timeout
								last_leader_contact = Instant::now();
							}
						}
					}

					Command::Append { current_command, last_command } => {}
				}
			}
		});
	}
	
	// connect the network nodes in random order
	let mut rng = thread_rng();
	for src in channels.iter() {
		for dst in channels.iter().choose_multiple(&mut rng, office_count) {
			if src.address == dst.address { continue; }
			src.send(Command::Accept(dst.clone()));
		}
	}
	
	Ok(channels)
}

fn main() -> io::Result<()> {
	use tracing_subscriber::{FmtSubscriber, fmt::time::ChronoLocal};
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
