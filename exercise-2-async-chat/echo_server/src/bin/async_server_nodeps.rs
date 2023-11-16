use std::{net, io::{self, Read, Write}, pin::pin};

// main program with no additional dependencies
fn main() -> io::Result<()> {
	let exec = Executor::new();
	let spawner = exec.spawner();
	
	exec.run(async {
		let listener = TcpListener::bind("127.0.0.1:8000")?;
		
		println!("Server: listening on {:?}", listener);
		
		while let Ok((mut stream, _)) = listener.accept().await {
			spawner.spawn(async move {
				let mut buf = [0u8; 256];
				
				println!("Server: connected {:?}", stream);
				while let Ok(bytes) = stream.read(&mut buf).await {
					if bytes == 0 || stream.write(&buf[0..bytes]).await.is_err() {
						break;
					}
				}
				println!("Server: disconnected {:?}", stream);
			});
		}
		
		Ok(())
	})
}

/* **************************** library contents **************************** */
// the following code belongs into a library but is shown here for clarity

// additional imports for the necessary execution environment
use std::{
	pin::Pin,
	cell::RefCell,
	net::ToSocketAddrs,
	io::ErrorKind,
	future::Future,
	task::{self, Poll, Context}
};

/* ************************** executor environment ************************** */

// heterogeneous, pinned list of futures
type FutureQ = Vec<Pin<Box<dyn Future<Output=()>>>>;

/// A simple executor that drives the event loop.
struct Executor {
	sched: RefCell<FutureQ>
}

impl Executor {
	/// Constructs a new executor.
	pub fn new() -> Self {
		Executor {
			sched: RefCell::new(vec![])
		}
	}
	
	/// Creates and returns a Spawner that can be used to insert new futures
	/// into the executors future queue.
	pub fn spawner(&self) -> Spawner {
		Spawner(&self.sched)
	}
	
	/// Runs a future to completion.
	pub fn run<F: Future>(&self, mut future: F) -> F::Output {
		// construct a custom context to pass into the poll()-method
		let simple_waker = SimpleWaker;
		let waker = unsafe {
			task::Waker::from_raw(task::RawWaker::new(
				&simple_waker as *const _ as *const (),
				&EXECUTOR_WAKER_VTABLE
			))
		};
		let mut cx = Context::from_waker(&waker);
		
		// pin the passed future for the duration of this function;
		// note: this is safe since we're not moving it around
		let mut future = pin!(future);
		let mut sched = FutureQ::new();
		
		// implement a busy-wait event loop for simplicity;
		// needs to be made non-busy to qualify for bonus points!
		loop {
			// poll the requested future, allowing for additional futures to spawn
			if let Poll::Ready(val) = future.as_mut().poll(&mut cx) {
				break val;
			}
			
			// swap the scheduled futures with an empty queue
			std::mem::swap(&mut sched, &mut self.sched.borrow_mut());
			
			// iterate over all futures presently scheduled
			for mut future in sched.drain(..) {
				// if they are not completed, reschedule
				if let Poll::Pending = future.as_mut().poll(&mut cx) {
					self.sched.borrow_mut().push(future);
				}
			}
		}
	}
}

/// A handle to the executors future queue that can be used to insert new tasks.
struct Spawner<'a>(&'a RefCell<FutureQ>);

impl<'a> Spawner<'a> {
	pub fn spawn(&self, future: impl Future<Output=()> + 'static) {
		self.0.borrow_mut().push(Box::pin(future));
	}
}

/* **************************** specialized waker *************************** */

/// Simple waker that isn't really used (yet) except to show the mechanism.
struct SimpleWaker;

impl SimpleWaker {
	unsafe fn clone(_this: *const ()) -> task::RawWaker {
		unimplemented!()
	}
	
	unsafe fn wake(_this: *const ()) {
		unimplemented!()
	}
	
	unsafe fn wake_by_ref(_this: *const ()) {
		unimplemented!()
	}
	
	unsafe fn drop(_this: *const ()) {}
}

/// Virtual function table for the simple waker. None of these do anything.
static EXECUTOR_WAKER_VTABLE: task::RawWakerVTable = task::RawWakerVTable::new(
	SimpleWaker::clone,
	SimpleWaker::wake,
	SimpleWaker::wake_by_ref,
	SimpleWaker::drop
);

/* ************************** asynchronous wrappers ************************* */

/// Asynchronous wrapper for the standard TcpStream.
#[derive(Debug)]
struct TcpStream(net::TcpStream);

/// Asynchronous wrapper for the standard TcpListener.
#[derive(Debug)]
struct TcpListener(net::TcpListener);

impl TcpStream {
	pub fn read<'a>(&'a mut self, buf: &'a mut [u8])
	-> impl Future<Output = io::Result<usize>> + 'a {
		IoFuture(move || self.0.read(buf))
	}
	
	pub fn write<'a>(&'a mut self, buf: &'a [u8])
	-> impl Future<Output = io::Result<usize>> + 'a {
		IoFuture(move || self.0.write(buf))
	}
}

// only necessary methods are implemented
impl TcpListener {
	pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<TcpListener> {
		let stream = net::TcpListener::bind(addr)?;
		
		// setup Listener as non-blocking, calls to accept() may return WouldBlock
		stream.set_nonblocking(true)?;
		Ok(TcpListener(stream))
	}
	
	pub fn accept(&self)
	-> impl Future<Output = io::Result<(TcpStream, net::SocketAddr)>> + '_ {
		IoFuture(move || {
			self.0
				.accept()
				.map(|(stream, addr)| {
					// setup TcpStream non-blocking, calls to read() may return WouldBlock
					stream.set_nonblocking(true).unwrap();
					(TcpStream(stream), addr)
				})
		})
	}
}

/// Helper struct that implements Future for non-blocking IO passed in via lambda.
struct IoFuture<R, F>(F)
where F: FnMut() -> io::Result<R>;

impl<R, F> Future for IoFuture<R, F>
where F: FnMut() -> io::Result<R> + Unpin {
	type Output = io::Result<R>;
	
	fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		// call the lambda and check if the Poll-result indicates blocking
		match self.get_mut().0() {
			Ok(val) => Poll::Ready(Ok(val)),
			Err(err) => {
				if err.kind() != ErrorKind::WouldBlock {
					Poll::Ready(Err(err))
				} else {
					Poll::Pending
				}
			}
		}
	}
}
