use crossbeam_channel::{bounded, Receiver};
use threadpool::ThreadPool;

// Generic parallel data processing pipe
pub struct ParaPipe<'a, O, P, U>
where
    U: FnMut(O),
{
    pool: &'a ThreadPool,
    processor: P,
    outlet: U,
    waiting_result: Vec<Receiver<O>>,
}

impl<'a, O, P, U> ParaPipe<'a, O, P, U>
where
    U: FnMut(O),
{
    // The processors is executed inside a thread.
    // The outlet is called for each processed data. The call the outlet
    // is done in the same order as the call to the input method.
    pub fn new<I>(pool: &'a ThreadPool, processor: P, outlet: U) -> Self
    where
        P: Fn(I) -> O + Send + 'static + Copy,
    {
        ParaPipe {
            pool,
            processor,
            outlet,
            waiting_result: Vec::new(),
        }
    }

    pub fn input<I>(&mut self, data: I)
    where
        I: Send + 'static,
        O: Send + 'static,
        P: Fn(I) -> O + Send + 'static + Copy,
    {
        if self.waiting_result.len() > (self.pool.max_count() * 4) {
            // When the waiting queue is long then wait for some result before continuing
            let outlet = &mut self.outlet;
            outlet(self.waiting_result[0].recv().expect("recv"));
            self.waiting_result.remove(0);
        }

        let (tx, rx) = bounded::<O>(1);
        self.waiting_result.push(rx);
        let processor = self.processor;
        self.pool.execute(move || {
            tx.send(processor(data)).expect("forward");
        });

        // Forward finished results
        self.pass_on();
    }

    fn pass_on(&mut self) {
        let outlet = &mut self.outlet;
        let mut done = false;
        while !done && !self.waiting_result.is_empty() {
            let mut count = 0;
            self.waiting_result[0].try_iter().for_each(|out| {
                outlet(out);
                count += 1;
            });
            if count > 0 {
                self.waiting_result.remove(0);
            } else {
                done = true;
            }
        }
    }
}

impl<'a, O, P, U> Drop for ParaPipe<'a, O, P, U>
where
    U: FnMut(O),
{
    fn drop(&mut self) {
        while !self.waiting_result.is_empty() {
            let outlet = &mut self.outlet;
            outlet(self.waiting_result[0].recv().expect("recv"));
            self.waiting_result.remove(0);
        }
        //self.pool.join();
        //self.pass_on();
    }
}
