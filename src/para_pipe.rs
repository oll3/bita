use crossbeam_channel::{bounded, Receiver};
use threadpool::ThreadPool;

// Generic parallel data processing pipe
pub struct ParaPipe<'a, O> {
    pool: &'a ThreadPool,
    output_callback: Box<FnMut(O) + 'a>,
    waiting_result: Vec<Receiver<O>>,
}

impl<'a, O> ParaPipe<'a, O> {
    // The processors is executed inside a thread.
    // The output is called for each processed data. The call to the output
    // callback is done in the same order as the call to the input method.
    pub fn new_output<U>(pool: &'a ThreadPool, output_callback: U) -> Self
    where
        O: Send + 'static,
        U: FnMut(O) + 'a,
    {
        ParaPipe {
            pool,
            output_callback: Box::new(output_callback),
            waiting_result: Vec::new(),
        }
    }

    pub fn input<I, P>(&mut self, data: I, processor: P)
    where
        P: Fn(I) -> O + Send + 'static + Copy,
        I: Send + 'static,
        O: Send + 'static,
    {
        if self.waiting_result.len() > (self.pool.max_count() * 4) {
            // When the waiting queue is long then wait for some result before continuing
            let output_callback = &mut self.output_callback;
            output_callback(self.waiting_result[0].recv().expect("recv"));
            self.waiting_result.remove(0);
        }

        let (tx, rx) = bounded::<O>(1);
        self.waiting_result.push(rx);
        self.pool.execute(move || {
            tx.send(processor(data)).expect("forward");
        });

        // Forward processed results
        self.pass_to_output();
    }

    fn pass_to_output(&mut self) {
        let output_callback = &mut self.output_callback;
        let mut done = false;
        while !done && !self.waiting_result.is_empty() {
            let mut count = 0;
            self.waiting_result[0].try_iter().for_each(|out| {
                output_callback(out);
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

impl<'a, O> Drop for ParaPipe<'a, O> {
    fn drop(&mut self) {
        while !self.waiting_result.is_empty() {
            let output_callback = &mut self.output_callback;
            output_callback(self.waiting_result[0].recv().expect("recv"));
            self.waiting_result.remove(0);
        }
    }
}
