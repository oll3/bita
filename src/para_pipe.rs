use threadpool::ThreadPool;

use crate::ordered_mpsc::OrderedMPSC;

// Generic parallel data processing pipe
pub struct ParaPipe<'a, O, P, U>
where
    U: FnMut(O),
{
    pool: &'a ThreadPool,
    processor: P,
    outlet: U,
    channel: OrderedMPSC<O>,
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
            channel: OrderedMPSC::new(),
        }
    }

    pub fn input<I>(&mut self, data: I)
    where
        I: Send + 'static,
        O: Send + 'static,
        P: Fn(I) -> O + Send + 'static + Copy,
    {
        let channel_tx = self.channel.new_tx();
        let processor = self.processor;
        // TODO: Limit the number of queued jobs by blocking?
        self.pool.execute(move || {
            channel_tx.send(processor(data)).expect("forward");
        });
        let outlet = &mut self.outlet;
        self.channel.rx().try_iter().for_each(|out| {
            outlet(out);
        });
    }
}

impl<'a, O, P, U> Drop for ParaPipe<'a, O, P, U>
where
    U: FnMut(O),
{
    fn drop(&mut self) {
        self.pool.join();
        let outlet = &mut self.outlet;
        self.channel.rx().try_iter().for_each(|out| {
            outlet(out);
        });
    }
}
