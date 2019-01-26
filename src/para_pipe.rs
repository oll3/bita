use threadpool::ThreadPool;

use crate::ordered_mpsc::OrderedMPSC;

// Generic parallel data processing pipe
pub struct ParaPipe<'a, O, P, U> {
    pool: &'a ThreadPool,
    processor: P,
    outlet: U,
    channel: OrderedMPSC<O>,
}

impl<'a, O, P, U> ParaPipe<'a, O, P, U> {
    pub fn new<I>(pool: &'a ThreadPool, processor: P, outlet: U) -> Self
    where
        P: Fn(I) -> O + Send + 'static + Copy,
        U: FnMut(O),
    {
        ParaPipe {
            pool,
            processor,
            outlet,
            channel: OrderedMPSC::new(),
        }
    }

    pub fn process<I>(&mut self, data: I)
    where
        I: Send + 'static,
        O: Send + 'static,
        U: FnMut(O),
        P: Fn(I) -> O + Send + 'static + Copy,
    {
        let channel_tx = self.channel.new_tx();
        let processor = self.processor;
        self.pool.execute(move || {
            channel_tx.send(processor(data)).expect("forward");
        });
        let outlet = &mut self.outlet;
        self.channel.rx().try_iter().for_each(|out| {
            outlet(out);
        });
    }

    pub fn finish(mut self)
    where
        U: FnMut(O),
    {
        self.pool.join();
        let outlet = &mut self.outlet;
        self.channel.rx().try_iter().for_each(|out| {
            outlet(out);
        });
    }
}
