use std::sync::mpsc::{channel, Receiver, SendError, Sender};

pub struct OrderedSender<T> {
    tx: Sender<Option<T>>,
}

impl<T> OrderedSender<T> {
    pub fn send(&self, obj: T) -> Result<(), SendError<Option<T>>> {
        self.tx.send(Some(obj))
    }

    pub fn done(self) {
        // Will consume the sender and hence result in a drop
    }
}

impl<T> Drop for OrderedSender<T> {
    fn drop(&mut self) {
        self.tx.send(None).expect("drop");
    }
}

pub struct OrderedMPSC<T> {
    outer_rx: Receiver<T>,
    inner_to_outer_tx: Sender<T>,
    inner_rx: Vec<Receiver<Option<T>>>,
}

impl<T> OrderedMPSC<T> {
    pub fn new() -> Self {
        let (tx, rx) = channel();
        OrderedMPSC {
            inner_to_outer_tx: tx,
            outer_rx: rx,
            inner_rx: Vec::new(),
        }
    }

    pub fn new_tx(&mut self) -> OrderedSender<T> {
        let (tx, rx) = channel();
        self.inner_rx.push(rx);
        OrderedSender { tx }
    }

    pub fn rx(&mut self) -> &Receiver<T> {
        while !self.inner_rx.is_empty() {
            let mut done = false;
            self.inner_rx[0].try_iter().for_each(|obj_opt| {
                if let Some(obj) = obj_opt {
                    self.inner_to_outer_tx.send(obj).expect("forward");
                } else {
                    done = true;
                }
            });

            if done {
                self.inner_rx.remove(0);
            } else {
                break;
            }
        }
        &self.outer_rx
    }
}

#[cfg(test)]
mod tests {
    use crate::ordered_mpsc::OrderedMPSC;
    #[test]
    fn test_ordering() {
        let mut channel = OrderedMPSC::new();
        {
            let tx1 = channel.new_tx();
            let tx2 = channel.new_tx();
            let tx3 = channel.new_tx();
            tx3.send("5");
            tx3.send("6");
            tx3.send("7");
            tx3.done();
            tx2.send("2");
            tx2.send("3");
            tx2.send("4");
            tx1.send("1");
        }

        let result: Vec<&str> = channel.rx().try_iter().collect();
        assert_eq!(result, ["1", "2", "3", "4", "5", "6", "7"]);
    }
}
