use crate::{event::Event, event::TickSizingStrategy, level::Level};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Orderbook {
    best_bid: Option<Level>,
    best_ask: Option<Level>,
    pub bids: BTreeMap<u64, Level>,
    pub asks: BTreeMap<u64, Level>,
    last_updated: u64,
    last_sequence: u64,
    pub inv_tick_size: f64,
    pub tick_strategy: TickSizingStrategy,
}

impl Default for Orderbook {
    fn default() -> Self {
        Self::new(1.0)
    }
}

impl Orderbook {
    pub fn new(tick_size: f64) -> Self {
        Self::new_with_strategy(tick_size, TickSizingStrategy::Fixed)
    }

    pub fn new_with_strategy(tick_size: f64, tick_strategy: TickSizingStrategy) -> Self {
        Self {
            best_bid: None,
            best_ask: None,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_updated: 0,
            last_sequence: 0,
            inv_tick_size: 1.0 / tick_size,
            tick_strategy,
        }
    }

    #[inline(always)]
    pub fn get_price_tick(&self, price: f64) -> u64 {
        self.tick_strategy.price_to_tick(price, self.inv_tick_size)
    }

    #[inline]
    pub fn process_raw(
        &mut self,
        timestamp: u64,
        seq: u64,
        is_trade: bool,
        is_buy: bool,
        price: f64,
        size: f64,
    ) {
        let event = Event {
            timestamp,
            seq,
            is_trade,
            is_buy,
            price,
            size,
        };

        self.process(event);
    }

    #[inline]
    pub fn process_stream_bbo_raw(
        &mut self,
        timestamp: u64,
        seq: u64,
        is_trade: bool,
        is_buy: bool,
        price: f64,
        size: f64,
    ) -> Option<(Option<Level>, Option<Level>)> {
        let event = Event {
            timestamp,
            seq,
            is_trade,
            is_buy,
            price,
            size,
        };

        self.process_stream_bbo(event)
    }

    #[inline]
    pub fn process(&mut self, event: Event) {
        // Per-level seq checking is now done in process_bid_level/process_ask_level
        // Keep global tracking for reference but don't reject based on it
        match event.is_trade {
            true => self.process_trade(event),
            false => self.process_lvl2(event),
        };

        // Update global sequence tracker (may be used for monitoring/debugging)
        if event.timestamp > self.last_updated {
            self.last_updated = event.timestamp;
        }
        if event.seq > self.last_sequence {
            self.last_sequence = event.seq;
        }
    }

    #[inline]
    pub fn process_stream_bbo(&mut self, event: Event) -> Option<(Option<Level>, Option<Level>)> {
        let old_bid = self.best_bid;
        let old_ask = self.best_ask;

        self.process(event);

        let new_bid = self.best_bid;
        let new_ask = self.best_ask;

        if old_bid != new_bid || old_ask != new_ask {
            Some((new_bid, new_ask))
        } else {
            None
        }
    }

    #[inline]
    fn process_lvl2(&mut self, event: Event) {
        let price_ticks = self.get_price_tick(event.price);
        match event.is_buy {
            true => self.fm_process_lvl2_bid(event, price_ticks),
            false => self.fm_process_lvl2_ask(event, price_ticks),
        }
    }

    fn fm_update_best_bid(&mut self) {
        // jj: must run the gc function separately.
        // If we prune immediately, we get incorrect results.
        for level in self.bids.values().rev() {
            if level.size != 0.0 {
                self.best_bid = Some(*level);
                return;
            }
        }
    }

    fn fm_update_best_ask(&mut self) {
        // jj: must run the gc function separately.
        // If we prune immediately, we get incorrect results.
        for level in self.asks.values() {
            if level.size != 0.0 {
                self.best_ask = Some(*level);
                return;
            }
        }
    }

    fn fm_process_lvl2_bid(&mut self, event: Event, price_ticks: u64) {
        // Check seq staleness for existing levels
        // Do NOT use equal here, as we may want to UPDATE a level.
        // e.g., update data from lazily deleted level (size=0.0), which has same seq.
        if let Some(existing_level) = self.bids.get(&price_ticks) {
            if event.seq < existing_level.seq {
                return; // Stale or equal seq, skip
            }
        }

        // If this would become the new best bid, check seq against current best bid
        if let Some(best_bid) = self.best_bid {
            if event.price > best_bid.price && event.seq < best_bid.seq {
                return; // Stale event trying to become new best, ignore
            }
        }

        // Insert/update level (seq is newer or level doesn't exist)
        let new_level = Level::from(event);
        self.bids.insert(price_ticks, new_level);

        self.fm_update_best_bid();
    }

    fn fm_process_lvl2_ask(&mut self, event: Event, price_ticks: u64) {
        // Check seq staleness for existing levels.
        // Do NOT use equal here, as we may want to UPDATE a level.
        // e.g., update data from lazily deleted level (size=0.0), which has same seq.
        if let Some(existing_level) = self.asks.get(&price_ticks) {
            if event.seq < existing_level.seq {
                return; // Stale or equal seq, skip
            }
        }

        // If this would become the new best ask, check seq against current best ask
        if let Some(best_ask) = self.best_ask {
            if event.price < best_ask.price && event.seq < best_ask.seq {
                return; // Stale event trying to become new best, ignore
            }
        }

        // Insert/update level (seq is newer or level doesn't exist)
        let new_level = Level::from(event);
        self.asks.insert(price_ticks, new_level);

        self.fm_update_best_ask();
    }

    #[inline]
    fn process_trade(&mut self, event: Event) {
        let price_ticks = self.get_price_tick(event.price);

        let buf = match event.is_buy {
            true => &mut self.bids,
            false => &mut self.asks,
        };

        if let Some(level) = buf.get_mut(&price_ticks) {
            if event.size >= level.size {
                buf.remove(&price_ticks);
            } else {
                level.size -= event.size;
            }
        };
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.best_bid
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.best_ask
    }

    #[inline]
    pub fn top_bids(&self, n: usize) -> Vec<Level> {
        self.bids.values().rev().take(n).cloned().collect()
    }

    #[inline]
    pub fn top_asks(&self, n: usize) -> Vec<Level> {
        self.asks.values().take(n).cloned().collect()
    }

    #[inline]
    pub fn midprice(&self) -> Option<f64> {
        if let (Some(best_bid), Some(best_ask)) = (self.best_bid, self.best_ask) {
            return Some((best_bid.price + best_ask.price) / 2.0);
        }

        None
    }

    #[inline]
    pub fn weighted_midprice(&self) -> Option<f64> {
        if let (Some(best_bid), Some(best_ask)) = (self.best_bid, self.best_ask) {
            let num = best_bid.size * best_ask.price + best_bid.price * best_ask.size;
            let den = best_bid.size + best_ask.size;
            return Some(num / den);
        }

        None
    }

    // Prune price levels better than (as in best bid/ask) the new best price.
    // is_bids: apply to bids if true, asks if false
    pub fn fm_delete_bid_levels_in_range(&mut self, p1: f64, p2: f64, seq: u64) {
        let min_price = p1.min(p2);
        let cutoff_tick = self.get_price_tick(min_price);

        // Iterate through bids and set size to 0.0 for items with key >= cutoff_tick and level.seq < seq
        for (tick, level) in self.bids.iter_mut().rev() {
            if tick < &cutoff_tick {
                break;
            }
            if level.seq < seq {
                level.size = 0.0;
                level.seq = seq;
            }
        }

        self.fm_update_best_bid();
    }

    pub fn fm_delete_ask_levels_in_range(&mut self, p1: f64, p2: f64, seq: u64) {
        let max_price = p1.max(p2);
        let cutoff_tick = self.get_price_tick(max_price);

        // Iterate through asks and set size to 0.0 for items with key <= cutoff_tick and level.seq < seq
        for (tick, level) in self.asks.iter_mut() {
            if tick > &cutoff_tick {
                break;
            }
            if level.seq < seq {
                level.size = 0.0;
                level.seq = seq;
            }
        }

        self.fm_update_best_ask();
    }

    /// Garbage collect zero-size levels with seq older than the threshold.
    /// This removes lazy-deleted levels that are no longer needed for staleness checks.
    pub fn fm_gc_zero_size_levels(&mut self, seq_threshold: u64) {
        self.bids
            .retain(|_, level| level.size != 0.0 || level.seq >= seq_threshold);
        self.asks
            .retain(|_, level| level.size != 0.0 || level.seq >= seq_threshold);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_lvl2_bids() {
        let mut ob = Orderbook::new(0.01);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 16.0,
            size: 1.0,
        };

        ob.process(event);

        assert_eq!(
            ob.top_bids(5),
            [Level {
                price: 16.0,
                size: 1.0,
                timestamp: 0,
                seq: 0
            },]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 7.0,
            size: 1.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_bids(5),
            [
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 10.0,
            size: 1.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_bids(5),
            [
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 8.0,
            size: 1.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_bids(5),
            [
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 8.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        // Update price 8.0 with a newer seq to change size from 1.0 to 2.0
        let event = Event {
            timestamp: 0,
            seq: 1,
            is_trade: false,
            is_buy: true,
            price: 8.0,
            size: 2.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_bids(5),
            [
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 8.0,
                    size: 2.0,
                    timestamp: 0,
                    seq: 1
                },
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 12.0,
            size: 1.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_bids(5),
            [
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 12.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 8.0,
                    size: 2.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 21.0,
            size: 1.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_bids(5),
            [
                Level {
                    price: 21.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 12.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 8.0,
                    size: 2.0,
                    timestamp: 0,
                    seq: 1
                },
            ]
        );

        // Delete price 8.0 with a newer seq
        let event = Event {
            timestamp: 0,
            seq: 2,
            is_trade: false,
            is_buy: true,
            price: 8.0,
            size: 0.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_bids(5),
            [
                Level {
                    price: 21.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 12.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        // Re-add price 8.0 with a newer seq
        let event = Event {
            timestamp: 0,
            seq: 3,
            is_trade: false,
            is_buy: true,
            price: 8.0,
            size: 10.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_bids(5),
            [
                Level {
                    price: 21.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 12.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 8.0,
                    size: 10.0,
                    timestamp: 0,
                    seq: 3
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 21.0,
            size: 0.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_bids(5),
            [
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 12.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 8.0,
                    size: 10.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );
    }

    #[test]
    fn process_lvl2_asks() {
        let mut ob = Orderbook::new(0.01);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 16.0,
            size: 1.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_asks(5),
            [Level {
                price: 16.0,
                size: 1.0,
                timestamp: 0,
                seq: 0
            },]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 7.0,
            size: 1.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_asks(5),
            [
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 10.0,
            size: 1.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_asks(5),
            [
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 6.0,
            size: 1.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_asks(5),
            [
                Level {
                    price: 6.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 8.0,
            size: 2.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_asks(5),
            [
                Level {
                    price: 6.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 8.0,
                    size: 2.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 50.0,
            size: 1.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_asks(5),
            [
                Level {
                    price: 6.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 8.0,
                    size: 2.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 6.0,
            size: 0.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_asks(5),
            [
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 8.0,
                    size: 2.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 50.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 8.0,
            size: 0.0,
        };
        ob.process(event);

        assert_eq!(
            ob.top_asks(5),
            [
                Level {
                    price: 7.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 16.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 50.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );
    }

    #[test]
    fn process_all_asks() {
        let mut ob = Orderbook::new(0.01);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 11.0,
            size: 1.0,
        };
        ob.process(event);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 10.0,
            size: 1.0,
        };
        ob.process(event);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 9.0,
            size: 1.0,
        };
        ob.process(event);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: true,
            is_buy: false,
            price: 9.0,
            size: 0.5,
        };
        ob.process(event);

        assert_eq!(
            ob.top_asks(3),
            [
                Level {
                    price: 9.0,
                    size: 0.5,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 11.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: true,
            is_buy: false,
            price: 9.0,
            size: 0.9,
        };
        ob.process(event);

        assert_eq!(
            ob.top_asks(3),
            [
                Level {
                    price: 10.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
                Level {
                    price: 11.0,
                    size: 1.0,
                    timestamp: 0,
                    seq: 0
                },
            ]
        );
    }

    #[test]
    fn old_event() {
        let mut ob = Orderbook::new(0.01);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 11.0,
            size: 1.0,
        };
        ob.process(event);

        let event = Event {
            timestamp: 1,
            seq: 1,
            is_trade: false,
            is_buy: false,
            price: 10.0,
            size: 1.0,
        };
        ob.process(event);

        // This event at price 9.0 would become new best ask, but has stale seq (0 < 1)
        // so it should be rejected
        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 9.0,
            size: 1.0,
        };
        ob.process(event);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: true,
            is_buy: false,
            price: 8.0,
            size: 1.0,
        };
        ob.process(event);

        // Best ask should remain 10.0 (stale event at 9.0 was rejected)
        assert_eq!(
            ob.best_ask.unwrap(),
            Level {
                price: 10.0,
                size: 1.0,
                timestamp: 1,
                seq: 1
            }
        )
    }

    #[test]
    fn process_stream_bbo() {
        let mut ob = Orderbook::new(0.01);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 16.0,
            size: 1.0,
        };

        ob.process(event);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 20.0,
            size: 1.0,
        };

        ob.process(event);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 22.0,
            size: 1.0,
        };

        ob.process(event);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 21.0,
            size: 1.0,
        };

        let (best_bid, best_ask) = ob.process_stream_bbo(event).unwrap();

        assert_eq!(
            best_bid.unwrap(),
            Level {
                price: 20.0,
                size: 1.0,
                timestamp: 0,
                seq: 0
            }
        );

        assert_eq!(
            best_ask.unwrap(),
            Level {
                price: 21.0,
                size: 1.0,
                timestamp: 0,
                seq: 0
            }
        );

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 23.0,
            size: 1.0,
        };

        assert_eq!(ob.process_stream_bbo(event), None);
    }

    #[test]
    fn remove_non_existing_level_with_trade() {
        let mut ob = Orderbook::new(0.01);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 16.0,
            size: 1.0,
        };

        ob.process(event);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 20.0,
            size: 1.0,
        };

        ob.process(event);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: true,
            is_buy: true,
            price: 12.0,
            size: 1.0,
        };

        ob.process(event);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: true,
            is_buy: false,
            price: 22.0,
            size: 1.0,
        };

        ob.process(event);

        assert_eq!(
            ob.best_bid().unwrap(),
            Level {
                price: 16.0,
                size: 1.0,
                timestamp: 0,
                seq: 0
            }
        );

        assert_eq!(
            ob.best_ask().unwrap(),
            Level {
                price: 20.0,
                size: 1.0,
                timestamp: 0,
                seq: 0
            }
        );
    }

    #[test]
    fn midprice() {
        let mut ob = Orderbook::new(0.01);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 16.0,
            size: 1.0,
        };

        ob.process(event);

        assert_eq!(ob.midprice(), None);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 20.0,
            size: 1.0,
        };

        ob.process(event);

        let midprice = ob.midprice().unwrap();

        assert_eq!(midprice, 18.0)
    }

    #[test]
    fn weighted_midprice() {
        let mut ob = Orderbook::new(0.01);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: true,
            price: 16.0,
            size: 1.0,
        };

        ob.process(event);

        assert_eq!(ob.weighted_midprice(), None);

        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 20.0,
            size: 4.0,
        };

        ob.process(event);

        let weighted_midprice = ob.weighted_midprice().unwrap();

        assert_eq!(weighted_midprice, 16.8)
    }

    #[test]
    fn test_reject_stale_seq_update() {
        let mut ob = Orderbook::new(0.01);

        // Insert level with seq=100
        ob.process(Event {
            timestamp: 1000,
            seq: 100,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 10.0,
        });

        // Try to update with seq=50 (stale)
        ob.process(Event {
            timestamp: 2000, // Even newer timestamp
            seq: 50,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 20.0,
        });

        // Verify size unchanged (stale update rejected)
        let level = ob.bids.get(&ob.get_price_tick(50.0)).unwrap();
        assert_eq!(level.size, 10.0);
        assert_eq!(level.seq, 100);
    }

    #[test]
    fn test_accept_newer_seq_update() {
        let mut ob = Orderbook::new(0.01);

        ob.process(Event {
            timestamp: 1000,
            seq: 100,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 10.0,
        });

        ob.process(Event {
            timestamp: 1500,
            seq: 200,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 25.0,
        });

        let level = ob.bids.get(&ob.get_price_tick(50.0)).unwrap();
        assert_eq!(level.size, 25.0);
        assert_eq!(level.seq, 200);
    }

    #[test]
    fn test_stale_deletion_rejected() {
        let mut ob = Orderbook::new(0.01);

        // Insert level with seq=1000
        ob.process(Event {
            timestamp: 1000,
            seq: 1000,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 10.0,
        });

        // Try stale deletion with seq=500
        ob.process(Event {
            timestamp: 2000,
            seq: 500,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 0.0,
        });

        // Level should still exist
        assert!(ob.bids.contains_key(&ob.get_price_tick(50.0)));
        let level = ob.bids.get(&ob.get_price_tick(50.0)).unwrap();
        assert_eq!(level.seq, 1000);
    }

    #[test]
    fn test_fresh_deletion_accepted() {
        let mut ob = Orderbook::new(0.01);

        ob.process(Event {
            timestamp: 1000,
            seq: 100,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 10.0,
        });

        // Send newer deletion
        ob.process(Event {
            timestamp: 2000,
            seq: 200,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 0.0,
        });

        // Level should be removed
        assert!(!ob.bids.contains_key(&ob.get_price_tick(50.0)));
    }

    #[test]
    fn test_mixed_seq_multiple_levels() {
        let mut ob = Orderbook::new(0.01);

        // Setup three levels
        ob.process(Event {
            timestamp: 1000,
            seq: 100,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 10.0,
        });
        ob.process(Event {
            timestamp: 1000,
            seq: 101,
            is_trade: false,
            is_buy: true,
            price: 49.0,
            size: 11.0,
        });
        ob.process(Event {
            timestamp: 1000,
            seq: 102,
            is_trade: false,
            is_buy: true,
            price: 48.0,
            size: 12.0,
        });

        // Send mixed updates
        ob.process(Event {
            timestamp: 2000,
            seq: 150,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 20.0,
        }); // Accept
        ob.process(Event {
            timestamp: 2000,
            seq: 90,
            is_trade: false,
            is_buy: true,
            price: 49.0,
            size: 21.0,
        }); // Reject
        ob.process(Event {
            timestamp: 2000,
            seq: 200,
            is_trade: false,
            is_buy: true,
            price: 48.0,
            size: 22.0,
        }); // Accept

        // Verify selective updates
        assert_eq!(ob.bids.get(&ob.get_price_tick(50.0)).unwrap().size, 20.0); // Updated
        assert_eq!(ob.bids.get(&ob.get_price_tick(49.0)).unwrap().size, 11.0); // Not updated
        assert_eq!(ob.bids.get(&ob.get_price_tick(48.0)).unwrap().size, 22.0); // Updated
    }

    #[test]
    fn test_reject_equal_seq() {
        let mut ob = Orderbook::new(0.01);

        // Insert level with seq=100
        ob.process(Event {
            timestamp: 1000,
            seq: 100,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 10.0,
        });

        // Try to update with seq=100 (equal)
        ob.process(Event {
            timestamp: 2000,
            seq: 100,
            is_trade: false,
            is_buy: true,
            price: 50.0,
            size: 20.0,
        });

        // Verify size unchanged (equal seq rejected)
        let level = ob.bids.get(&ob.get_price_tick(50.0)).unwrap();
        assert_eq!(level.size, 10.0);
        assert_eq!(level.seq, 100);
    }

    #[test]
    fn test_seq_check_on_asks() {
        let mut ob = Orderbook::new(0.01);

        // Insert ask with seq=100
        ob.process(Event {
            timestamp: 1000,
            seq: 100,
            is_trade: false,
            is_buy: false,
            price: 50.0,
            size: 10.0,
        });

        // Try stale update
        ob.process(Event {
            timestamp: 2000,
            seq: 50,
            is_trade: false,
            is_buy: false,
            price: 50.0,
            size: 20.0,
        });

        // Verify size unchanged
        let level = ob.asks.get(&ob.get_price_tick(50.0)).unwrap();
        assert_eq!(level.size, 10.0);
        assert_eq!(level.seq, 100);
    }

    #[test]
    fn test_stale_deletion_on_asks() {
        let mut ob = Orderbook::new(0.01);

        // Insert ask with seq=1000
        ob.process(Event {
            timestamp: 1000,
            seq: 1000,
            is_trade: false,
            is_buy: false,
            price: 50.0,
            size: 10.0,
        });

        // Try stale deletion
        ob.process(Event {
            timestamp: 2000,
            seq: 500,
            is_trade: false,
            is_buy: false,
            price: 50.0,
            size: 0.0,
        });

        // Level should still exist
        assert!(ob.asks.contains_key(&ob.get_price_tick(50.0)));
        let level = ob.asks.get(&ob.get_price_tick(50.0)).unwrap();
        assert_eq!(level.seq, 1000);
    }
}
