use serde::de::Deserializer;
use serde::de::Error;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Event {
    pub timestamp: u64,
    pub seq: u64,
    pub is_trade: bool,
    pub is_buy: bool,
    pub price: f64,
    pub size: f64,
}

impl Event {
    pub fn price_ticks(&self, inv_tick_size: f64) -> u64 {
        (self.price * inv_tick_size) as u64
    }
}

/// Tick sizing strategy - zero overhead enum dispatch
#[derive(Debug, Clone, Copy)]
pub enum TickSizingStrategy {
    /// Standard fixed tick size: price * inv_tick_size
    Fixed,
    /// Bithumb's piecewise linear tick sizing
    BithumbPwl,
    /// Upbit's piecewise linear tick sizing (same as Bithumb for KRW pairs)
    UpbitPwl,
}

impl TickSizingStrategy {
    #[inline(always)]
    pub fn price_to_tick(&self, price: f64, inv_tick_size: f64) -> u64 {
        match self {
            TickSizingStrategy::Fixed => (price * inv_tick_size) as u64,
            TickSizingStrategy::BithumbPwl => bithumb_price_to_tick(price),
            TickSizingStrategy::UpbitPwl => upbit_price_to_tick(price),
        }
    }
}

#[inline]
pub fn bithumb_price_to_tick(price: f64) -> u64 {
    let (inv_tick_size, offset) = if price >= 1_000_000.0 {
        (1.0 / 1000.0, 42899)
    } else if price >= 500_000.0 {
        (1.0 / 500.0, 41900)
    } else if price >= 100_000.0 {
        (1.0 / 100.0, 37900)
    } else if price >= 50_000.0 {
        (1.0 / 50.0, 36900)
    } else if price >= 10_000.0 {
        (1.0 / 10.0, 32900)
    } else if price >= 5_000.0 {
        (1.0 / 5.0, 31900)
    } else if price >= 100.0 {
        (1.0, 27900)
    } else if price >= 10.0 {
        (1.0 / 0.01, 18000)
    } else if price >= 1.0 {
        (1.0 / 0.001, 9000)
    } else {
        (1.0 / 0.0001, 0)
    };

    offset + (price * inv_tick_size).round() as u64
}

#[inline]
fn upbit_price_to_tick(price: f64) -> u64 {
    let (inv_tick_size, offset) = if price >= 1_000_000.0 {
        (1.0 / 1000.0, 29_000)
    } else if price >= 500_000.0 {
        (1.0 / 500.0, 27_000)
    } else if price >= 100_000.0 {
        (1.0 / 100.0, 22_000)
    } else if price >= 50_000.0 {
        (1.0 / 50.0, 20_000)
    } else if price >= 10_000.0 {
        (1.0 / 10.0, 16_000)
    } else if price >= 5_000.0 {
        (1.0 / 5.0, 14_000)
    } else if price >= 1_000.0 {
        (1.0, 9000)
    } else if price >= 100.0 {
        (1.0, 8000)
    } else if price >= 10.0 {
        (1.0 / 0.1, 7000)
    } else if price >= 1.0 {
        (1.0 / 0.01, 6000)
    } else if price >= 0.1 {
        (1.0 / 0.001, 5000)
    } else if price >= 0.01 {
        (1.0 / 0.0001, 4000)
    } else if price >= 0.001 {
        (1.0 / 0.00001, 3000)
    } else if price >= 0.0001 {
        (1.0 / 0.000001, 2000)
    } else if price >= 0.00001 {
        (1.0 / 0.0000001, 1000)
    } else {
        (1.0 / 0.00000001, 0)
    };

    offset + (price * inv_tick_size).round() as u64
}

impl<'de> Deserialize<'de> for Event {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EventVisitor;

        impl<'de> Visitor<'de> for EventVisitor {
            type Value = Event;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Event")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut timestamp = None;
                let mut seq = None;
                let mut is_trade = None;
                let mut is_buy = None;
                let mut price = None;
                let mut size = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        "timestamp" => {
                            if timestamp.is_some() {
                                return Err(Error::duplicate_field("timestamp"));
                            }
                            timestamp = Some(map.next_value()?);
                        }
                        "seq" => {
                            if seq.is_some() {
                                return Err(Error::duplicate_field("seq"));
                            }
                            seq = Some(map.next_value()?);
                        }
                        "is_trade" => {
                            if is_trade.is_some() {
                                return Err(Error::duplicate_field("is_trade"));
                            }
                            is_trade = match map.next_value()? {
                                0 => Some(false),
                                1 => Some(true),
                                _ => None,
                            }
                        }
                        "is_buy" => {
                            if is_buy.is_some() {
                                return Err(Error::duplicate_field("is_buy"));
                            }
                            is_buy = match map.next_value()? {
                                0 => Some(false),
                                1 => Some(true),
                                _ => None,
                            }
                        }
                        "price" => {
                            if price.is_some() {
                                return Err(Error::duplicate_field("price"));
                            }
                            price = Some(map.next_value()?);
                        }
                        "size" => {
                            if size.is_some() {
                                return Err(Error::duplicate_field("size"));
                            }
                            size = Some(map.next_value()?);
                        }
                        _ => {
                            // Ignore unknown fields
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let timestamp = timestamp.ok_or_else(|| Error::missing_field("timestamp"))?;
                let seq = seq.ok_or_else(|| Error::missing_field("seq"))?;
                let is_trade = is_trade.ok_or_else(|| Error::missing_field("is_trade"))?;
                let is_buy = is_buy.ok_or_else(|| Error::missing_field("is_buy"))?;
                let price = price.ok_or_else(|| Error::missing_field("price"))?;
                let size = size.ok_or_else(|| Error::missing_field("size"))?;

                Ok(Event {
                    timestamp,
                    seq,
                    is_trade,
                    is_buy,
                    price,
                    size,
                })
            }
        }

        deserializer.deserialize_map(EventVisitor)
    }
}

mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn deser_level() {
        let data =
            "timestamp,seq,is_trade,is_buy,price,size\n1575158405045139,0,0,0,7541.38,0.085806";

        let mut reader = csv::Reader::from_reader(data.as_bytes());

        let event = reader.deserialize::<Event>().next().unwrap().unwrap();

        assert_eq!(event.timestamp, 1575158405045139);
        assert_eq!(event.seq, 0);
        assert!(!event.is_trade);
        assert!(!event.is_buy);
        assert_eq!(event.price, 7541.38);
        assert_eq!(event.size, 0.085806);
    }

    #[test]
    fn price_ticks() {
        let event = Event {
            timestamp: 0,
            seq: 0,
            is_trade: false,
            is_buy: false,
            price: 10.0,
            size: 1.0,
        };

        assert_eq!(event.price_ticks(1.0 / 0.01), 1000);
    }
}
