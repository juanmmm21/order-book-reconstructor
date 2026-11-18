//! L2 book levels for the hot path; mirrors `order_book_reconstructor/fast.py::PyBookLevels`.
//!
//! Prices and quantities cross the boundary as the exchange's own decimal strings. Keys compare
//! numerically (`"100.0" == "100.00"`), a level keeps the price string it was created with and
//! the latest quantity string, and a numeric zero quantity removes it — exactly like the Python
//! implementation, so both return the same strings for the same inputs.

// pyo3 0.22 #[pyfunction]/#[pymethods] expansions trip this lint on `PyResult` returns.
#![allow(clippy::useless_conversion)]

use std::collections::BTreeMap;
use std::str::FromStr;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rust_decimal::Decimal;

type Level = (String, String);

const MAX_INT_DIGITS: usize = 15;
const MAX_FRAC_DIGITS: usize = 12;

fn is_plain_decimal(text: &str) -> bool {
    let (int_part, frac_part) = match text.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (text, None),
    };
    let digits = |s: &str, max: usize| {
        !s.is_empty() && s.len() <= max && s.bytes().all(|b| b.is_ascii_digit())
    };
    digits(int_part, MAX_INT_DIGITS) && frac_part.is_none_or(|f| digits(f, MAX_FRAC_DIGITS))
}

fn parse(text: &str) -> PyResult<Decimal> {
    if !is_plain_decimal(text) {
        return Err(PyValueError::new_err(format!("invalid decimal: {text:?}")));
    }
    Decimal::from_str(text).map_err(|e| PyValueError::new_err(format!("invalid decimal: {e}")))
}

#[derive(Default)]
struct Side {
    levels: BTreeMap<Decimal, Level>,
}

fn parse_all(changes: &[Level]) -> PyResult<Vec<(Decimal, Decimal)>> {
    changes
        .iter()
        .map(|(p, q)| Ok((parse(p)?, parse(q)?)))
        .collect()
}

impl Side {
    fn apply(&mut self, parsed: Vec<(Decimal, Decimal)>, changes: Vec<Level>) {
        for ((price, quantity), (price_text, quantity_text)) in parsed.into_iter().zip(changes) {
            if quantity.is_zero() {
                self.levels.remove(&price);
            } else if let Some(level) = self.levels.get_mut(&price) {
                level.1 = quantity_text;
            } else {
                self.levels.insert(price, (price_text, quantity_text));
            }
        }
    }
}

#[pyclass]
#[derive(Default)]
struct BookLevels {
    bids: Side,
    asks: Side,
}

#[pymethods]
impl BookLevels {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn clear(&mut self) {
        self.bids.levels.clear();
        self.asks.levels.clear();
    }

    /// Apply absolute level quantities ("0" removes). All-or-nothing on invalid input.
    fn apply(&mut self, bids: Vec<Level>, asks: Vec<Level>) -> PyResult<()> {
        // validate both sides before touching either: a bad message changes nothing
        let parsed_bids = parse_all(&bids)?;
        let parsed_asks = parse_all(&asks)?;
        self.bids.apply(parsed_bids, bids);
        self.asks.apply(parsed_asks, asks);
        Ok(())
    }

    fn top(&self, depth: usize) -> (Vec<Level>, Vec<Level>) {
        let bids = self
            .bids
            .levels
            .values()
            .rev()
            .take(depth)
            .cloned()
            .collect();
        let asks = self.asks.levels.values().take(depth).cloned().collect();
        (bids, asks)
    }

    fn best(&self) -> (Option<Level>, Option<Level>) {
        (
            self.bids.levels.values().next_back().cloned(),
            self.asks.levels.values().next().cloned(),
        )
    }

    fn sizes(&self) -> (usize, usize) {
        (self.bids.levels.len(), self.asks.levels.len())
    }
}

#[pymodule]
fn order_book_reconstructor_rs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<BookLevels>()?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(p: &str, q: &str) -> Level {
        (p.to_string(), q.to_string())
    }

    #[test]
    fn numeric_keys_keep_first_spelling_and_zero_removes() {
        let mut side = Side::default();
        let changes = vec![
            level("100.0", "1"),
            level("100.00", "2.5"),
            level("99", "1"),
        ];
        side.apply(parse_all(&changes).unwrap(), changes);
        let values: Vec<_> = side.levels.values().cloned().collect();
        assert_eq!(values, vec![level("99", "1"), level("100.0", "2.5")]);
        let removal = vec![level("100", "0.000")];
        side.apply(parse_all(&removal).unwrap(), removal);
        assert_eq!(side.levels.len(), 1);
    }

    #[test]
    fn grammar_rejects_what_python_rejects() {
        for bad in ["", "-1", "1e3", "NaN", "1.", ".1", "1234567890123456"] {
            assert!(!is_plain_decimal(bad), "{bad}");
        }
    }
}
